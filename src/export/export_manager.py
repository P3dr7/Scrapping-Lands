#!/usr/bin/env python3
"""
Export Manager - Controle de Qualidade, Scoring e Exportação para CSV.

Este módulo consolida todas as tabelas do sistema (parks_master, owners,
companies, contacts) em um único arquivo plano otimizado para mala direta.

Funcionalidades:
1. Lead Scoring (Tier A/B/C)
2. Query Mestra com deduplicação de contatos
3. Filtros de QA
4. Exportação CSV com relatório de resumo

Autor: BellaTerra Intelligence
Data: 2025-12
"""

import os
import sys
from datetime import datetime
from enum import Enum
from typing import Optional, Dict, Any, List, Tuple
from dataclasses import dataclass, field

import pandas as pd
from sqlalchemy import text, create_engine
from sqlalchemy.orm import sessionmaker
from loguru import logger


# =============================================================================
# ENUMS E CONSTANTES
# =============================================================================

class LeadTier(Enum):
    """Classificação de qualidade dos leads."""
    TIER_A = 'A'  # Ouro: Nome + Endereço + Contato
    TIER_B = 'B'  # Prata: Nome + Endereço (sem contato digital)
    TIER_C = 'C'  # Bronze: Apenas endereço do parque
    INVALID = 'X' # Inválido: Sem dados suficientes


@dataclass
class ExportStats:
    """Estatísticas da exportação."""
    total_parks: int = 0
    parks_with_owner: int = 0
    parks_with_company: int = 0
    parks_with_contacts: int = 0
    
    tier_a_count: int = 0
    tier_b_count: int = 0
    tier_c_count: int = 0
    invalid_count: int = 0
    
    total_emails: int = 0
    total_phones: int = 0
    
    qa_filtered: int = 0
    final_records: int = 0
    
    def owner_success_rate(self) -> float:
        """Taxa de sucesso na identificação de proprietários."""
        if self.total_parks == 0:
            return 0.0
        return (self.parks_with_owner / self.total_parks) * 100
    
    def contact_success_rate(self) -> float:
        """Taxa de sucesso na coleta de contatos."""
        if self.total_parks == 0:
            return 0.0
        return (self.parks_with_contacts / self.total_parks) * 100


# =============================================================================
# EXPORT MANAGER
# =============================================================================

class ExportManager:
    """
    Gerenciador de exportação de leads para mala direta.
    
    Consolida dados de múltiplas tabelas, aplica scoring de qualidade,
    e exporta CSV pronto para impressão de etiquetas/cartas.
    """
    
    def __init__(self, engine):
        """
        Inicializa o ExportManager.
        
        Args:
            engine: SQLAlchemy engine conectado ao banco
        """
        self.engine = engine
        self.stats = ExportStats()
        logger.info("ExportManager inicializado")
    
    def _build_master_query(self) -> str:
        """
        Constrói a query mestra que consolida todas as tabelas.
        
        A query faz LEFT JOIN de parks_master com owners, companies e contacts,
        agregando múltiplos contatos em uma única linha por parque.
        
        Returns:
            SQL query string
        """
        query = """
        WITH aggregated_contacts AS (
            -- Agrupa contatos por parque, concatenando emails e telefones
            SELECT 
                park_id,
                STRING_AGG(DISTINCT email, '; ' ORDER BY email) FILTER (WHERE email IS NOT NULL) as emails,
                STRING_AGG(DISTINCT phone, '; ' ORDER BY phone) FILTER (WHERE phone IS NOT NULL) as phones,
                STRING_AGG(DISTINCT person_name, '; ' ORDER BY person_name) FILTER (WHERE person_name IS NOT NULL) as contact_names,
                COUNT(DISTINCT email) FILTER (WHERE email IS NOT NULL) as email_count,
                COUNT(DISTINCT phone) FILTER (WHERE phone IS NOT NULL) as phone_count,
                MAX(confidence_level) as max_confidence
            FROM contacts
            WHERE is_valid = TRUE
            GROUP BY park_id
        ),
        company_contacts AS (
            -- Agrupa contatos por empresa
            SELECT 
                company_id,
                STRING_AGG(DISTINCT email, '; ' ORDER BY email) FILTER (WHERE email IS NOT NULL) as company_emails,
                STRING_AGG(DISTINCT phone, '; ' ORDER BY phone) FILTER (WHERE phone IS NOT NULL) as company_phones
            FROM contacts
            WHERE is_valid = TRUE AND company_id IS NOT NULL
            GROUP BY company_id
        )
        SELECT 
            -- Identificação do Parque
            pm.id as park_id,
            pm.master_id,
            pm.name as park_name,
            pm.park_type,
            
            -- Endereço do Parque
            pm.address as park_address,
            pm.city as park_city,
            pm.state as park_state,
            pm.zip_code as park_zip,
            pm.county as park_county,
            
            -- Coordenadas
            pm.latitude,
            pm.longitude,
            
            -- Contato do Parque (Google Places)
            pm.phone as park_phone,
            pm.website as park_website,
            pm.email as park_email,
            
            -- Status do Parque
            pm.business_status,
            pm.avg_rating,
            pm.total_reviews,
            pm.total_lots,
            
            -- Dados do Owner (schema real)
            o.id as owner_id,
            o.full_name as owner_full_name,
            o.first_name as owner_first_name,
            o.last_name as owner_last_name,
            o.is_individual as owner_is_individual,
            o.mailing_address as owner_mailing_address,
            o.phone as owner_phone,
            o.email as owner_email,
            
            -- Dados da Empresa (se corporate)
            c.id as company_id,
            c.legal_name as company_legal_name,
            c.entity_type as company_entity_type,
            c.registered_agent_name,
            c.registered_agent_address,
            c.principals as company_principals,
            c.sos_status as company_status,
            
            -- Contatos Agregados do Parque
            ac.emails as park_contact_emails,
            ac.phones as park_contact_phones,
            ac.contact_names as park_contact_names,
            ac.email_count,
            ac.phone_count,
            ac.max_confidence as contact_confidence,
            
            -- Contatos Agregados da Empresa
            cc.company_emails,
            cc.company_phones,
            
            -- Qualidade
            pm.confidence_score as data_confidence,
            pm.data_quality_flags,
            pm.needs_manual_review,
            pm.last_verified_at
            
        FROM parks_master pm
        LEFT JOIN owners o ON pm.owner_id = o.id
        LEFT JOIN companies c ON pm.company_id = c.id
        LEFT JOIN aggregated_contacts ac ON pm.id = ac.park_id
        LEFT JOIN company_contacts cc ON c.id = cc.company_id
        
        ORDER BY pm.name
        """
        return query
    
    def _calculate_tier(self, row: pd.Series) -> LeadTier:
        """
        Calcula o tier de qualidade do lead.
        
        Tier A (Ouro): Nome + Endereço + (Telefone OU Email)
        Tier B (Prata): Nome + Endereço (sem contato digital)
        Tier C (Bronze): Apenas endereço do parque
        
        Args:
            row: Linha do DataFrame com dados do lead
            
        Returns:
            LeadTier correspondente
        """
        # Verifica se tem nome do proprietário
        has_owner_name = bool(
            pd.notna(row.get('owner_full_name')) or
            pd.notna(row.get('registered_agent_name')) or
            pd.notna(row.get('company_legal_name'))
        )
        
        # Verifica se tem endereço de correspondência
        # owner_mailing_address é JSONB, pode ser dict ou string
        owner_addr = row.get('owner_mailing_address')
        has_mailing_address = bool(
            (pd.notna(owner_addr) and owner_addr) or
            pd.notna(row.get('registered_agent_address'))
        )
        
        # Verifica se tem contato digital
        has_digital_contact = bool(
            pd.notna(row.get('park_contact_emails')) or
            pd.notna(row.get('park_contact_phones')) or
            pd.notna(row.get('company_emails')) or
            pd.notna(row.get('company_phones')) or
            pd.notna(row.get('park_phone')) or
            pd.notna(row.get('park_email')) or
            pd.notna(row.get('owner_phone')) or
            pd.notna(row.get('owner_email'))
        )
        
        # Verifica se tem endereço do parque válido
        has_park_address = bool(
            pd.notna(row.get('park_address')) and
            pd.notna(row.get('park_city')) and
            pd.notna(row.get('park_state'))
        )
        
        # Classificação por tier
        if has_owner_name and has_mailing_address and has_digital_contact:
            return LeadTier.TIER_A
        elif has_owner_name and has_mailing_address:
            return LeadTier.TIER_B
        elif has_park_address:
            return LeadTier.TIER_C
        else:
            return LeadTier.INVALID
    
    def _get_best_recipient_name(self, row: pd.Series) -> str:
        """
        Determina o melhor nome de destinatário.
        
        Prioridade:
        1. Agente Registrado (se empresa)
        2. Nome completo do owner
        3. Nome legal da empresa
        4. "Proprietário" (fallback)
        """
        if pd.notna(row.get('registered_agent_name')):
            return str(row['registered_agent_name'])
        if pd.notna(row.get('owner_full_name')):
            return str(row['owner_full_name'])
        if pd.notna(row.get('company_legal_name')):
            return str(row['company_legal_name'])
        return "Proprietário"
    
    def _get_best_mailing_address(self, row: pd.Series) -> Tuple[str, str, str, str]:
        """
        Determina o melhor endereço de correspondência.
        
        Prioridade:
        1. Endereço do Agente Registrado
        2. Endereço de correspondência do owner (JSONB)
        3. Endereço do parque
        
        Returns:
            Tuple (address, city, state, zip)
        """
        # Tenta agente registrado primeiro
        if pd.notna(row.get('registered_agent_address')):
            addr = str(row['registered_agent_address'])
            # Parse simples - assume formato "Rua, Cidade, ST ZIP"
            parts = addr.split(',')
            if len(parts) >= 3:
                street = parts[0].strip()
                city = parts[1].strip() if len(parts) > 1 else ""
                state_zip = parts[-1].strip().split() if len(parts) > 2 else ["", ""]
                state = state_zip[0] if len(state_zip) > 0 else ""
                zip_code = state_zip[1] if len(state_zip) > 1 else ""
                return (street, city, state, zip_code)
            return (addr, "", "", "")
        
        # Tenta endereço de correspondência do owner (é JSONB)
        owner_addr = row.get('owner_mailing_address')
        if pd.notna(owner_addr) and owner_addr:
            if isinstance(owner_addr, dict):
                # É um dict/JSONB
                street = owner_addr.get('line1', '') or ''
                if owner_addr.get('line2'):
                    street += ' ' + owner_addr.get('line2', '')
                city = owner_addr.get('city', '') or ''
                state = owner_addr.get('state', '') or ''
                zip_code = owner_addr.get('zip', '') or ''
                return (street.strip(), city, state, zip_code)
            elif isinstance(owner_addr, str):
                # Parse como string
                parts = owner_addr.split(',')
                if len(parts) >= 3:
                    street = parts[0].strip()
                    city = parts[1].strip() if len(parts) > 1 else ""
                    state_zip = parts[-1].strip().split() if len(parts) > 2 else ["", ""]
                    state = state_zip[0] if len(state_zip) > 0 else ""
                    zip_code = state_zip[1] if len(state_zip) > 1 else ""
                    return (street, city, state, zip_code)
                return (owner_addr, "", "", "")
        
        # Fallback para endereço do parque
        return (
            str(row.get('park_address', '')) if pd.notna(row.get('park_address')) else '',
            str(row.get('park_city', '')) if pd.notna(row.get('park_city')) else '',
            str(row.get('park_state', '')) if pd.notna(row.get('park_state')) else '',
            str(row.get('park_zip', '')) if pd.notna(row.get('park_zip')) else ''
        )
    
    def _get_best_contact(self, row: pd.Series) -> Tuple[str, str]:
        """
        Obtém o melhor contato disponível.
        
        Returns:
            Tuple (email, phone)
        """
        # Prioriza contatos enriquecidos
        email = ""
        phone = ""
        
        # Email: prioriza contatos verificados
        for col in ['park_contact_emails', 'company_emails', 'owner_email', 'park_email']:
            if pd.notna(row.get(col)):
                val = str(row[col])
                emails = val.split(';')
                email = emails[0].strip()  # Primeiro email
                break
        
        # Telefone: prioriza contatos enriquecidos
        for col in ['park_contact_phones', 'company_phones', 'owner_phone', 'park_phone']:
            if pd.notna(row.get(col)):
                val = str(row[col])
                phones = val.split(';')
                phone = phones[0].strip()  # Primeiro telefone
                break
        
        return (email, phone)
    
    def _apply_qa_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica filtros de controle de qualidade.
        
        Remove:
        - Linhas sem endereço de correspondência E sem endereço do parque válido
        - Parques com status "CLOSED_PERMANENTLY"
        - Entradas duplicadas
        
        Args:
            df: DataFrame com todos os leads
            
        Returns:
            DataFrame filtrado
        """
        original_count = len(df)
        
        # Remove parques fechados permanentemente
        if 'business_status' in df.columns:
            df = df[df['business_status'] != 'CLOSED_PERMANENTLY']
        
        # Remove linhas sem nenhum endereço utilizável
        mask_has_address = (
            (df['mailing_address'].notna() & (df['mailing_address'] != '')) |
            (df['park_address'].notna() & (df['park_address'] != ''))
        )
        df = df[mask_has_address]
        
        # Remove duplicatas baseado no nome do parque e cidade
        df = df.drop_duplicates(subset=['park_name', 'park_city'], keep='first')
        
        # Remove tier inválido
        df = df[df['lead_tier'] != 'X']
        
        filtered_count = original_count - len(df)
        self.stats.qa_filtered = filtered_count
        
        logger.info(f"QA removeu {filtered_count} registros")
        
        return df
    
    def _transform_to_flat_file(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma o DataFrame em formato flat file para mala direta.
        
        Args:
            df: DataFrame bruto da query
            
        Returns:
            DataFrame formatado para exportação
        """
        flat_records = []
        
        for idx, row in df.iterrows():
            # Calcula tier
            tier = self._calculate_tier(row)
            
            # Obtém melhores dados
            recipient_name = self._get_best_recipient_name(row)
            mail_addr, mail_city, mail_state, mail_zip = self._get_best_mailing_address(row)
            best_email, best_phone = self._get_best_contact(row)
            
            # Atualiza estatísticas
            if tier == LeadTier.TIER_A:
                self.stats.tier_a_count += 1
            elif tier == LeadTier.TIER_B:
                self.stats.tier_b_count += 1
            elif tier == LeadTier.TIER_C:
                self.stats.tier_c_count += 1
            else:
                self.stats.invalid_count += 1
            
            if pd.notna(row.get('owner_id')):
                self.stats.parks_with_owner += 1
            if pd.notna(row.get('company_id')):
                self.stats.parks_with_company += 1
            if best_email or best_phone:
                self.stats.parks_with_contacts += 1
            if best_email:
                self.stats.total_emails += 1
            if best_phone:
                self.stats.total_phones += 1
            
            # Monta registro flat
            record = {
                # Identificação
                'lead_tier': tier.value,
                'park_id': row.get('park_id'),
                'park_name': row.get('park_name'),
                'park_type': row.get('park_type'),
                
                # Destinatário
                'recipient_name': recipient_name,
                'is_company': 'Sim' if pd.notna(row.get('company_id')) else 'Não',
                'company_name': row.get('company_legal_name') if pd.notna(row.get('company_legal_name')) else '',
                'entity_type': row.get('company_entity_type') if pd.notna(row.get('company_entity_type')) else '',
                
                # Endereço de Correspondência
                'mailing_address': mail_addr,
                'mailing_city': mail_city,
                'mailing_state': mail_state,
                'mailing_zip': mail_zip,
                
                # Endereço do Parque
                'park_address': row.get('park_address') if pd.notna(row.get('park_address')) else '',
                'park_city': row.get('park_city') if pd.notna(row.get('park_city')) else '',
                'park_state': row.get('park_state') if pd.notna(row.get('park_state')) else '',
                'park_zip': row.get('park_zip') if pd.notna(row.get('park_zip')) else '',
                'park_county': row.get('park_county') if pd.notna(row.get('park_county')) else '',
                
                # Contato Principal
                'primary_email': best_email,
                'primary_phone': best_phone,
                
                # Todos os Contatos (concatenados)
                'all_emails': row.get('park_contact_emails') if pd.notna(row.get('park_contact_emails')) else '',
                'all_phones': row.get('park_contact_phones') if pd.notna(row.get('park_contact_phones')) else '',
                
                # Website
                'website': row.get('park_website') if pd.notna(row.get('park_website')) else '',
                
                # Métricas do Parque
                'rating': row.get('avg_rating') if pd.notna(row.get('avg_rating')) else '',
                'reviews': row.get('total_reviews') if pd.notna(row.get('total_reviews')) else '',
                'total_lots': row.get('total_lots') if pd.notna(row.get('total_lots')) else '',
                'business_status': row.get('business_status') if pd.notna(row.get('business_status')) else '',
                
                # Coordenadas
                'latitude': row.get('latitude') if pd.notna(row.get('latitude')) else '',
                'longitude': row.get('longitude') if pd.notna(row.get('longitude')) else '',
                
                # Metadados
                'data_confidence': row.get('data_confidence') if pd.notna(row.get('data_confidence')) else '',
                'needs_review': 'Sim' if row.get('needs_manual_review') else 'Não',
            }
            
            flat_records.append(record)
        
        return pd.DataFrame(flat_records)
    
    def export_leads(
        self,
        output_dir: str = "output",
        filename_prefix: str = "indiana_final_leads",
        apply_qa: bool = True,
        min_tier: Optional[str] = None,
    ) -> Tuple[str, ExportStats]:
        """
        Executa a exportação completa de leads.
        
        Args:
            output_dir: Diretório de saída
            filename_prefix: Prefixo do nome do arquivo
            apply_qa: Se deve aplicar filtros de QA
            min_tier: Tier mínimo para incluir ('A', 'B', ou 'C')
            
        Returns:
            Tuple (caminho do arquivo, estatísticas)
        """
        logger.info("="*60)
        logger.info("  INICIANDO EXPORTAÇÃO DE LEADS")
        logger.info("="*60)
        
        # Reseta estatísticas
        self.stats = ExportStats()
        
        # 1. Executa a query mestra
        logger.info("Executando query mestra...")
        query = self._build_master_query()
        
        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        
        self.stats.total_parks = len(df)
        logger.info(f"Total de parques carregados: {len(df)}")
        
        # 2. Transforma para flat file
        logger.info("Transformando para formato flat file...")
        flat_df = self._transform_to_flat_file(df)
        
        # 3. Aplica filtros de QA
        if apply_qa:
            logger.info("Aplicando filtros de QA...")
            flat_df = self._apply_qa_filters(flat_df)
        
        # 4. Filtra por tier mínimo se especificado
        if min_tier:
            tier_order = {'A': 0, 'B': 1, 'C': 2, 'X': 3}
            min_order = tier_order.get(min_tier, 3)
            flat_df = flat_df[flat_df['lead_tier'].apply(lambda x: tier_order.get(x, 3) <= min_order)]
        
        self.stats.final_records = len(flat_df)
        
        # 5. Cria diretório de saída
        os.makedirs(output_dir, exist_ok=True)
        
        # 6. Gera nome do arquivo com data
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{date_str}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # 7. Exporta CSV
        logger.info(f"Exportando para {filepath}...")
        flat_df.to_csv(filepath, index=False, encoding='utf-8-sig')  # utf-8-sig para Excel
        
        # 8. Imprime relatório
        self._print_summary_report()
        
        logger.info(f"\n✅ Arquivo exportado: {filepath}")
        
        return filepath, self.stats
    
    def _print_summary_report(self):
        """Imprime relatório de resumo da exportação."""
        s = self.stats
        
        print("\n" + "="*60)
        print("  📊 RELATÓRIO DE EXPORTAÇÃO")
        print("="*60)
        
        print(f"\n  📍 PARQUES")
        print(f"     Total de Parques Processados: {s.total_parks}")
        print(f"     Parques com Proprietário Identificado: {s.parks_with_owner}")
        print(f"     Parques com Empresa Vinculada: {s.parks_with_company}")
        print(f"     Parques com Contato Digital: {s.parks_with_contacts}")
        
        print(f"\n  🏆 CLASSIFICAÇÃO DOS LEADS")
        print(f"     Tier A (Ouro - Completo): {s.tier_a_count}")
        print(f"     Tier B (Prata - Correio): {s.tier_b_count}")
        print(f"     Tier C (Bronze - Básico): {s.tier_c_count}")
        print(f"     Inválidos (Filtrados): {s.invalid_count}")
        
        print(f"\n  📞 CONTATOS COLETADOS")
        print(f"     Total de Emails: {s.total_emails}")
        print(f"     Total de Telefones: {s.total_phones}")
        
        print(f"\n  📈 MÉTRICAS DE SUCESSO")
        print(f"     Taxa de Identificação de Proprietário: {s.owner_success_rate():.1f}%")
        print(f"     Taxa de Coleta de Contato: {s.contact_success_rate():.1f}%")
        
        print(f"\n  🔍 CONTROLE DE QUALIDADE")
        print(f"     Registros Filtrados pelo QA: {s.qa_filtered}")
        print(f"     Registros Finais Exportados: {s.final_records}")
        
        print("\n" + "="*60)
    
    def export_by_tier(
        self,
        output_dir: str = "output",
    ) -> Dict[str, str]:
        """
        Exporta leads separados por tier.
        
        Args:
            output_dir: Diretório de saída
            
        Returns:
            Dict com caminhos dos arquivos por tier
        """
        logger.info("Exportando leads separados por tier...")
        
        # Executa query
        query = self._build_master_query()
        with self.engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
        
        # Transforma
        flat_df = self._transform_to_flat_file(df)
        flat_df = self._apply_qa_filters(flat_df)
        
        # Cria diretório
        os.makedirs(output_dir, exist_ok=True)
        date_str = datetime.now().strftime("%Y%m%d")
        
        files = {}
        
        for tier in ['A', 'B', 'C']:
            tier_df = flat_df[flat_df['lead_tier'] == tier]
            if len(tier_df) > 0:
                filename = f"indiana_tier_{tier}_{date_str}.csv"
                filepath = os.path.join(output_dir, filename)
                tier_df.to_csv(filepath, index=False, encoding='utf-8-sig')
                files[tier] = filepath
                logger.info(f"  Tier {tier}: {len(tier_df)} leads -> {filepath}")
        
        return files
    
    def get_quality_report(self) -> pd.DataFrame:
        """
        Gera relatório detalhado de qualidade dos dados.
        
        Returns:
            DataFrame com métricas de qualidade por campo
        """
        query = """
        SELECT 
            COUNT(*) as total,
            COUNT(address) as has_address,
            COUNT(city) as has_city,
            COUNT(phone) as has_phone,
            COUNT(website) as has_website,
            COUNT(owner_id) as has_owner,
            COUNT(company_id) as has_company
        FROM parks_master
        """
        
        with self.engine.connect() as conn:
            result = conn.execute(text(query))
            row = result.fetchone()
        
        total = row[0]
        fields = ['address', 'city', 'phone', 'website', 'owner_id', 'company_id']
        values = row[1:]
        
        report_data = []
        for field, value in zip(fields, values):
            report_data.append({
                'campo': field,
                'preenchidos': value,
                'vazios': total - value,
                'taxa_preenchimento': f"{(value/total)*100:.1f}%" if total > 0 else "0%"
            })
        
        return pd.DataFrame(report_data)


# =============================================================================
# CLI HELPER
# =============================================================================

def run_export(
    output_dir: str = "output",
    apply_qa: bool = True,
    min_tier: Optional[str] = None,
    separate_tiers: bool = False,
) -> None:
    """
    Função helper para executar exportação via CLI.
    
    Args:
        output_dir: Diretório de saída
        apply_qa: Se deve aplicar filtros de QA
        min_tier: Tier mínimo ('A', 'B', 'C')
        separate_tiers: Se deve gerar arquivos separados por tier
    """
    # Importa aqui para evitar circular import
    import sys
    sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
    from src.database import get_engine
    
    engine = get_engine()
    manager = ExportManager(engine)
    
    if separate_tiers:
        files = manager.export_by_tier(output_dir)
        print(f"\nArquivos gerados: {files}")
    else:
        filepath, stats = manager.export_leads(
            output_dir=output_dir,
            apply_qa=apply_qa,
            min_tier=min_tier,
        )


if __name__ == "__main__":
    run_export()
