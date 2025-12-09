"""
Owner Lookup Orchestrator
==========================

Orquestrador principal da Fase 3: Identificação de proprietários.

FLUXO:
------
1. Ler parques da tabela parks_master
2. Para cada parque:
   a. Identificar condado (county_mapper)
   b. Selecionar fetcher apropriado
   c. Buscar proprietário nos registros fiscais
   d. Salvar em owners + atualizar parks_master
3. Gerar relatório de sucesso/falhas

ESTRATÉGIAS DE ROBUSTEZ:
-------------------------
- Retry com backoff exponencial para erros de rede
- Skip e continuar se um parque falhar (não parar tudo)
- Logging detalhado de cada etapa
- Checkpoint: salvar progresso a cada N parques
- Rate limiting agressivo para evitar bloqueios

PROTEÇÕES ANTI-SCRAPING:
-------------------------
⚠️ SISTEMAS BEACON/SCHNEIDER (40 condados):
   - Rate limit: ~10-20 req/min
   - CAPTCHA após ~50 requests consecutivos
   - Detecção de User-Agent de bots
   - SOLUÇÃO: Delays de 3-5 segundos + User-Agent rotation + Selenium headless

⚠️ SISTEMAS VANGUARD (15 condados):
   - Rate limit: ~30 req/min (mais relaxado)
   - Sem CAPTCHA geralmente
   - SOLUÇÃO: Delays de 2-3 segundos suficientes

⚠️ GIS CUSTOMIZADOS (25 condados):
   - Proteções variam muito
   - Alguns sem proteção, outros com WAF (Web Application Firewall)
   - SOLUÇÃO: Análise individual + delays conservadores (5s)

ALTERNATIVAS SE BLOQUEADO:
---------------------------
1. Proxy rotation (serviços como ScraperAPI, Bright Data)
2. Selenium com perfil humanizado (mouse movements, scrolling)
3. CAPTCHA solving services (2Captcha, Anti-Captcha) - $$
4. Comprar dados de provedores comerciais (DataTree, CoreLogic) - $$$$
5. FOIA Request em lote (gratuito mas lento - 30+ dias)

Author: BellaTerra Intelligence Team
Date: December 2025
"""

import time
from typing import Optional, List, Dict, Tuple
from datetime import datetime
from pathlib import Path
import sys

from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session

# Importações internas
from src.database import get_db_session
from src.owners.county_mapper import CountyMapper
from src.owners.base_fetcher import (
    CountyAssessorFetcher,
    OwnerRecord,
    FetchResult
)
from src.owners.fetchers.generic_fetcher import (
    get_fetcher_for_county,
    MockFetcher
)


class OwnerLookupOrchestrator:
    """
    Orquestrador principal para busca de proprietários.
    
    Coordena:
    - Leitura de parques do banco
    - Identificação de condado
    - Seleção de fetcher
    - Persistência de dados
    - Tratamento de erros
    """
    
    def __init__(
        self,
        use_mock: bool = False,
        max_retries: int = 3,
        delay_between_requests: float = 3.0,
        checkpoint_interval: int = 10
    ):
        """
        Args:
            use_mock: Se True, usa MockFetcher (para testes sem consumir APIs)
            max_retries: Tentativas máximas em caso de erro
            delay_between_requests: Segundos entre cada request (rate limiting)
            checkpoint_interval: Salvar progresso a cada N parques
        """
        self.use_mock = use_mock
        self.max_retries = max_retries
        self.delay_between_requests = delay_between_requests
        self.checkpoint_interval = checkpoint_interval
        
        # Inicializar county mapper
        self.county_mapper = CountyMapper()
        
        # Cache de fetchers (um por condado)
        self._fetcher_cache: Dict[str, CountyAssessorFetcher] = {}
        
        # Estatísticas de processamento
        self.stats = {
            'total_parks': 0,
            'processed': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
            'county_not_identified': 0,
            'owner_found': 0,
            'owner_not_found': 0,
            'start_time': None,
            'end_time': None
        }
        
        # Configurar logging para arquivo
        log_dir = Path(__file__).parent.parent.parent / "logs"
        log_dir.mkdir(exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file = log_dir / f"owner_lookup_{timestamp}.log"
        
        logger.add(
            log_file,
            rotation="100 MB",
            retention="30 days",
            level="DEBUG"
        )
        
        logger.info("=" * 80)
        logger.info("OWNER LOOKUP ORCHESTRATOR - Iniciado")
        logger.info("=" * 80)
        logger.info(f"Modo: {'MOCK (desenvolvimento)' if use_mock else 'PRODUÇÃO'}")
        logger.info(f"Max retries: {max_retries}")
        logger.info(f"Delay entre requests: {delay_between_requests}s")
        logger.info(f"Checkpoint a cada: {checkpoint_interval} parques")
        logger.info(f"Log file: {log_file}")
        logger.info("=" * 80)
    
    def process_all_parks(self, limit: Optional[int] = None):
        """
        Processa todos os parques da tabela parks_master.
        
        Args:
            limit: Limitar processamento a N parques (para testes)
        """
        self.stats['start_time'] = datetime.now()
        
        logger.info("🚀 Iniciando processamento de parques...")
        
        with get_db_session() as session:
            # Buscar parques que ainda não têm proprietário
            parks = self._get_parks_without_owner(session, limit)
            
            self.stats['total_parks'] = len(parks)
            logger.info(f"📊 Total de parques a processar: {len(parks)}")
            
            if len(parks) == 0:
                logger.info("✅ Nenhum parque pendente. Todos já processados!")
                return
            
            # Processar cada parque
            for i, park in enumerate(parks, 1):
                logger.info("")
                logger.info("=" * 80)
                logger.info(f"PARQUE {i}/{len(parks)}")
                logger.info("=" * 80)
                
                try:
                    self._process_single_park(session, park)
                    self.stats['processed'] += 1
                    
                except Exception as e:
                    logger.error(f"❌ Erro ao processar parque {park['id']}: {e}")
                    self.stats['failed'] += 1
                    continue
                
                # Checkpoint
                if i % self.checkpoint_interval == 0:
                    logger.info(f"💾 Checkpoint: {i}/{len(parks)} parques processados")
                    session.commit()
                
                # Delay entre requests (rate limiting)
                if i < len(parks):  # Não delay no último
                    logger.debug(f"⏳ Aguardando {self.delay_between_requests}s (rate limiting)...")
                    time.sleep(self.delay_between_requests)
            
            # Commit final
            session.commit()
        
        self.stats['end_time'] = datetime.now()
        self._print_final_report()
    
    def process_single_park_by_id(self, park_id: int):
        """
        Processa um único parque específico (útil para testes ou reprocessamento).
        
        Args:
            park_id: ID do parque na tabela parks_master
        """
        logger.info(f"🎯 Processando parque específico: ID {park_id}")
        
        with get_db_session() as session:
            park = self._get_park_by_id(session, park_id)
            
            if not park:
                logger.error(f"❌ Parque {park_id} não encontrado")
                return
            
            self._process_single_park(session, park)
            session.commit()
            
            logger.info("✅ Processamento concluído")
    
    # ========================================================================
    # PROCESSAMENTO DE PARQUE INDIVIDUAL
    # ========================================================================
    
    def _process_single_park(self, session: Session, park: Dict):
        """
        Processa um único parque: identifica condado, busca proprietário, salva.
        
        Args:
            session: Sessão do SQLAlchemy
            park: Dicionário com dados do parque (da query)
        """
        park_id = park['id']
        park_name = park['name']
        lat = park['latitude']
        lon = park['longitude']
        address = park.get('address', 'Endereço não disponível')
        
        logger.info(f"📍 Parque: {park_name}")
        logger.info(f"   Endereço: {address}")
        logger.info(f"   Coordenadas: ({lat}, {lon})")
        
        # PASSO 1: Identificar condado
        county = self._identify_county(lat, lon)
        
        if not county:
            logger.warning("⚠️ Condado não identificado - pulando parque")
            self.stats['county_not_identified'] += 1
            self.stats['skipped'] += 1
            return
        
        logger.info(f"   🏛️ Condado: {county}")
        
        # PASSO 2: Obter fetcher apropriado
        fetcher = self._get_fetcher(county)
        
        # PASSO 3: Buscar proprietário (com retries)
        result = self._lookup_owner_with_retry(
            fetcher,
            address,
            lat,
            lon
        )
        
        # PASSO 4: Processar resultado
        if result.success and result.found_owner:
            logger.info(f"✅ Proprietário encontrado!")
            
            for owner_record in result.records:
                # Salvar proprietário
                owner_id = self._save_owner(session, owner_record)
                
                # Atualizar parks_master
                self._update_park_owner(session, park_id, owner_id)
                
                logger.info(f"   💾 Salvo: {owner_record.owner_name_1}")
            
            self.stats['successful'] += 1
            self.stats['owner_found'] += 1
        
        else:
            logger.warning(f"⚠️ Proprietário não encontrado: {result.error_message}")
            self.stats['owner_not_found'] += 1
            
            # Marcar parque para revisão manual
            self._mark_for_manual_review(session, park_id, result.error_message)
    
    # ========================================================================
    # HELPERS - IDENTIFICAÇÃO E BUSCA
    # ========================================================================
    
    def _identify_county(self, lat: float, lon: float) -> Optional[str]:
        """Identifica condado usando county_mapper."""
        try:
            county = self.county_mapper.identify_county(lat, lon)
            return county
        except Exception as e:
            logger.error(f"Erro ao identificar condado: {e}")
            return None
    
    def _get_fetcher(self, county: str) -> CountyAssessorFetcher:
        """
        Obtém fetcher apropriado para o condado (com cache).
        
        Args:
            county: Nome do condado
        
        Returns:
            Instância de fetcher (reutilizada se já existir)
        """
        if county not in self._fetcher_cache:
            logger.debug(f"Criando novo fetcher para {county}")
            self._fetcher_cache[county] = get_fetcher_for_county(
                county,
                use_mock=self.use_mock
            )
        
        return self._fetcher_cache[county]
    
    def _lookup_owner_with_retry(
        self,
        fetcher: CountyAssessorFetcher,
        address: str,
        lat: float,
        lon: float
    ) -> FetchResult:
        """
        Busca proprietário com retries em caso de erro.
        
        Implementa backoff exponencial: 1s, 2s, 4s, 8s...
        """
        for attempt in range(1, self.max_retries + 1):
            try:
                logger.debug(f"Tentativa {attempt}/{self.max_retries}")
                
                result = fetcher.lookup_owner(address, lat, lon)
                
                if result.success:
                    return result
                
                # Se não teve sucesso mas também não é erro de rede, não retry
                if "rate limit" not in result.error_message.lower():
                    return result
                
                # Rate limited - aguardar antes de retry
                if result.retry_after_seconds:
                    wait_time = result.retry_after_seconds
                else:
                    wait_time = 2 ** (attempt - 1)  # Backoff exponencial
                
                logger.warning(f"⚠️ Rate limited. Aguardando {wait_time}s antes de retry...")
                time.sleep(wait_time)
            
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt}: {e}")
                
                if attempt == self.max_retries:
                    return FetchResult(
                        success=False,
                        error_message=f"Falha após {self.max_retries} tentativas: {e}"
                    )
                
                # Backoff exponencial
                wait_time = 2 ** (attempt - 1)
                logger.debug(f"Aguardando {wait_time}s antes de retry...")
                time.sleep(wait_time)
        
        return FetchResult(
            success=False,
            error_message=f"Esgotadas {self.max_retries} tentativas"
        )
    
    # ========================================================================
    # HELPERS - BANCO DE DADOS
    # ========================================================================
    
    def _get_parks_without_owner(
        self,
        session: Session,
        limit: Optional[int] = None
    ) -> List[Dict]:
        """
        Busca parques que ainda não têm proprietário identificado.
        
        Returns:
            Lista de dicionários com dados dos parques
        """
        query = """
            SELECT 
                id,
                name,
                ST_Y(geom::geometry) as latitude,
                ST_X(geom::geometry) as longitude,
                address,
                city,
                county,
                zip_code
            FROM parks_master
            WHERE owner_id IS NULL
              AND needs_manual_review = FALSE
            ORDER BY id
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        result = session.execute(text(query))
        
        parks = []
        for row in result:
            # Índices corretos: 0=id, 1=name, 2=lat, 3=lon, 4=address, 5=city, 6=county, 7=zip_code
            address_parts = [row[4] or '']  # address
            if row[5]:  # city
                address_parts.append(row[5])
            if row[6]:  # county
                address_parts.append(f"{row[6]} County")
            if row[7]:  # zip_code
                address_parts.append(row[7])
            
            full_address = ', '.join(p for p in address_parts if p) or 'N/A'
            
            parks.append({
                'id': row[0],
                'name': row[1],
                'latitude': row[2],
                'longitude': row[3],
                'address': full_address,
                'city': row[5] or '',
                'county': row[6] or '',
                'zip_code': row[7] or ''
            })
        
        return parks
    
    def _get_park_by_id(self, session: Session, park_id: int) -> Optional[Dict]:
        """Busca um parque específico por ID."""
        query = """
            SELECT 
                id,
                name,
                ST_Y(geom::geometry) as latitude,
                ST_X(geom::geometry) as longitude,
                address,
                city,
                county,
                zip_code
            FROM parks_master
            WHERE id = :park_id
        """
        
        result = session.execute(text(query), {'park_id': park_id}).fetchone()
        
        if result:
            # Construir endereço completo
            address_parts = [result[4] or '']  # address
            if result[5]:  # city
                address_parts.append(result[5])
            if result[6]:  # county
                address_parts.append(f"{result[6]} County")
            if result[7]:  # zip_code
                address_parts.append(result[7])
            
            full_address = ', '.join(p for p in address_parts if p) or 'N/A'
            
            return {
                'id': result[0],
                'name': result[1],
                'latitude': result[2],
                'longitude': result[3],
                'address': full_address,
                'city': result[5] or '',
                'county': result[6] or '',
                'zip_code': result[7] or ''
            }
        
        return None
    
    def _save_owner(self, session: Session, owner_record: OwnerRecord) -> int:
        """
        Salva proprietário na tabela owners.
        
        Returns:
            ID do proprietário salvo
        """
        # Verificar se já existe (por mailing address)
        query = """
            SELECT id FROM owners
            WHERE full_name = :name
              AND mailing_address->>'line1' = :addr_line1
              AND mailing_address->>'zip' = :zip
            LIMIT 1
        """
        
        existing = session.execute(text(query), {
            'name': owner_record.owner_name_1,
            'addr_line1': owner_record.mailing_address_line1,
            'zip': owner_record.mailing_zip
        }).fetchone()
        
        if existing:
            logger.debug(f"Proprietário já existe: ID {existing[0]}")
            return existing[0]
        
        # Inserir novo
        insert_query = """
            INSERT INTO owners (
                full_name,
                mailing_address,
                metadata,
                mail_eligible,
                created_at
            ) VALUES (
                :name,
                :mailing_address::jsonb,
                :metadata::jsonb,
                :mail_eligible,
                NOW()
            )
            RETURNING id
        """
        
        mailing_address_json = {
            'line1': owner_record.mailing_address_line1,
            'line2': owner_record.mailing_address_line2,
            'city': owner_record.mailing_city,
            'state': owner_record.mailing_state,
            'zip': owner_record.mailing_zip,
            'country': owner_record.mailing_country
        }
        
        metadata_json = {
            'source': owner_record.source,
            'source_url': owner_record.source_url,
            'fetched_at': owner_record.fetched_at.isoformat(),
            'confidence_score': owner_record.confidence_score,
            'parcel_id': owner_record.parcel_id,
            'property_class_code': owner_record.property_class_code,
            'notes': owner_record.notes
        }
        
        result = session.execute(text(insert_query), {
            'name': owner_record.owner_name_1,
            'mailing_address': str(mailing_address_json).replace("'", '"'),
            'metadata': str(metadata_json).replace("'", '"'),
            'mail_eligible': owner_record.is_valid_mailing_address
        }).fetchone()
        
        owner_id = result[0]
        logger.debug(f"Novo proprietário inserido: ID {owner_id}")
        
        return owner_id
    
    def _update_park_owner(self, session: Session, park_id: int, owner_id: int):
        """Atualiza parks_master com o owner_id."""
        query = """
            UPDATE parks_master
            SET owner_id = :owner_id,
                updated_at = NOW()
            WHERE id = :park_id
        """
        
        session.execute(text(query), {
            'park_id': park_id,
            'owner_id': owner_id
        })
        
        logger.debug(f"Parque {park_id} atualizado com owner {owner_id}")
    
    def _mark_for_manual_review(
        self,
        session: Session,
        park_id: int,
        reason: str
    ):
        """Marca parque para revisão manual."""
        query = """
            UPDATE parks_master
            SET needs_manual_review = TRUE,
                updated_at = NOW()
            WHERE id = :park_id
        """
        
        session.execute(text(query), {
            'park_id': park_id
        })
        
        logger.debug(f"Parque {park_id} marcado para revisão manual")
    
    # ========================================================================
    # RELATÓRIOS
    # ========================================================================
    
    def _print_final_report(self):
        """Imprime relatório final do processamento."""
        duration = (self.stats['end_time'] - self.stats['start_time']).total_seconds()
        
        logger.info("")
        logger.info("=" * 80)
        logger.info("RELATÓRIO FINAL - OWNER LOOKUP")
        logger.info("=" * 80)
        logger.info(f"Total de parques: {self.stats['total_parks']}")
        logger.info(f"Processados: {self.stats['processed']}")
        logger.info(f"Sucessos: {self.stats['successful']}")
        logger.info(f"Falhas: {self.stats['failed']}")
        logger.info(f"Pulados: {self.stats['skipped']}")
        logger.info("")
        logger.info(f"Proprietários encontrados: {self.stats['owner_found']}")
        logger.info(f"Proprietários NÃO encontrados: {self.stats['owner_not_found']}")
        logger.info(f"Condados não identificados: {self.stats['county_not_identified']}")
        logger.info("")
        logger.info(f"Duração: {duration:.1f}s ({duration/60:.1f} minutos)")
        
        if self.stats['processed'] > 0:
            avg_time = duration / self.stats['processed']
            logger.info(f"Tempo médio por parque: {avg_time:.2f}s")
            
            success_rate = (self.stats['owner_found'] / self.stats['processed']) * 100
            logger.info(f"Taxa de sucesso: {success_rate:.1f}%")
        
        logger.info("=" * 80)
        
        # Estatísticas por fetcher
        logger.info("\n📊 ESTATÍSTICAS POR CONDADO:")
        logger.info("-" * 80)
        for county, fetcher in self._fetcher_cache.items():
            stats = fetcher.get_statistics()
            logger.info(f"{county}:")
            for key, value in stats.items():
                logger.info(f"  {key}: {value}")
        
        logger.info("=" * 80)


# ============================================================================
# STANDALONE EXECUTION
# ============================================================================

if __name__ == "__main__":
    """
    Execução standalone para testes.
    
    Uso:
        # Modo MOCK (sem consumir APIs)
        python src/owners/orchestrator.py --mock --limit 5
        
        # Modo PRODUÇÃO (cuidado!)
        python src/owners/orchestrator.py --limit 10
    """
    import argparse
    
    parser = argparse.ArgumentParser(description="Owner Lookup Orchestrator")
    parser.add_argument('--mock', action='store_true', help='Usar MockFetcher (desenvolvimento)')
    parser.add_argument('--limit', type=int, help='Limitar número de parques processados')
    parser.add_argument('--park-id', type=int, help='Processar apenas um parque específico')
    
    args = parser.parse_args()
    
    # Criar orchestrator
    orchestrator = OwnerLookupOrchestrator(
        use_mock=args.mock,
        max_retries=3,
        delay_between_requests=3.0 if not args.mock else 0.5,  # Delays menores para mock
        checkpoint_interval=10
    )
    
    # Processar
    if args.park_id:
        orchestrator.process_single_park_by_id(args.park_id)
    else:
        orchestrator.process_all_parks(limit=args.limit)
    
    print("\n✅ Processamento concluído! Verifique os logs para detalhes.")
