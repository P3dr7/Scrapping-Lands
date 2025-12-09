#!/usr/bin/env python3
"""
Script para gerar os 3 CSVs solicitados:
1. dados_propriedades.csv - Apenas dados das propriedades
2. propriedades_donos.csv - Propriedades + donos identificados
3. propriedades_donos_contatos.csv - Dados completos

Author: BellaTerra Intelligence
Date: December 2025
"""

import sys
from pathlib import Path
from datetime import datetime
import pandas as pd
from loguru import logger
from sqlalchemy import text

# Adicionar src ao path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database import get_db_session, get_engine


def generate_properties_only_csv(output_dir: Path, state_filter: str = None) -> str:
    """
    CSV 1: Apenas dados das propriedades.
    
    Colunas: nome, tipo, endereco, cidade, estado, cep, condado, lat, lon, 
             status, rating, reviews, telefone, website
    """
    logger.info("📊 Gerando CSV 1: Dados das Propriedades...")
    
    state_clause = f"AND state = '{state_filter}'" if state_filter else ""
    
    query = f"""
    SELECT 
        name AS nome_propriedade,
        park_type AS tipo,
        address AS endereco,
        city AS cidade,
        state AS estado,
        zip_code AS cep,
        county AS condado,
        latitude AS lat,
        longitude AS lon,
        business_status AS status,
        avg_rating AS avaliacao_media,
        total_reviews AS total_avaliacoes,
        phone AS telefone,
        website
    FROM parks_master
    WHERE 1=1 {state_clause}
    ORDER BY state, city, name
    """
    
    with get_db_session() as session:
        result = session.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()
    
    df = pd.DataFrame(rows, columns=columns)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{state_filter}" if state_filter else ""
    filename = f"dados_propriedades{suffix}_{timestamp}.csv"
    filepath = output_dir / filename
    
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    
    logger.success(f"✅ CSV 1 gerado: {filepath}")
    logger.info(f"   Total de registros: {len(df)}")
    
    return str(filepath)


def generate_properties_with_owners_csv(output_dir: Path, state_filter: str = None) -> str:
    """
    CSV 2: Propriedades + donos identificados.
    
    Para propriedades sem owner identificado, usa o nome do negócio como proxy.
    """
    logger.info("📊 Gerando CSV 2: Propriedades + Donos...")
    
    state_clause = f"AND pm.state = '{state_filter}'" if state_filter else ""
    
    query = f"""
    SELECT 
        pm.name AS nome_propriedade,
        pm.park_type AS tipo,
        pm.address AS endereco,
        pm.city AS cidade,
        pm.state AS estado,
        pm.zip_code AS cep,
        pm.county AS condado,
        pm.latitude AS lat,
        pm.longitude AS lon,
        pm.business_status AS status,
        pm.avg_rating AS avaliacao_media,
        pm.total_reviews AS total_avaliacoes,
        -- Dados do proprietário (se existir)
        COALESCE(o.full_name, pm.name) AS nome_proprietario,
        CASE WHEN o.is_individual THEN 'individual' ELSE 'business' END AS tipo_proprietario,
        o.mailing_address AS endereco_correspondencia,
        o.mailing_city AS cidade_correspondencia,
        o.mailing_state AS estado_correspondencia,
        o.mailing_zip AS cep_correspondencia
    FROM parks_master pm
    LEFT JOIN owners o ON pm.owner_id = o.id
    WHERE 1=1 {state_clause}
    ORDER BY pm.state, pm.city, pm.name
    """
    
    with get_db_session() as session:
        result = session.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()
    
    df = pd.DataFrame(rows, columns=columns)
    
    # Preencher endereço de correspondência com endereço do parque se vazio
    df['endereco_correspondencia'] = df['endereco_correspondencia'].fillna(df['endereco'])
    df['cidade_correspondencia'] = df['cidade_correspondencia'].fillna(df['cidade'])
    df['estado_correspondencia'] = df['estado_correspondencia'].fillna(df['estado'])
    df['cep_correspondencia'] = df['cep_correspondencia'].fillna(df['cep'])
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{state_filter}" if state_filter else ""
    filename = f"propriedades_donos{suffix}_{timestamp}.csv"
    filepath = output_dir / filename
    
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    
    logger.success(f"✅ CSV 2 gerado: {filepath}")
    logger.info(f"   Total de registros: {len(df)}")
    
    # Stats
    with_owner = df[df['nome_proprietario'] != df['nome_propriedade']].shape[0]
    logger.info(f"   Com proprietário identificado: {with_owner}")
    logger.info(f"   Usando nome do negócio: {len(df) - with_owner}")
    
    return str(filepath)


def generate_full_csv(output_dir: Path, state_filter: str = None) -> str:
    """
    CSV 3: Dados completos - Propriedades + Donos + Contatos.
    
    Inclui telefone, email e website para direct mail e marketing digital.
    """
    logger.info("📊 Gerando CSV 3: Dados Completos (Propriedades + Donos + Contatos)...")
    
    state_clause = f"AND pm.state = '{state_filter}'" if state_filter else ""
    
    query = f"""
    SELECT 
        -- Dados da propriedade
        pm.master_id,
        pm.name AS nome_propriedade,
        pm.park_type AS tipo,
        pm.address AS endereco_propriedade,
        pm.city AS cidade,
        pm.state AS estado,
        pm.zip_code AS cep,
        pm.county AS condado,
        pm.latitude AS lat,
        pm.longitude AS lon,
        pm.business_status AS status,
        pm.avg_rating AS avaliacao_media,
        pm.total_reviews AS total_avaliacoes,
        pm.phone AS telefone_propriedade,
        pm.website AS website_propriedade,
        pm.email AS email_propriedade,
        -- Dados do proprietário
        COALESCE(o.full_name, pm.name) AS nome_proprietario,
        CASE WHEN o.is_individual THEN 'individual' ELSE 'business' END AS tipo_proprietario,
        COALESCE(o.mailing_address, pm.address) AS endereco_correspondencia,
        COALESCE(o.mailing_city, pm.city) AS cidade_correspondencia,
        COALESCE(o.mailing_state, pm.state) AS estado_correspondencia,
        COALESCE(o.mailing_zip, pm.zip_code) AS cep_correspondencia,
        -- Dados de contato do proprietário/empresa
        c.person_name AS nome_contato,
        c.person_title AS cargo_contato,
        c.email AS email_contato,
        c.phone AS telefone_contato,
        -- Dados da empresa (se corporativo)
        comp.legal_name AS nome_empresa,
        comp.company_type AS tipo_negocio,
        comp.registered_agent_name AS agente_registrado,
        comp.sos_formation_date AS data_fundacao
    FROM parks_master pm
    LEFT JOIN owners o ON pm.owner_id = o.id
    LEFT JOIN contacts c ON pm.id = c.park_id
    LEFT JOIN companies comp ON pm.company_id = comp.id
    WHERE 1=1 {state_clause}
    ORDER BY pm.state, pm.city, pm.name
    """
    
    with get_db_session() as session:
        result = session.execute(text(query))
        rows = result.fetchall()
        columns = result.keys()
    
    df = pd.DataFrame(rows, columns=columns)
    
    # Consolidar dados de contato (preferir contato do proprietário, senão usar da propriedade)
    df['telefone_final'] = df['telefone_contato'].fillna(df['telefone_propriedade'])
    df['email_final'] = df['email_contato'].fillna(df['email_propriedade'])
    df['website_final'] = df['website_propriedade']
    
    # Classificar qualidade do lead
    def classify_lead(row):
        has_name = pd.notna(row['nome_proprietario']) and row['nome_proprietario'] != ''
        has_address = pd.notna(row['endereco_correspondencia']) and row['endereco_correspondencia'] != ''
        has_contact = (pd.notna(row['telefone_final']) or pd.notna(row['email_final']))
        
        if has_name and has_address and has_contact:
            return 'A'  # Tier A: Completo
        elif has_name and has_address:
            return 'B'  # Tier B: Nome + Endereço
        elif has_address:
            return 'C'  # Tier C: Apenas endereço
        else:
            return 'X'  # Inválido
    
    df['tier_qualidade'] = df.apply(classify_lead, axis=1)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    suffix = f"_{state_filter}" if state_filter else ""
    filename = f"propriedades_donos_contatos{suffix}_{timestamp}.csv"
    filepath = output_dir / filename
    
    df.to_csv(filepath, index=False, encoding='utf-8-sig')
    
    logger.success(f"✅ CSV 3 gerado: {filepath}")
    logger.info(f"   Total de registros: {len(df)}")
    
    # Stats por tier
    tier_counts = df['tier_qualidade'].value_counts()
    logger.info(f"   Tier A (completo): {tier_counts.get('A', 0)}")
    logger.info(f"   Tier B (nome+endereço): {tier_counts.get('B', 0)}")
    logger.info(f"   Tier C (só endereço): {tier_counts.get('C', 0)}")
    
    # Stats de contato
    with_phone = df['telefone_final'].notna().sum()
    with_email = df['email_final'].notna().sum()
    with_website = df['website_final'].notna().sum()
    logger.info(f"   Com telefone: {with_phone}")
    logger.info(f"   Com email: {with_email}")
    logger.info(f"   Com website: {with_website}")
    
    return str(filepath)


def main():
    """Função principal."""
    print("=" * 80)
    print("GERAÇÃO DE CSVs PARA DIRECT MAIL")
    print("=" * 80)
    print()
    
    # Configurar logging
    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)
    
    # Diretório de saída
    output_dir = Path(__file__).parent.parent / "output"
    output_dir.mkdir(exist_ok=True)
    
    # Perguntar filtro de estado
    print("Filtrar por estado?")
    print("  1. Apenas Indiana (IN)")
    print("  2. Todos os estados")
    
    choice = input("\nEscolha (1/2) [1]: ").strip() or "1"
    state_filter = "IN" if choice == "1" else None
    
    if state_filter:
        logger.info(f"📍 Filtrando apenas: {state_filter}")
    else:
        logger.info("📍 Incluindo todos os estados")
    
    print()
    
    # Gerar os 3 CSVs
    csv1 = generate_properties_only_csv(output_dir, state_filter)
    print()
    
    csv2 = generate_properties_with_owners_csv(output_dir, state_filter)
    print()
    
    csv3 = generate_full_csv(output_dir, state_filter)
    print()
    
    # Resumo final
    print("=" * 80)
    print("✅ GERAÇÃO CONCLUÍDA!")
    print("=" * 80)
    print()
    print("Arquivos gerados:")
    print(f"  1. {csv1}")
    print(f"  2. {csv2}")
    print(f"  3. {csv3}")
    print()
    print("=" * 80)


if __name__ == "__main__":
    main()
