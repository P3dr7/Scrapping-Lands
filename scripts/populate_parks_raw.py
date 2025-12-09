"""
Script para popular a tabela parks_raw com dados do OSM e Google Places.
Execute após configurar o banco de dados e as variáveis de ambiente.
"""
import sys
from pathlib import Path
from typing import List
import yaml
from loguru import logger
from sqlalchemy import text

# Adicionar src ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root / "src"))

from models import StateConfig, ParkRawData
from database import get_db_session, test_connection
from ingestion.osm_query import fetch_osm_parks
from ingestion.google_places import fetch_google_parks


def load_config(config_path: str = "config/indiana.yaml") -> StateConfig:
    """Carrega configuração do estado."""
    with open(config_path, 'r', encoding='utf-8') as f:
        config_dict = yaml.safe_load(f)
    return StateConfig(**config_dict)


def insert_parks_to_db(parks: List[ParkRawData], batch_size: int = 100):
    """
    Insere parques na tabela parks_raw.
    
    Args:
        parks: Lista de ParkRawData
        batch_size: Tamanho do batch para inserção
    """
    if not parks:
        logger.warning("Nenhum parque para inserir")
        return
    
    logger.info(f"Inserindo {len(parks)} parques na tabela parks_raw...")
    
    insert_sql = text("""
        INSERT INTO parks_raw (
            external_id, source, name, park_type,
            address, city, state, zip_code, county,
            latitude, longitude, geom,
            phone, website, email,
            business_status, rating, total_reviews,
            raw_data, tags, fetched_at, is_processed
        ) VALUES (
            :external_id, :source, :name, :park_type,
            :address, :city, :state, :zip_code, :county,
            :latitude, :longitude, 
            ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326)::geography,
            :phone, :website, :email,
            :business_status, :rating, :total_reviews,
            :raw_data::jsonb, :tags::jsonb, :fetched_at, :is_processed
        )
        ON CONFLICT (external_id, source) DO UPDATE SET
            name = EXCLUDED.name,
            park_type = EXCLUDED.park_type,
            address = EXCLUDED.address,
            city = EXCLUDED.city,
            state = EXCLUDED.state,
            zip_code = EXCLUDED.zip_code,
            county = EXCLUDED.county,
            latitude = EXCLUDED.latitude,
            longitude = EXCLUDED.longitude,
            geom = EXCLUDED.geom,
            phone = EXCLUDED.phone,
            website = EXCLUDED.website,
            email = EXCLUDED.email,
            business_status = EXCLUDED.business_status,
            rating = EXCLUDED.rating,
            total_reviews = EXCLUDED.total_reviews,
            raw_data = EXCLUDED.raw_data,
            tags = EXCLUDED.tags,
            updated_at = CURRENT_TIMESTAMP
    """)
    
    inserted = 0
    updated = 0
    errors = 0
    
    with get_db_session() as session:
        for i in range(0, len(parks), batch_size):
            batch = parks[i:i+batch_size]
            
            for park in batch:
                try:
                    # Converter para dict
                    park_dict = park.model_dump()
                    
                    # Converter raw_data e tags para JSON string se necessário
                    import json
                    if park_dict.get('raw_data'):
                        park_dict['raw_data'] = json.dumps(park_dict['raw_data'])
                    if park_dict.get('tags'):
                        park_dict['tags'] = json.dumps(park_dict['tags'])
                    
                    # Executar insert
                    session.execute(insert_sql, park_dict)
                    inserted += 1
                    
                except Exception as e:
                    logger.error(f"Erro ao inserir {park.external_id}: {e}")
                    errors += 1
            
            # Commit do batch
            session.commit()
            logger.info(f"Batch {i//batch_size + 1}: {len(batch)} registros processados")
    
    logger.success(
        f"Inserção completa: {inserted} inseridos/atualizados, {errors} erros"
    )


def main():
    """Execução principal."""
    
    # Configurar logging
    logger.add(
        "logs/populate_db_{time}.log",
        rotation="1 day",
        level="INFO"
    )
    
    logger.info("="*60)
    logger.info("POPULANDO BANCO DE DADOS - parks_raw")
    logger.info("="*60)
    
    # Testar conexão
    logger.info("\n1. Testando conexão com banco de dados...")
    if not test_connection():
        logger.error("Falha na conexão. Verifique o arquivo .env e o banco PostgreSQL")
        return
    
    # Carregar configuração
    logger.info("\n2. Carregando configuração do estado...")
    config = load_config()
    logger.info(f"Estado: {config.state['name']}")
    
    # Escolher fonte
    print("\nEscolha a fonte de dados:")
    print("1 - OpenStreetMap (OSM) - GRATUITO")
    print("2 - Google Places API - REQUER API KEY E TEM CUSTO")
    print("3 - Ambas (OSM primeiro, depois Google Places)")
    
    choice = input("\nOpção (1/2/3): ").strip()
    
    all_parks = []
    
    # OSM
    if choice in ['1', '3']:
        logger.info("\n3. Buscando dados do OpenStreetMap...")
        try:
            osm_parks = fetch_osm_parks(config)
            all_parks.extend(osm_parks)
            logger.success(f"OSM: {len(osm_parks)} parques encontrados")
        except Exception as e:
            logger.error(f"Erro na busca OSM: {e}")
    
    # Google Places
    if choice in ['2', '3']:
        logger.info("\n4. Buscando dados do Google Places...")
        logger.warning(
            "ATENÇÃO: Esta operação irá consumir quota da Google Places API "
            "e pode gerar custos!"
        )
        
        confirm = input("Continuar? (s/N): ").strip().lower()
        
        if confirm == 's':
            try:
                google_parks = fetch_google_parks(
                    config,
                    grid_spacing_km=50  # 50km de espaçamento
                )
                all_parks.extend(google_parks)
                logger.success(f"Google Places: {len(google_parks)} parques encontrados")
            except Exception as e:
                logger.error(f"Erro na busca Google Places: {e}")
        else:
            logger.info("Busca Google Places cancelada")
    
    # Inserir no banco
    if all_parks:
        logger.info(f"\n5. Inserindo {len(all_parks)} parques no banco...")
        insert_parks_to_db(all_parks)
        
        # Estatísticas finais
        with get_db_session() as session:
            result = session.execute(text("SELECT COUNT(*) FROM parks_raw"))
            total = result.scalar()
            
            logger.info("\n" + "="*60)
            logger.success(f"TOTAL DE REGISTROS NA TABELA parks_raw: {total}")
            logger.info("="*60)
    else:
        logger.warning("Nenhum parque encontrado para inserir")
    
    logger.info("\nProcesso concluído!")


if __name__ == "__main__":
    main()
