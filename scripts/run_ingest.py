"""
Script para ingestão automatizada de dados do Google Places.
Executa sem interação para testes E2E.
"""
import sys
from pathlib import Path
import yaml
from loguru import logger
from sqlalchemy import text
import json

# Adicionar src ao path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))
sys.path.insert(0, str(project_root / "src"))

from src.models import StateConfig, ParkRawData
from src.database import get_db_session, test_connection
from src.ingestion.google_places_new import fetch_google_parks


def load_config(config_path: str = "config/indiana.yaml") -> StateConfig:
    """Carrega configuração do estado."""
    with open(config_path, 'r', encoding='utf-8') as f:
        config_dict = yaml.safe_load(f)
    return StateConfig(**config_dict)


def insert_parks_to_db(parks, batch_size: int = 100):
    """Insere parques na tabela parks_raw."""
    if not parks:
        logger.warning("Nenhum parque para inserir")
        return 0
    
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
            CAST(ST_SetSRID(ST_MakePoint(:longitude, :latitude), 4326) AS geography),
            :phone, :website, :email,
            :business_status, :rating, :total_reviews,
            CAST(:raw_data AS jsonb), CAST(:tags AS jsonb), :fetched_at, :is_processed
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
    errors = 0
    
    with get_db_session() as session:
        for i in range(0, len(parks), batch_size):
            batch = parks[i:i+batch_size]
            
            for park in batch:
                try:
                    park_dict = park.model_dump()
                    
                    if park_dict.get('raw_data'):
                        park_dict['raw_data'] = json.dumps(park_dict['raw_data'])
                    if park_dict.get('tags'):
                        park_dict['tags'] = json.dumps(park_dict['tags'])
                    
                    session.execute(insert_sql, park_dict)
                    inserted += 1
                except Exception as e:
                    logger.error(f"Erro ao inserir {park.external_id}: {e}")
                    errors += 1
            
            session.commit()
            logger.info(f"Batch {i//batch_size + 1}: {len(batch)} registros processados")
    
    logger.success(f"Inserção completa: {inserted} inseridos, {errors} erros")
    return inserted


def main():
    """Execução principal - Google Places automatizado."""
    
    logger.info("="*60)
    logger.info("INGESTÃO GOOGLE PLACES - MODO AUTOMATIZADO")
    logger.info("="*60)
    
    # Testar conexão
    if not test_connection():
        logger.error("Falha na conexão com o banco")
        return False
    
    # Carregar config
    config = load_config()
    logger.info(f"Estado: {config.state['name']}")
    
    # Buscar dados do Google Places
    logger.info("Buscando dados do Google Places API...")
    try:
        parks = fetch_google_parks(
            config,
            grid_spacing_km=50  # 50km de espaçamento
        )
        logger.success(f"Google Places: {len(parks)} parques encontrados")
    except Exception as e:
        logger.error(f"Erro na busca Google Places: {e}")
        return False
    
    # Inserir no banco
    if parks:
        count = insert_parks_to_db(parks)
        
        # Estatísticas finais
        with get_db_session() as session:
            total = session.execute(text("SELECT COUNT(*) FROM parks_raw")).scalar()
            logger.info(f"TOTAL DE REGISTROS NA TABELA parks_raw: {total}")
        
        return count > 0
    else:
        logger.warning("Nenhum parque encontrado")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
