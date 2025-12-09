"""
Módulo de ingestão de dados do OpenStreetMap via Overpass API.
Respeita rate limits e conformidade legal.
"""
import os
import time
import yaml
from typing import List, Dict, Any, Optional
from pathlib import Path
import requests
from loguru import logger
from dotenv import load_dotenv

from ..models import OSMElement, ParkRawData, StateConfig

load_dotenv()


class OSMQueryBuilder:
    """Construtor de queries Overpass QL."""
    
    def __init__(self, state_config: StateConfig):
        self.config = state_config
        self.bbox = state_config.bbox
    
    def build_query(self) -> str:
        """
        Constrói query Overpass QL para parques em Indiana.
        
        Busca por:
        - tourism=camp_site (campgrounds)
        - tourism=caravan_site (RV parks)
        - landuse=residential + residential=mobile_home_park
        - landuse=residential + residential=trailer_park
        """
        
        # Bounding box: (min_lat, min_lon, max_lat, max_lon)
        bbox_str = f"{self.bbox['min_lat']},{self.bbox['min_lon']},{self.bbox['max_lat']},{self.bbox['max_lon']}"
        
        query = f"""
[out:json][timeout:90];
(
  // Camp sites (campgrounds)
  node["tourism"="camp_site"]({bbox_str});
  way["tourism"="camp_site"]({bbox_str});
  relation["tourism"="camp_site"]({bbox_str});
  
  // Caravan sites (RV parks)
  node["tourism"="caravan_site"]({bbox_str});
  way["tourism"="caravan_site"]({bbox_str});
  relation["tourism"="caravan_site"]({bbox_str});
  
  // Mobile home parks
  node["landuse"="residential"]["residential"="mobile_home"]({bbox_str});
  way["landuse"="residential"]["residential"="mobile_home"]({bbox_str});
  relation["landuse"="residential"]["residential"="mobile_home"]({bbox_str});
  
  // Trailer parks
  node["landuse"="residential"]["residential"="trailer_park"]({bbox_str});
  way["landuse"="residential"]["residential"="trailer_park"]({bbox_str});
  relation["landuse"="residential"]["residential"="trailer_park"]({bbox_str});
  
  // Busca adicional por nome (pode conter "mobile home park", "rv park", etc)
  node["name"~"mobile home|trailer park|rv park|rv resort",i]({bbox_str});
  way["name"~"mobile home|trailer park|rv park|rv resort",i]({bbox_str});
);
out center;
out meta;
"""
        return query.strip()


class OverpassAPI:
    """Cliente para Overpass API com rate limiting."""
    
    def __init__(self):
        self.base_url = os.getenv(
            "OVERPASS_API_URL",
            "https://overpass-api.de/api/interpreter"
        )
        self.rate_limit_seconds = float(os.getenv("OVERPASS_RATE_LIMIT", "1"))
        self.last_request_time = 0
    
    def _respect_rate_limit(self):
        """Aplica rate limiting entre requisições."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.rate_limit_seconds:
            sleep_time = self.rate_limit_seconds - elapsed
            logger.debug(f"Rate limiting: aguardando {sleep_time:.2f}s")
            time.sleep(sleep_time)
    
    def execute_query(self, query: str, timeout: int = 90) -> Dict[str, Any]:
        """
        Executa query Overpass QL.
        
        Args:
            query: Query Overpass QL
            timeout: Timeout em segundos
            
        Returns:
            Resposta JSON da API
            
        Raises:
            requests.RequestException: Em caso de erro na requisição
        """
        self._respect_rate_limit()
        
        logger.info("Executando query Overpass API...")
        logger.debug(f"Query: {query[:200]}...")
        
        try:
            response = requests.post(
                self.base_url,
                data={'data': query},
                timeout=timeout,
                headers={
                    'User-Agent': 'MHP-BI-Research/1.0 (Legal Compliance)',
                    'Accept': 'application/json'
                }
            )
            
            self.last_request_time = time.time()
            
            response.raise_for_status()
            data = response.json()
            
            logger.info(f"Query executada com sucesso. Elementos retornados: {len(data.get('elements', []))}")
            
            return data
            
        except requests.Timeout:
            logger.error(f"Timeout após {timeout}s aguardando resposta da Overpass API")
            raise
        except requests.RequestException as e:
            logger.error(f"Erro na requisição Overpass API: {e}")
            raise
        except ValueError as e:
            logger.error(f"Erro ao parsear JSON da resposta: {e}")
            raise


def fetch_osm_parks(state_config: StateConfig) -> List[ParkRawData]:
    """
    Busca parques do OpenStreetMap para o estado configurado.
    
    Args:
        state_config: Configuração do estado (carregada de indiana.yaml)
        
    Returns:
        Lista de objetos ParkRawData prontos para inserção no banco
        
    Example:
        >>> import yaml
        >>> from models import StateConfig
        >>> 
        >>> with open('config/indiana.yaml') as f:
        >>>     config_dict = yaml.safe_load(f)
        >>> state_config = StateConfig(**config_dict)
        >>> 
        >>> parks = fetch_osm_parks(state_config)
        >>> print(f"Encontrados {len(parks)} parques no OSM")
    """
    
    logger.info(f"Iniciando busca OSM para {state_config.state['name']}")
    
    # Construir query
    query_builder = OSMQueryBuilder(state_config)
    query = query_builder.build_query()
    
    # Executar query
    api = OverpassAPI()
    
    try:
        response = api.execute_query(query)
    except Exception as e:
        logger.error(f"Falha ao executar query OSM: {e}")
        return []
    
    # Parsear elementos
    elements = response.get('elements', [])
    logger.info(f"Parseando {len(elements)} elementos do OSM...")
    
    parks_raw = []
    skipped = 0
    
    for element_data in elements:
        try:
            # Validar elemento com Pydantic
            osm_element = OSMElement(**element_data)
            
            # Converter para ParkRawData
            park_raw = osm_element.to_park_raw()
            
            # Validar que tem coordenadas
            if park_raw.latitude is None or park_raw.longitude is None:
                logger.warning(
                    f"Elemento OSM {osm_element.type}/{osm_element.id} "
                    f"sem coordenadas - pulando"
                )
                skipped += 1
                continue
            
            parks_raw.append(park_raw)
            
        except Exception as e:
            logger.warning(f"Erro ao processar elemento OSM: {e}")
            skipped += 1
            continue
    
    logger.info(
        f"OSM fetch completo: {len(parks_raw)} parques válidos, "
        f"{skipped} elementos pulados"
    )
    
    return parks_raw


def load_state_config(config_path: str = "config/indiana.yaml") -> StateConfig:
    """
    Carrega configuração do estado a partir do arquivo YAML.
    
    Args:
        config_path: Caminho para o arquivo de configuração
        
    Returns:
        StateConfig validado
    """
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config_dict = yaml.safe_load(f)
    
    return StateConfig(**config_dict)


def main():
    """Exemplo de uso do módulo."""
    
    # Configurar logging
    logger.add(
        "logs/osm_ingestion_{time}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO"
    )
    
    # Carregar configuração
    logger.info("Carregando configuração do estado...")
    state_config = load_state_config()
    
    # Buscar parques
    parks = fetch_osm_parks(state_config)
    
    # Exibir resumo
    logger.info(f"\n{'='*60}")
    logger.info(f"RESUMO DA INGESTÃO OSM")
    logger.info(f"{'='*60}")
    logger.info(f"Total de parques encontrados: {len(parks)}")
    
    if parks:
        # Estatísticas por tipo
        types_count = {}
        for park in parks:
            park_type = park.park_type or 'unknown'
            types_count[park_type] = types_count.get(park_type, 0) + 1
        
        logger.info("\nDistribuição por tipo:")
        for park_type, count in sorted(types_count.items()):
            logger.info(f"  {park_type}: {count}")
        
        # Exibir alguns exemplos
        logger.info("\nPrimeiros 5 parques:")
        for i, park in enumerate(parks[:5], 1):
            logger.info(
                f"  {i}. {park.name or 'Sem nome'} "
                f"({park.park_type}) - {park.city or 'N/A'}, {park.state}"
            )
    
    logger.info(f"{'='*60}\n")
    
    # Próximo passo: inserir no banco de dados
    logger.info(
        "Para inserir estes dados no banco, use o módulo database.py "
        "e insira na tabela parks_raw"
    )
    
    return parks


if __name__ == "__main__":
    main()
