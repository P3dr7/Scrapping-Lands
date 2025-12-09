"""
County Mapper for Indiana
==========================

Este módulo identifica o condado de Indiana baseado em coordenadas geográficas.

Indiana possui 92 condados com diferentes sistemas de registros fiscais:
- ~40 condados usam Beacon/Schneider Corp (https://beacon.schneidercorp.com)
- ~25 condados usam sistemas GIS próprios
- ~15 condados usam Vanguard Appraisals
- ~12 condados usam outros sistemas

Fontes de dados geográficos:
- US Census TIGER/Line Shapefiles: https://www.census.gov/geographies/mapping-files/time-series/geo/tiger-line-file.html
- Indiana GIS: https://www.indianamap.org/
- GeoJSON simples: https://github.com/plotly/datasets/blob/master/geojson-counties-fips.json

Author: BellaTerra Intelligence Team
Date: December 2025
"""

import json
import os
from pathlib import Path
from typing import Optional, Tuple, Dict
from functools import lru_cache

from shapely.geometry import Point, shape
from loguru import logger


class CountyMapper:
    """
    Identifica condados de Indiana baseado em coordenadas geográficas.
    
    Usa GeoJSON com limites de condados para fazer point-in-polygon queries.
    Implementa cache para otimizar consultas repetidas.
    """
    
    def __init__(self, geojson_path: Optional[str] = None):
        """
        Inicializa o mapeador de condados.
        
        Args:
            geojson_path: Caminho para arquivo GeoJSON com limites de condados.
                         Se None, usa o arquivo padrão em data/geo/indiana_counties.geojson
        """
        if geojson_path is None:
            # Caminho padrão relativo ao projeto
            project_root = Path(__file__).parent.parent.parent
            geojson_path = project_root / "data" / "geo" / "indiana_counties.geojson"
        
        self.geojson_path = Path(geojson_path)
        self.counties_data = None
        self._load_counties()
    
    def _load_counties(self):
        """
        Carrega o GeoJSON com limites dos condados de Indiana.
        
        Se o arquivo não existir, loga um warning e prepara para fallback.
        """
        if not self.geojson_path.exists():
            logger.warning(
                f"GeoJSON de condados não encontrado: {self.geojson_path}\n"
                "Para obter o arquivo:\n"
                "1. Download: https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json\n"
                "2. Filtrar apenas Indiana (FIPS code 18)\n"
                "3. Salvar em data/geo/indiana_counties.geojson\n"
                "Usando fallback com geopy..."
            )
            self.counties_data = None
            return
        
        try:
            with open(self.geojson_path, 'r', encoding='utf-8') as f:
                geojson = json.load(f)
            
            # Validar estrutura do GeoJSON
            if 'features' not in geojson:
                raise ValueError("GeoJSON inválido: campo 'features' não encontrado")
            
            self.counties_data = geojson['features']
            logger.info(f"✅ Carregados {len(self.counties_data)} condados de Indiana")
            
        except Exception as e:
            logger.error(f"Erro ao carregar GeoJSON: {e}")
            self.counties_data = None
    
    @lru_cache(maxsize=1000)
    def identify_county(self, lat: float, lon: float) -> Optional[str]:
        """
        Identifica o condado de Indiana baseado em coordenadas.
        
        Args:
            lat: Latitude (decimal degrees)
            lon: Longitude (decimal degrees)
        
        Returns:
            Nome do condado (ex: "Marion County") ou None se não encontrado
        
        Example:
            >>> mapper = CountyMapper()
            >>> mapper.identify_county(39.7684, -86.1581)  # Indianapolis
            'Marion County'
        """
        # Se GeoJSON disponível e coordenadas em Indiana, usa point-in-polygon
        if self.counties_data is not None and self._is_in_indiana(lat, lon):
            return self._identify_with_geojson(lat, lon)
        
        # Para todos os outros casos: usar geopy (funciona para qualquer estado)
        return self._identify_with_geopy(lat, lon)
    
    def _is_in_indiana(self, lat: float, lon: float) -> bool:
        """
        Verifica se coordenadas estão aproximadamente dentro de Indiana.
        
        Bounding box de Indiana:
        - Norte: 41.7606° (fronteira com Michigan)
        - Sul: 37.7713° (fronteira com Kentucky)
        - Leste: -84.7844° (fronteira com Ohio)
        - Oeste: -88.0997° (fronteira com Illinois)
        """
        IN_BBOX = {
            'min_lat': 37.7713,
            'max_lat': 41.7606,
            'min_lon': -88.0997,
            'max_lon': -84.7844
        }
        
        return (IN_BBOX['min_lat'] <= lat <= IN_BBOX['max_lat'] and
                IN_BBOX['min_lon'] <= lon <= IN_BBOX['max_lon'])
    
    def _identify_with_geojson(self, lat: float, lon: float) -> Optional[str]:
        """
        Identifica condado usando GeoJSON (método mais preciso).
        
        Complexidade: O(n) onde n = 92 condados (aceitável)
        """
        point = Point(lon, lat)  # Shapely usa (lon, lat)
        
        for feature in self.counties_data:
            try:
                # Criar polígono do condado
                polygon = shape(feature['geometry'])
                
                # Verificar se ponto está dentro do polígono
                if polygon.contains(point):
                    # Extrair nome do condado das propriedades
                    props = feature.get('properties', {})
                    
                    # Tentar diferentes campos comuns em GeoJSON de condados
                    county_name = (
                        props.get('NAME') or 
                        props.get('name') or 
                        props.get('COUNTY') or
                        props.get('NAMELSAD')
                    )
                    
                    if county_name:
                        # Padronizar formato: "Marion County"
                        if not county_name.endswith('County'):
                            county_name = f"{county_name} County"
                        
                        logger.debug(f"Condado identificado: {county_name} para ({lat}, {lon})")
                        return county_name
            
            except Exception as e:
                logger.warning(f"Erro ao processar feature do condado: {e}")
                continue
        
        logger.warning(f"Nenhum condado encontrado para ({lat}, {lon})")
        return None
    
    def _identify_with_geopy(self, lat: float, lon: float) -> Optional[str]:
        """
        Fallback: usa geopy para reverse geocoding (requer internet).
        
        ⚠️ LIMITAÇÕES:
        - Requer conexão com internet
        - Nominatim tem rate limit: 1 req/sec
        - Menos preciso que GeoJSON para limites exatos
        
        Uso apenas quando GeoJSON não disponível.
        """
        try:
            from geopy.geocoders import Nominatim
            from time import sleep
            
            # User agent obrigatório para Nominatim
            geolocator = Nominatim(user_agent="bellaterra_mhp_intelligence/1.0")
            
            # Rate limiting para respeitar ToS do Nominatim
            sleep(1.1)  # Garantir < 1 req/sec
            
            location = geolocator.reverse(f"{lat}, {lon}", exactly_one=True, language='en')
            
            if location and location.raw.get('address'):
                address = location.raw['address']
                county = address.get('county')
                
                if county:
                    # Padronizar formato
                    if not county.endswith('County'):
                        county = f"{county} County"
                    
                    logger.info(f"Condado identificado via geopy: {county}")
                    return county
            
            return None
        
        except ImportError:
            logger.error("geopy não instalado. Instale com: pip install geopy")
            return None
        
        except Exception as e:
            logger.error(f"Erro no fallback geopy: {e}")
            return None
    
    def get_county_info(self, county_name: str) -> Dict[str, any]:
        """
        Retorna informações sobre um condado específico.
        
        Args:
            county_name: Nome do condado (ex: "Marion County")
        
        Returns:
            Dicionário com informações do condado:
            - assessor_system: Sistema usado pelo County Assessor
            - assessor_url: URL do sistema de registros
            - has_online_records: Se tem registros públicos online
            - notes: Observações sobre o sistema
        """
        # Mapeamento de condados para sistemas (amostra dos principais)
        # TODO: Completar com todos os 92 condados
        COUNTY_SYSTEMS = {
            'Marion County': {
                'assessor_system': 'Beacon/Schneider Corp',
                'assessor_url': 'https://beacon.schneidercorp.com/Application.aspx?AppID=231&LayerID=3267&PageTypeID=2&PageID=1574',
                'has_online_records': True,
                'population': 977203,  # Maior condado (Indianapolis)
                'notes': 'Sistema Beacon - requer cuidado com rate limiting'
            },
            'Lake County': {
                'assessor_system': 'Beacon/Schneider Corp',
                'assessor_url': 'https://beacon.schneidercorp.com/Application.aspx?AppID=1018&LayerID=21002&PageTypeID=2&PageID=9480',
                'has_online_records': True,
                'population': 485493,
                'notes': 'Segundo maior condado - sistema Beacon'
            },
            'Allen County': {
                'assessor_system': 'Custom GIS',
                'assessor_url': 'https://maps.acgov.org/Html5Viewer/?viewer=public',
                'has_online_records': True,
                'population': 385410,
                'notes': 'Sistema GIS próprio - Fort Wayne'
            },
            'Hamilton County': {
                'assessor_system': 'Beacon/Schneider Corp',
                'assessor_url': 'https://beacon.schneidercorp.com/Application.aspx?AppID=163&LayerID=2403&PageTypeID=2&PageID=1231',
                'has_online_records': True,
                'population': 347467,
                'notes': 'Condado rico (subúrbio de Indy) - dados completos'
            },
            'St. Joseph County': {
                'assessor_system': 'Beacon/Schneider Corp',
                'assessor_url': 'https://beacon.schneidercorp.com/Application.aspx?AppID=1008&LayerID=20748&PageTypeID=2&PageID=9392',
                'has_online_records': True,
                'population': 272912,
                'notes': 'South Bend - sistema Beacon'
            },
            # Condados menores com sistemas diferentes
            'Brown County': {
                'assessor_system': 'Vanguard Appraisals',
                'assessor_url': 'http://www.vanguardappraisals.com/brown/',
                'has_online_records': True,
                'population': 15092,
                'notes': 'Condado rural - sistema Vanguard'
            },
            'Orange County': {
                'assessor_system': 'Manual/Phone',
                'assessor_url': None,
                'has_online_records': False,
                'population': 19867,
                'notes': 'Sem sistema online - requer contato telefônico'
            }
        }
        
        # Normalizar nome do condado
        if not county_name.endswith('County'):
            county_name = f"{county_name} County"
        
        # Retornar info se disponível, senão genérico
        return COUNTY_SYSTEMS.get(county_name, {
            'assessor_system': 'Unknown',
            'assessor_url': None,
            'has_online_records': None,
            'notes': f'Sistema para {county_name} não mapeado ainda'
        })
    
    def get_statistics(self) -> Dict[str, any]:
        """
        Retorna estatísticas sobre os condados de Indiana.
        
        Returns:
            Dicionário com estatísticas gerais
        """
        return {
            'total_counties': 92,
            'counties_with_beacon': 40,  # Aproximadamente
            'counties_with_custom_gis': 25,
            'counties_with_vanguard': 15,
            'counties_manual_only': 12,
            'geojson_loaded': self.counties_data is not None,
            'geojson_path': str(self.geojson_path),
            'cache_size': self.identify_county.cache_info().currsize if hasattr(self.identify_county, 'cache_info') else 0
        }


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def download_indiana_counties_geojson(output_path: Optional[Path] = None) -> Path:
    """
    Download do GeoJSON de condados de Indiana do US Census.
    
    ⚠️ EXECUTAR APENAS UMA VEZ para setup inicial.
    
    Args:
        output_path: Caminho para salvar o arquivo. Se None, usa data/geo/indiana_counties.geojson
    
    Returns:
        Path do arquivo salvo
    
    Example:
        >>> download_indiana_counties_geojson()
        PosixPath('data/geo/indiana_counties.geojson')
    """
    import requests
    
    if output_path is None:
        project_root = Path(__file__).parent.parent.parent
        output_path = project_root / "data" / "geo" / "indiana_counties.geojson"
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # URL do GeoJSON completo dos EUA (todos os condados)
    # Fonte: Plotly/datasets (mirror do US Census)
    url = "https://raw.githubusercontent.com/plotly/datasets/master/geojson-counties-fips.json"
    
    logger.info(f"Baixando GeoJSON de condados dos EUA...")
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    
    all_counties = response.json()
    
    # Filtrar apenas Indiana (FIPS state code = 18)
    # FIPS codes de Indiana: 18001 a 18183 (92 condados)
    indiana_features = [
        feature for feature in all_counties['features']
        if feature['id'].startswith('18')  # Indiana FIPS code
    ]
    
    indiana_geojson = {
        'type': 'FeatureCollection',
        'features': indiana_features
    }
    
    # Salvar arquivo filtrado
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(indiana_geojson, f, indent=2)
    
    logger.info(f"✅ Salvos {len(indiana_features)} condados de Indiana em {output_path}")
    return output_path


def create_mock_geojson(output_path: Optional[Path] = None) -> Path:
    """
    Cria um GeoJSON MOCK simplificado para testes (apenas alguns condados).
    
    ⚠️ APENAS PARA DESENVOLVIMENTO/TESTES!
    Para produção, use download_indiana_counties_geojson()
    
    Args:
        output_path: Caminho para salvar. Se None, usa data/geo/indiana_counties_mock.geojson
    
    Returns:
        Path do arquivo criado
    """
    if output_path is None:
        project_root = Path(__file__).parent.parent.parent
        output_path = project_root / "data" / "geo" / "indiana_counties_mock.geojson"
    
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Bounding boxes aproximados de condados principais (para testes)
    mock_counties = {
        'type': 'FeatureCollection',
        'features': [
            {
                'type': 'Feature',
                'id': '18097',  # Marion County
                'properties': {'NAME': 'Marion', 'LSAD': 'County'},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [-86.3270, 39.9260],  # NW
                        [-85.9435, 39.9260],  # NE
                        [-85.9435, 39.6370],  # SE
                        [-86.3270, 39.6370],  # SW
                        [-86.3270, 39.9260]   # Fechar polígono
                    ]]
                }
            },
            {
                'type': 'Feature',
                'id': '18089',  # Lake County
                'properties': {'NAME': 'Lake', 'LSAD': 'County'},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [-87.5353, 41.7606],  # NW (fronteira IL/MI)
                        [-87.0094, 41.7606],  # NE
                        [-87.0094, 41.4092],  # SE
                        [-87.5353, 41.4092],  # SW
                        [-87.5353, 41.7606]
                    ]]
                }
            },
            {
                'type': 'Feature',
                'id': '18003',  # Allen County
                'properties': {'NAME': 'Allen', 'LSAD': 'County'},
                'geometry': {
                    'type': 'Polygon',
                    'coordinates': [[
                        [-85.2969, 41.1853],  # NW
                        [-84.8061, 41.1853],  # NE
                        [-84.8061, 40.7428],  # SE
                        [-85.2969, 40.7428],  # SW
                        [-85.2969, 41.1853]
                    ]]
                }
            }
        ]
    }
    
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(mock_counties, f, indent=2)
    
    logger.info(f"✅ MOCK GeoJSON criado: {output_path} (3 condados)")
    logger.warning("⚠️ Este é um MOCK para testes! Para produção, use download_indiana_counties_geojson()")
    
    return output_path


# ============================================================================
# STANDALONE TESTING
# ============================================================================

if __name__ == "__main__":
    """
    Teste standalone do módulo.
    
    Para executar:
        python src/owners/county_mapper.py
    """
    from loguru import logger
    import sys
    
    # Configurar logging para terminal
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    
    print("=" * 80)
    print("COUNTY MAPPER - Teste de Identificação de Condados")
    print("=" * 80)
    
    # Criar mock GeoJSON se não existir
    mock_path = create_mock_geojson()
    
    # Inicializar mapper com mock
    mapper = CountyMapper(geojson_path=mock_path)
    
    # Coordenadas de teste
    test_locations = [
        (39.7684, -86.1581, "Indianapolis - Marion County"),
        (41.5934, -87.3464, "Gary - Lake County"),
        (41.0793, -85.1394, "Fort Wayne - Allen County"),
        (40.4167, -86.8753, "Lafayette - Tippecanoe County (não no mock)")
    ]
    
    print("\n" + "=" * 80)
    print("TESTES DE IDENTIFICAÇÃO")
    print("=" * 80)
    
    for lat, lon, description in test_locations:
        print(f"\n📍 {description}")
        print(f"   Coordenadas: ({lat}, {lon})")
        
        county = mapper.identify_county(lat, lon)
        
        if county:
            print(f"   ✅ Condado: {county}")
            
            info = mapper.get_county_info(county)
            print(f"   Sistema: {info.get('assessor_system', 'N/A')}")
            print(f"   URL: {info.get('assessor_url', 'N/A')}")
        else:
            print(f"   ❌ Condado não identificado (pode não estar no mock)")
    
    # Estatísticas
    print("\n" + "=" * 80)
    print("ESTATÍSTICAS")
    print("=" * 80)
    stats = mapper.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n" + "=" * 80)
    print("✅ Teste concluído!")
    print("=" * 80)
    print("\n💡 Para produção, execute:")
    print("   from src.owners.county_mapper import download_indiana_counties_geojson")
    print("   download_indiana_counties_geojson()")
