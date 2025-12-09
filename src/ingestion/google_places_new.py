"""
Módulo de ingestão de dados via Google Places API (New).
Implementa busca usando a nova API v1 do Google Places.
https://developers.google.com/maps/documentation/places/web-service/op-overview
"""
import os
import time
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime, timedelta
from decimal import Decimal
import requests
from loguru import logger
from dotenv import load_dotenv
import math

from ..models import ParkRawData, StateConfig

load_dotenv()


class PlacesAPICache:
    """
    Cache simples baseado em arquivos JSON para Place IDs.
    Evita chamadas duplicadas à API Place Details.
    """
    
    def __init__(self, cache_dir: str = "data/cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        
        self.places_file = self.cache_dir / "google_places_cache.json"
        self.details_dir = self.cache_dir / "place_details"
        self.details_dir.mkdir(exist_ok=True)
        
        # Carregar cache de place_ids processados
        self.processed_place_ids: Set[str] = self._load_processed_ids()
        
        logger.info(f"Cache inicializado: {len(self.processed_place_ids)} place_ids em cache")
    
    def _load_processed_ids(self) -> Set[str]:
        """Carrega set de place_ids já processados."""
        if self.places_file.exists():
            with open(self.places_file, 'r') as f:
                data = json.load(f)
                return set(data.get('processed_ids', []))
        return set()
    
    def _save_processed_ids(self):
        """Salva set de place_ids processados."""
        with open(self.places_file, 'w') as f:
            json.dump({
                'processed_ids': list(self.processed_place_ids),
                'last_updated': datetime.now().isoformat()
            }, f, indent=2)
    
    def is_processed(self, place_id: str) -> bool:
        """Verifica se place_id já foi processado."""
        return place_id in self.processed_place_ids
    
    def get_details(self, place_id: str) -> Optional[Dict[str, Any]]:
        """Recupera detalhes do cache se existirem."""
        cache_file = self.details_dir / f"{place_id}.json"
        
        if cache_file.exists():
            with open(cache_file, 'r') as f:
                data = json.load(f)
                
                # Verificar se cache não expirou (7 dias)
                cached_at = datetime.fromisoformat(data.get('cached_at'))
                if datetime.now() - cached_at < timedelta(days=7):
                    logger.debug(f"Cache hit para {place_id}")
                    return data.get('details')
        
        return None
    
    def save_details(self, place_id: str, details: Dict[str, Any]):
        """Salva detalhes no cache."""
        cache_file = self.details_dir / f"{place_id}.json"
        
        with open(cache_file, 'w') as f:
            json.dump({
                'place_id': place_id,
                'details': details,
                'cached_at': datetime.now().isoformat()
            }, f, indent=2)
        
        self.processed_place_ids.add(place_id)
        self._save_processed_ids()
        
        logger.debug(f"Cache salvo para {place_id}")


class GridGenerator:
    """Gerador de grade de coordenadas para cobrir Indiana completamente."""
    
    def __init__(self, state_config: StateConfig, grid_spacing_km: float = 40):
        self.config = state_config
        self.bbox = state_config.bbox
        self.grid_spacing_km = grid_spacing_km
    
    def _km_to_degrees_lat(self, km: float) -> float:
        """Converte km para graus de latitude (aproximado)."""
        return km / 111.0
    
    def _km_to_degrees_lon(self, km: float, latitude: float) -> float:
        """Converte km para graus de longitude na latitude dada."""
        return km / (111.0 * math.cos(math.radians(latitude)))
    
    def generate_grid_points(self) -> List[Tuple[float, float]]:
        """Gera pontos da grade cobrindo o bounding box de Indiana."""
        min_lat = self.bbox['min_lat']
        max_lat = self.bbox['max_lat']
        min_lon = self.bbox['min_lon']
        max_lon = self.bbox['max_lon']
        
        center_lat = (min_lat + max_lat) / 2
        
        lat_spacing = self._km_to_degrees_lat(self.grid_spacing_km)
        lon_spacing = self._km_to_degrees_lon(self.grid_spacing_km, center_lat)
        
        grid_points = []
        
        current_lat = min_lat
        while current_lat <= max_lat:
            current_lon = min_lon
            while current_lon <= max_lon:
                grid_points.append((current_lat, current_lon))
                current_lon += lon_spacing
            current_lat += lat_spacing
        
        logger.info(
            f"Grade gerada: {len(grid_points)} pontos "
            f"(espaçamento ~{self.grid_spacing_km}km)"
        )
        
        return grid_points


class GooglePlacesNewAPI:
    """
    Cliente para Google Places API (New) com rate limiting.
    
    Usa os novos endpoints v1:
    - POST /v1/places:searchNearby
    - GET /v1/places/{place_id}
    """
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GOOGLE_PLACES_API_KEY")
        
        if not self.api_key:
            raise ValueError(
                "GOOGLE_PLACES_API_KEY não definida. "
                "Configure no arquivo .env ou passe como parâmetro."
            )
        
        self.base_url = "https://places.googleapis.com/v1"
        
        # Rate limiting
        self.requests_per_second = 10
        self.min_delay = 1.0 / self.requests_per_second
        self.last_request_time = 0
        
        # Quota tracking
        self.daily_quota = int(os.getenv("MAX_API_CALLS_PER_DAY", "10000"))
        self.requests_today = 0
        self.quota_reset_date = datetime.now().date()
        
        # Cache
        self.cache = PlacesAPICache()
    
    def _check_quota(self):
        """Verifica e reseta quota diária."""
        today = datetime.now().date()
        
        if today > self.quota_reset_date:
            logger.info(f"Nova data: resetando contador de quota")
            self.requests_today = 0
            self.quota_reset_date = today
        
        if self.requests_today >= self.daily_quota:
            raise Exception(f"Quota diária atingida: {self.requests_today}/{self.daily_quota}")
    
    def _respect_rate_limit(self):
        """Aplica rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
    
    def _get_headers(self, field_mask: Optional[List[str]] = None) -> Dict[str, str]:
        """Retorna headers para a nova API."""
        headers = {
            'Content-Type': 'application/json',
            'X-Goog-Api-Key': self.api_key,
        }
        
        if field_mask:
            headers['X-Goog-FieldMask'] = ','.join(field_mask)
        
        return headers
    
    def nearby_search(
        self,
        location: Tuple[float, float],
        radius: float = 50000,
        text_query: str = "rv park"
    ) -> List[Dict[str, Any]]:
        """
        Busca lugares próximos usando a nova API.
        
        Args:
            location: Tupla (latitude, longitude)
            radius: Raio em metros (max 50000)
            text_query: Texto de busca
            
        Returns:
            Lista de dicionários com dados dos lugares
        """
        self._check_quota()
        self._respect_rate_limit()
        
        lat, lon = location
        
        # Nova API usa POST com JSON body
        url = f"{self.base_url}/places:searchNearby"
        
        # Field mask para os campos que queremos
        field_mask = [
            'places.id',
            'places.displayName',
            'places.formattedAddress',
            'places.addressComponents',
            'places.location',
            'places.types',
            'places.businessStatus',
            'places.rating',
            'places.userRatingCount',
            'places.nationalPhoneNumber',
            'places.internationalPhoneNumber',
            'places.websiteUri',
        ]
        
        headers = self._get_headers(field_mask)
        
        # Body da requisição
        body = {
            'includedTypes': ['rv_park', 'campground', 'mobile_home_park'],
            'maxResultCount': 20,
            'locationRestriction': {
                'circle': {
                    'center': {
                        'latitude': lat,
                        'longitude': lon
                    },
                    'radius': min(radius, 50000.0)
                }
            }
        }
        
        logger.debug(f"Nearby search @ ({lat:.4f}, {lon:.4f}), radius={radius}m")
        
        try:
            response = requests.post(url, headers=headers, json=body, timeout=30)
            self.last_request_time = time.time()
            self.requests_today += 1
            
            if response.status_code == 200:
                data = response.json()
                places = data.get('places', [])
                logger.debug(f"Encontrados {len(places)} resultados")
                return places
            elif response.status_code == 400:
                # Tentar text search como fallback
                return self._text_search_fallback(location, radius, text_query)
            else:
                logger.error(f"Erro {response.status_code}: {response.text}")
                return []
                
        except requests.RequestException as e:
            logger.error(f"Erro na requisição: {e}")
            return []
    
    def _text_search_fallback(
        self,
        location: Tuple[float, float],
        radius: float,
        text_query: str
    ) -> List[Dict[str, Any]]:
        """
        Fallback usando Text Search quando Nearby Search falha.
        """
        self._check_quota()
        self._respect_rate_limit()
        
        lat, lon = location
        
        url = f"{self.base_url}/places:searchText"
        
        field_mask = [
            'places.id',
            'places.displayName',
            'places.formattedAddress',
            'places.addressComponents',
            'places.location',
            'places.types',
            'places.businessStatus',
            'places.rating',
            'places.userRatingCount',
            'places.nationalPhoneNumber',
            'places.internationalPhoneNumber',
            'places.websiteUri',
        ]
        
        headers = self._get_headers(field_mask)
        
        body = {
            'textQuery': text_query,
            'maxResultCount': 20,
            'locationBias': {
                'circle': {
                    'center': {
                        'latitude': lat,
                        'longitude': lon
                    },
                    'radius': min(radius, 50000.0)
                }
            }
        }
        
        logger.debug(f"Text search: '{text_query}' @ ({lat:.4f}, {lon:.4f})")
        
        try:
            response = requests.post(url, headers=headers, json=body, timeout=30)
            self.last_request_time = time.time()
            self.requests_today += 1
            
            if response.status_code == 200:
                data = response.json()
                places = data.get('places', [])
                logger.debug(f"Text search encontrou {len(places)} resultados")
                return places
            else:
                logger.error(f"Text search erro {response.status_code}: {response.text}")
                return []
                
        except requests.RequestException as e:
            logger.error(f"Erro no text search: {e}")
            return []
    
    def get_place_details(self, place_id: str) -> Optional[Dict[str, Any]]:
        """
        Busca detalhes de um lugar específico.
        
        Args:
            place_id: ID do lugar (formato: places/XXXX)
            
        Returns:
            Dicionário com detalhes ou None
        """
        # Verificar cache primeiro
        cached = self.cache.get_details(place_id)
        if cached:
            logger.debug(f"Usando cache para {place_id}")
            return cached
        
        self._check_quota()
        self._respect_rate_limit()
        
        # Normalizar place_id (a nova API usa 'places/XXXXX')
        if not place_id.startswith('places/'):
            place_id = f"places/{place_id}"
        
        url = f"{self.base_url}/{place_id}"
        
        field_mask = [
            'id',
            'displayName',
            'formattedAddress',
            'addressComponents',
            'location',
            'types',
            'businessStatus',
            'rating',
            'userRatingCount',
            'nationalPhoneNumber',
            'internationalPhoneNumber',
            'websiteUri',
            'regularOpeningHours',
        ]
        
        headers = self._get_headers(field_mask)
        
        logger.debug(f"Buscando detalhes: {place_id}")
        
        try:
            response = requests.get(url, headers=headers, timeout=30)
            self.last_request_time = time.time()
            self.requests_today += 1
            
            if response.status_code == 200:
                data = response.json()
                self.cache.save_details(place_id, data)
                return data
            else:
                logger.warning(f"Erro ao buscar detalhes: {response.status_code}")
                return None
                
        except requests.RequestException as e:
            logger.error(f"Erro ao buscar detalhes de {place_id}: {e}")
            return None


def parse_place_to_park_raw(place: Dict[str, Any]) -> ParkRawData:
    """
    Converte um lugar da nova API para ParkRawData.
    
    Args:
        place: Dicionário com dados do lugar da nova API
        
    Returns:
        ParkRawData pronto para inserção
    """
    # Extrair ID (remover prefixo 'places/')
    place_id = place.get('id', '')
    if place_id.startswith('places/'):
        place_id = place_id[7:]
    
    # Nome
    display_name = place.get('displayName', {})
    name = display_name.get('text', '') if isinstance(display_name, dict) else str(display_name)
    
    # Localização
    location = place.get('location', {})
    lat = location.get('latitude')
    lon = location.get('longitude')
    
    # Endereço
    formatted_address = place.get('formattedAddress', '')
    
    # Componentes do endereço
    address_components = place.get('addressComponents', [])
    city = None
    state = 'IN'
    zip_code = None
    county = None
    
    for comp in address_components:
        types = comp.get('types', [])
        long_name = comp.get('longText', '')
        
        if 'locality' in types:
            city = long_name
        elif 'administrative_area_level_1' in types:
            state = comp.get('shortText', 'IN')
        elif 'postal_code' in types:
            zip_code = long_name
        elif 'administrative_area_level_2' in types:
            county = long_name
    
    # Tipo de parque
    types = place.get('types', [])
    park_type = None
    if 'rv_park' in types:
        park_type = 'rv_park'
    elif 'campground' in types:
        park_type = 'campground'
    elif 'mobile_home_park' in types:
        park_type = 'mobile_home_park'
    
    # Contato
    phone = place.get('nationalPhoneNumber') or place.get('internationalPhoneNumber')
    website = place.get('websiteUri')
    
    # Business status
    business_status = place.get('businessStatus')
    
    # Rating
    rating = place.get('rating')
    user_rating_count = place.get('userRatingCount')
    
    return ParkRawData(
        external_id=place_id,
        source="google_places",
        name=name,
        park_type=park_type,
        address=formatted_address,
        city=city,
        state=state,
        zip_code=zip_code,
        county=county,
        latitude=Decimal(str(lat)) if lat else None,
        longitude=Decimal(str(lon)) if lon else None,
        phone=phone,
        website=website,
        business_status=business_status,
        rating=Decimal(str(rating)) if rating else None,
        total_reviews=user_rating_count,
        raw_data={
            'place_id': place_id,
            'types': types,
            'address_components': address_components,
        },
        tags={
            'types': types,
            'source_api': 'places_api_new'
        },
        fetched_at=datetime.now()
    )


def fetch_google_parks(
    state_config: StateConfig,
    keywords: Optional[List[str]] = None,
    grid_spacing_km: float = 40
) -> List[ParkRawData]:
    """
    Busca parques usando Google Places API (New) com cobertura em grade.
    
    Args:
        state_config: Configuração do estado
        keywords: Lista de palavras-chave para text search
        grid_spacing_km: Espaçamento da grade em km
        
    Returns:
        Lista de ParkRawData
    """
    # Keywords para text search
    if keywords is None:
        keywords = [
            "rv park",
            "mobile home park",
            "trailer park",
            "manufactured home community",
            "campground",
            "rv resort"
        ]
    
    logger.info(f"Iniciando busca Google Places (New) para {state_config.state['name']}")
    logger.info(f"Keywords: {', '.join(keywords)}")
    
    # Inicializar API
    api = GooglePlacesNewAPI()
    
    # Gerar grade
    grid = GridGenerator(state_config, grid_spacing_km)
    grid_points = grid.generate_grid_points()
    
    # Configuração de raio
    search_radius = state_config.data_sources.get('google_places', {}).get('radius_meters', 50000)
    
    # Coletar places únicos por ID
    all_places: Dict[str, Dict[str, Any]] = {}
    
    logger.info(f"Executando busca em {len(grid_points)} pontos da grade...")
    
    # Para cada ponto da grade
    for i, location in enumerate(grid_points, 1):
        logger.info(f"Processando ponto {i}/{len(grid_points)}: ({location[0]:.4f}, {location[1]:.4f})")
        
        # Primeiro tentar Nearby Search
        try:
            results = api.nearby_search(
                location=location,
                radius=search_radius,
                text_query=keywords[0]  # Usar primeiro keyword como fallback
            )
            
            for place in results:
                place_id = place.get('id', '')
                if place_id and place_id not in all_places:
                    all_places[place_id] = place
            
            logger.info(f"  Nearby: {len(results)} resultados (total único: {len(all_places)})")
            
        except Exception as e:
            logger.error(f"Erro no Nearby Search: {e}")
        
        # Text search para cada keyword
        for keyword in keywords:
            try:
                results = api._text_search_fallback(
                    location=location,
                    radius=search_radius,
                    text_query=keyword
                )
                
                for place in results:
                    place_id = place.get('id', '')
                    if place_id and place_id not in all_places:
                        all_places[place_id] = place
                
                if results:
                    logger.debug(f"  '{keyword}': {len(results)} resultados")
                    
            except Exception as e:
                logger.error(f"Erro no Text Search '{keyword}': {e}")
                continue
    
    logger.info(f"\nTotal de lugares únicos encontrados: {len(all_places)}")
    
    # Converter para ParkRawData
    parks_raw = []
    skipped = 0
    
    logger.info("Convertendo dados...")
    
    for place_id, place in all_places.items():
        try:
            park_raw = parse_place_to_park_raw(place)
            
            # Validar que tem coordenadas
            if park_raw.latitude is None or park_raw.longitude is None:
                logger.warning(f"Lugar {place_id} sem coordenadas - pulando")
                skipped += 1
                continue
            
            parks_raw.append(park_raw)
            
        except Exception as e:
            logger.error(f"Erro ao converter {place_id}: {e}")
            skipped += 1
    
    logger.info(
        f"\nGoogle Places (New) fetch completo: "
        f"{len(parks_raw)} parques válidos, {skipped} pulados"
    )
    logger.info(f"Total de requests à API: {api.requests_today}")
    
    return parks_raw


def load_state_config(config_path: str = "config/indiana.yaml") -> StateConfig:
    """Carrega configuração do estado."""
    import yaml
    
    config_file = Path(config_path)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Config não encontrado: {config_path}")
    
    with open(config_file, 'r', encoding='utf-8') as f:
        config_dict = yaml.safe_load(f)
    
    return StateConfig(**config_dict)


def main():
    """Exemplo de uso."""
    
    logger.add(
        "logs/google_places_new_{time}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO"
    )
    
    logger.info("Carregando configuração...")
    state_config = load_state_config()
    
    parks = fetch_google_parks(state_config, grid_spacing_km=50)
    
    logger.info(f"\n{'='*60}")
    logger.info("RESUMO DA INGESTÃO GOOGLE PLACES (NEW)")
    logger.info(f"{'='*60}")
    logger.info(f"Total de parques: {len(parks)}")
    
    if parks:
        types_count = {}
        for park in parks:
            park_type = park.park_type or 'unknown'
            types_count[park_type] = types_count.get(park_type, 0) + 1
        
        logger.info("\nPor tipo:")
        for park_type, count in sorted(types_count.items()):
            logger.info(f"  {park_type}: {count}")
        
        logger.info("\nPrimeiros 5 parques:")
        for i, park in enumerate(parks[:5], 1):
            logger.info(f"  {i}. {park.name} - {park.city}, {park.state}")
    
    return parks


if __name__ == "__main__":
    main()
