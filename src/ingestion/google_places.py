"""
Módulo de ingestão de dados via Google Places API.
Implementa busca em grade (grid) para cobertura completa de Indiana.
Inclui caching para evitar chamadas duplicadas à API Place Details.
"""
import os
import time
import json
import hashlib
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
from datetime import datetime, timedelta
import requests
from loguru import logger
from dotenv import load_dotenv
import math

from ..models import GooglePlaceResult, GooglePlaceDetails, ParkRawData, StateConfig

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
        """
        Recupera detalhes do cache se existirem.
        
        Returns:
            Dados do lugar ou None se não estiver em cache
        """
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
    
    def clear_expired(self, max_age_days: int = 7):
        """Remove cache expirado."""
        logger.info(f"Removendo cache com mais de {max_age_days} dias...")
        
        removed = 0
        for cache_file in self.details_dir.glob("*.json"):
            with open(cache_file, 'r') as f:
                data = json.load(f)
                cached_at = datetime.fromisoformat(data.get('cached_at'))
                
                if datetime.now() - cached_at > timedelta(days=max_age_days):
                    cache_file.unlink()
                    removed += 1
        
        logger.info(f"Removidos {removed} arquivos de cache expirados")


class GridGenerator:
    """
    Gerador de grade de coordenadas para cobrir Indiana completamente.
    """
    
    def __init__(self, state_config: StateConfig, grid_spacing_km: float = 40):
        """
        Args:
            state_config: Configuração do estado
            grid_spacing_km: Espaçamento da grade em km (padrão 40km)
        """
        self.config = state_config
        self.bbox = state_config.bbox
        self.grid_spacing_km = grid_spacing_km
    
    def _km_to_degrees_lat(self, km: float) -> float:
        """Converte km para graus de latitude (aproximado)."""
        return km / 111.0  # 1 grau lat ≈ 111 km
    
    def _km_to_degrees_lon(self, km: float, latitude: float) -> float:
        """Converte km para graus de longitude na latitude dada."""
        # 1 grau lon = 111 km * cos(lat)
        return km / (111.0 * math.cos(math.radians(latitude)))
    
    def generate_grid_points(self) -> List[Tuple[float, float]]:
        """
        Gera pontos da grade cobrindo o bounding box de Indiana.
        
        Returns:
            Lista de tuplas (latitude, longitude)
        """
        min_lat = self.bbox['min_lat']
        max_lat = self.bbox['max_lat']
        min_lon = self.bbox['min_lon']
        max_lon = self.bbox['max_lon']
        
        # Centro para cálculo de longitude
        center_lat = (min_lat + max_lat) / 2
        
        # Espaçamento em graus
        lat_spacing = self._km_to_degrees_lat(self.grid_spacing_km)
        lon_spacing = self._km_to_degrees_lon(self.grid_spacing_km, center_lat)
        
        # Gerar pontos
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


class GooglePlacesAPI:
    """Cliente para Google Places API com rate limiting e quota management."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.getenv("GOOGLE_PLACES_API_KEY")
        
        if not self.api_key:
            raise ValueError(
                "GOOGLE_PLACES_API_KEY não definida. "
                "Configure no arquivo .env ou passe como parâmetro."
            )
        
        self.base_url = "https://maps.googleapis.com/maps/api/place"
        
        # Rate limiting (Google permite ~100 req/s, mas vamos ser conservadores)
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
            logger.info(f"Nova data: resetando contador de quota ({self.requests_today} requests ontem)")
            self.requests_today = 0
            self.quota_reset_date = today
        
        if self.requests_today >= self.daily_quota:
            raise Exception(
                f"Quota diária atingida: {self.requests_today}/{self.daily_quota} requests. "
                f"Execute novamente amanhã ou aumente MAX_API_CALLS_PER_DAY no .env"
            )
    
    def _respect_rate_limit(self):
        """Aplica rate limiting."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.min_delay:
            time.sleep(self.min_delay - elapsed)
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Faz requisição à API com rate limiting e error handling.
        
        Args:
            endpoint: Endpoint da API (nearbysearch, details, etc)
            params: Parâmetros da query
            
        Returns:
            Resposta JSON
        """
        self._check_quota()
        self._respect_rate_limit()
        
        params['key'] = self.api_key
        url = f"{self.base_url}/{endpoint}/json"
        
        try:
            response = requests.get(url, params=params, timeout=30)
            self.last_request_time = time.time()
            self.requests_today += 1
            
            response.raise_for_status()
            data = response.json()
            
            status = data.get('status')
            
            if status == 'OK' or status == 'ZERO_RESULTS':
                return data
            elif status == 'OVER_QUERY_LIMIT':
                logger.error("OVER_QUERY_LIMIT: quota da API excedida")
                raise Exception("Quota da Google Places API excedida")
            elif status == 'REQUEST_DENIED':
                logger.error(f"REQUEST_DENIED: {data.get('error_message', 'Sem mensagem')}")
                raise Exception("Requisição negada pela API")
            else:
                logger.warning(f"Status não esperado: {status}")
                return data
                
        except requests.RequestException as e:
            logger.error(f"Erro na requisição Google Places: {e}")
            raise
    
    def nearby_search(
        self,
        location: Tuple[float, float],
        radius: int = 50000,
        keyword: str = "rv park"
    ) -> List[GooglePlaceResult]:
        """
        Busca lugares próximos a uma localização.
        
        Args:
            location: Tupla (latitude, longitude)
            radius: Raio em metros (max 50000)
            keyword: Palavra-chave de busca
            
        Returns:
            Lista de GooglePlaceResult
        """
        lat, lon = location
        
        params = {
            'location': f"{lat},{lon}",
            'radius': min(radius, 50000),  # Max 50km
            'keyword': keyword
        }
        
        logger.debug(f"Nearby search: {keyword} @ ({lat:.4f}, {lon:.4f}), radius={radius}m")
        
        data = self._make_request('nearbysearch', params)
        
        results = []
        for result_data in data.get('results', []):
            try:
                result = GooglePlaceResult(**result_data)
                results.append(result)
            except Exception as e:
                logger.warning(f"Erro ao parsear resultado: {e}")
        
        logger.debug(f"Encontrados {len(results)} resultados")
        
        return results
    
    def place_details(self, place_id: str) -> Optional[GooglePlaceDetails]:
        """
        Busca detalhes completos de um lugar.
        USA CACHE para evitar chamadas duplicadas.
        
        Args:
            place_id: ID do lugar
            
        Returns:
            GooglePlaceDetails ou None se erro
        """
        # Verificar cache primeiro
        cached = self.cache.get_details(place_id)
        if cached:
            logger.debug(f"Usando cache para place_id {place_id}")
            return GooglePlaceDetails(**cached)
        
        # Buscar da API
        params = {
            'place_id': place_id,
            'fields': ','.join([
                'place_id',
                'name',
                'formatted_address',
                'address_components',
                'geometry',
                'formatted_phone_number',
                'international_phone_number',
                'website',
                'business_status',
                'rating',
                'user_ratings_total',
                'types',
                'opening_hours',
                'reviews',
                'photos'
            ])
        }
        
        logger.debug(f"Buscando detalhes para place_id {place_id}")
        
        try:
            data = self._make_request('details', params)
            
            result = data.get('result')
            if not result:
                logger.warning(f"Sem resultado para place_id {place_id}")
                return None
            
            # Salvar no cache
            self.cache.save_details(place_id, result)
            
            return GooglePlaceDetails(**result)
            
        except Exception as e:
            logger.error(f"Erro ao buscar detalhes de {place_id}: {e}")
            return None


def fetch_google_parks(
    state_config: StateConfig,
    keywords: Optional[List[str]] = None,
    grid_spacing_km: float = 40
) -> List[ParkRawData]:
    """
    Busca parques usando Google Places API com cobertura em grade.
    
    Esta função:
    1. Gera uma grade de pontos cobrindo Indiana
    2. Para cada ponto, executa Nearby Search com múltiplos keywords
    3. Para cada resultado, busca Place Details (com cache)
    4. Converte para ParkRawData
    
    Args:
        state_config: Configuração do estado
        keywords: Lista de palavras-chave (padrão: rv park, mobile home park, etc)
        grid_spacing_km: Espaçamento da grade em km
        
    Returns:
        Lista de ParkRawData
        
    Example:
        >>> import yaml
        >>> from models import StateConfig
        >>> 
        >>> with open('config/indiana.yaml') as f:
        >>>     config = yaml.safe_load(f)
        >>> state_config = StateConfig(**config)
        >>> 
        >>> parks = fetch_google_parks(state_config)
        >>> print(f"Encontrados {len(parks)} parques")
    """
    
    # Keywords padrão
    if keywords is None:
        keywords = [
            "rv park",
            "mobile home park",
            "trailer park",
            "manufactured home community",
            "campground",
            "rv resort"
        ]
    
    logger.info(f"Iniciando busca Google Places para {state_config.state['name']}")
    logger.info(f"Keywords: {', '.join(keywords)}")
    
    # Inicializar API
    api = GooglePlacesAPI()
    
    # Gerar grade
    grid = GridGenerator(state_config, grid_spacing_km)
    grid_points = grid.generate_grid_points()
    
    # Configuração de raio (50km = máximo da API)
    search_radius = state_config.data_sources.get('google_places', {}).get('radius_meters', 50000)
    
    # Coletar place_ids únicos
    all_place_ids: Set[str] = set()
    
    logger.info(f"Executando Nearby Search em {len(grid_points)} pontos da grade...")
    
    # Para cada ponto da grade
    for i, location in enumerate(grid_points, 1):
        logger.info(f"Processando ponto {i}/{len(grid_points)}: {location}")
        
        # Para cada keyword
        for keyword in keywords:
            try:
                results = api.nearby_search(
                    location=location,
                    radius=search_radius,
                    keyword=keyword
                )
                
                for result in results:
                    all_place_ids.add(result.place_id)
                
                logger.info(
                    f"  {keyword}: {len(results)} resultados "
                    f"(total único: {len(all_place_ids)})"
                )
                
            except Exception as e:
                logger.error(f"Erro no Nearby Search: {e}")
                # Continuar com próximo keyword
                continue
    
    logger.info(f"\nTotal de place_ids únicos encontrados: {len(all_place_ids)}")
    
    # Buscar detalhes para cada place_id (com cache!)
    parks_raw = []
    skipped = 0
    
    logger.info("Buscando detalhes de cada lugar (usando cache quando possível)...")
    
    for i, place_id in enumerate(sorted(all_place_ids), 1):
        
        if i % 50 == 0:
            logger.info(
                f"Progresso: {i}/{len(all_place_ids)} "
                f"({100*i/len(all_place_ids):.1f}%) - "
                f"Quota: {api.requests_today}/{api.daily_quota}"
            )
        
        # Verificar se já foi processado
        if api.cache.is_processed(place_id):
            # Carregar do cache
            cached_details = api.cache.get_details(place_id)
            if cached_details:
                try:
                    details = GooglePlaceDetails(**cached_details)
                    park_raw = details.to_park_raw()
                    parks_raw.append(park_raw)
                except Exception as e:
                    logger.warning(f"Erro ao processar cache de {place_id}: {e}")
                    skipped += 1
            continue
        
        # Buscar da API
        try:
            details = api.place_details(place_id)
            
            if details:
                park_raw = details.to_park_raw()
                parks_raw.append(park_raw)
            else:
                skipped += 1
                
        except Exception as e:
            logger.error(f"Erro ao processar place_id {place_id}: {e}")
            skipped += 1
            continue
    
    logger.info(
        f"\nGoogle Places fetch completo: "
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
    
    # Configurar logging
    logger.add(
        "logs/google_places_{time}.log",
        rotation="1 day",
        retention="30 days",
        level="INFO"
    )
    
    # Carregar configuração
    logger.info("Carregando configuração...")
    state_config = load_state_config()
    
    # Buscar parques
    parks = fetch_google_parks(state_config, grid_spacing_km=50)
    
    # Resumo
    logger.info(f"\n{'='*60}")
    logger.info("RESUMO DA INGESTÃO GOOGLE PLACES")
    logger.info(f"{'='*60}")
    logger.info(f"Total de parques: {len(parks)}")
    
    if parks:
        # Estatísticas
        types_count = {}
        cities_count = {}
        
        for park in parks:
            # Tipo
            park_type = park.park_type or 'unknown'
            types_count[park_type] = types_count.get(park_type, 0) + 1
            
            # Cidade
            city = park.city or 'Unknown'
            cities_count[city] = cities_count.get(city, 0) + 1
        
        logger.info("\nPor tipo:")
        for park_type, count in sorted(types_count.items()):
            logger.info(f"  {park_type}: {count}")
        
        logger.info(f"\nCidades com mais parques:")
        for city, count in sorted(cities_count.items(), key=lambda x: x[1], reverse=True)[:10]:
            logger.info(f"  {city}: {count}")
        
        # Exemplos
        logger.info("\nPrimeiros 5 parques:")
        for i, park in enumerate(parks[:5], 1):
            logger.info(
                f"  {i}. {park.name} - {park.city}, {park.state} "
                f"({park.park_type})"
            )
    
    logger.info(f"{'='*60}\n")
    
    return parks


if __name__ == "__main__":
    main()
