"""
Modelos Pydantic para validação e serialização de dados.
"""
from datetime import datetime
from typing import Optional, List, Dict, Any
from decimal import Decimal
from pydantic import BaseModel, Field, validator, ConfigDict


class ParkRawData(BaseModel):
    """Modelo para dados brutos de parques (parks_raw table)."""
    
    model_config = ConfigDict(from_attributes=True)
    
    # Identificação
    external_id: Optional[str] = None
    source: str = Field(..., description="Fonte dos dados: google_places, osm, etc")
    
    # Informações básicas
    name: Optional[str] = None
    park_type: Optional[str] = None
    
    # Localização
    address: Optional[str] = None
    city: Optional[str] = None
    state: str = "IN"
    zip_code: Optional[str] = None
    county: Optional[str] = None
    
    # Coordenadas
    latitude: Optional[Decimal] = None
    longitude: Optional[Decimal] = None
    
    # Contato
    phone: Optional[str] = None
    website: Optional[str] = None
    email: Optional[str] = None
    
    # Informações operacionais
    business_status: Optional[str] = None
    rating: Optional[Decimal] = None
    total_reviews: Optional[int] = None
    
    # Metadados
    raw_data: Optional[Dict[str, Any]] = None
    tags: Optional[Dict[str, Any]] = None
    
    # Auditoria
    fetched_at: datetime = Field(default_factory=datetime.now)
    is_processed: bool = False
    
    @validator('latitude')
    def validate_latitude(cls, v):
        if v is not None and not (-90 <= float(v) <= 90):
            raise ValueError('Latitude deve estar entre -90 e 90')
        return v
    
    @validator('longitude')
    def validate_longitude(cls, v):
        if v is not None and not (-180 <= float(v) <= 180):
            raise ValueError('Longitude deve estar entre -180 e 180')
        return v


class OSMElement(BaseModel):
    """Modelo para elementos do OpenStreetMap."""
    
    model_config = ConfigDict(from_attributes=True)
    
    type: str  # node, way, relation
    id: int
    lat: Optional[float] = None
    lon: Optional[float] = None
    tags: Dict[str, str] = Field(default_factory=dict)
    
    # Para ways e relations que têm centróide calculado
    center: Optional[Dict[str, float]] = None
    
    def to_park_raw(self) -> ParkRawData:
        """Converte OSM element para ParkRawData."""
        
        # Determinar coordenadas
        lat = self.lat if self.lat else (self.center.get('lat') if self.center else None)
        lon = self.lon if self.lon else (self.center.get('lon') if self.center else None)
        
        # Determinar tipo de parque baseado nas tags
        park_type = None
        if self.tags.get('tourism') == 'camp_site':
            park_type = 'campground'
        elif self.tags.get('tourism') == 'caravan_site':
            park_type = 'rv_park'
        elif 'mobile' in self.tags.get('residential', '').lower():
            park_type = 'mobile_home_park'
        elif 'trailer' in self.tags.get('residential', '').lower():
            park_type = 'trailer_park'
        
        return ParkRawData(
            external_id=f"osm_{self.type}_{self.id}",
            source="osm",
            name=self.tags.get('name'),
            park_type=park_type,
            address=self.tags.get('addr:street'),
            city=self.tags.get('addr:city'),
            state=self.tags.get('addr:state', 'IN'),
            zip_code=self.tags.get('addr:postcode'),
            latitude=Decimal(str(lat)) if lat else None,
            longitude=Decimal(str(lon)) if lon else None,
            phone=self.tags.get('phone') or self.tags.get('contact:phone'),
            website=self.tags.get('website') or self.tags.get('contact:website'),
            raw_data={
                'osm_type': self.type,
                'osm_id': self.id,
                'osm_tags': self.tags
            },
            tags=self.tags,
            fetched_at=datetime.now()
        )


class GooglePlaceResult(BaseModel):
    """Modelo para resultados da Google Places API (Nearby Search)."""
    
    model_config = ConfigDict(from_attributes=True)
    
    place_id: str
    name: str
    types: List[str] = Field(default_factory=list)
    business_status: Optional[str] = None
    
    # Geometria
    geometry: Dict[str, Any]
    
    # Informações básicas
    vicinity: Optional[str] = None  # Endereço aproximado
    rating: Optional[float] = None
    user_ratings_total: Optional[int] = None
    
    # Outras
    plus_code: Optional[Dict[str, str]] = None
    
    @property
    def latitude(self) -> Optional[float]:
        """Extrai latitude da geometria."""
        return self.geometry.get('location', {}).get('lat')
    
    @property
    def longitude(self) -> Optional[float]:
        """Extrai longitude da geometria."""
        return self.geometry.get('location', {}).get('lng')


class GooglePlaceDetails(BaseModel):
    """Modelo para detalhes completos de um lugar (Place Details API)."""
    
    model_config = ConfigDict(from_attributes=True)
    
    place_id: str
    name: str
    types: List[str] = Field(default_factory=list)
    business_status: Optional[str] = None
    
    # Endereço
    formatted_address: Optional[str] = None
    address_components: List[Dict[str, Any]] = Field(default_factory=list)
    
    # Coordenadas
    geometry: Dict[str, Any]
    
    # Contato
    formatted_phone_number: Optional[str] = None
    international_phone_number: Optional[str] = None
    website: Optional[str] = None
    
    # Avaliações
    rating: Optional[float] = None
    user_ratings_total: Optional[int] = None
    
    # Operação
    opening_hours: Optional[Dict[str, Any]] = None
    
    # Outros
    reviews: List[Dict[str, Any]] = Field(default_factory=list)
    photos: List[Dict[str, Any]] = Field(default_factory=list)
    
    def extract_address_component(self, component_type: str) -> Optional[str]:
        """Extrai um componente específico do endereço."""
        for component in self.address_components:
            if component_type in component.get('types', []):
                return component.get('long_name') or component.get('short_name')
        return None
    
    def to_park_raw(self) -> ParkRawData:
        """Converte Google Place Details para ParkRawData."""
        
        # Determinar tipo de parque baseado em types
        park_type = None
        types_lower = [t.lower() for t in self.types]
        if 'rv_park' in types_lower:
            park_type = 'rv_park'
        elif 'campground' in types_lower:
            park_type = 'campground'
        elif any('mobile' in t for t in types_lower):
            park_type = 'mobile_home_park'
        
        return ParkRawData(
            external_id=self.place_id,
            source="google_places",
            name=self.name,
            park_type=park_type,
            address=self.formatted_address,
            city=self.extract_address_component('locality'),
            state=self.extract_address_component('administrative_area_level_1'),
            zip_code=self.extract_address_component('postal_code'),
            county=self.extract_address_component('administrative_area_level_2'),
            latitude=Decimal(str(self.geometry['location']['lat'])),
            longitude=Decimal(str(self.geometry['location']['lng'])),
            phone=self.formatted_phone_number,
            website=self.website,
            business_status=self.business_status,
            rating=Decimal(str(self.rating)) if self.rating else None,
            total_reviews=self.user_ratings_total,
            raw_data={
                'place_id': self.place_id,
                'types': self.types,
                'address_components': self.address_components,
                'geometry': self.geometry,
                'opening_hours': self.opening_hours
            },
            tags={
                'types': self.types,
                'has_photos': len(self.photos) > 0,
                'review_count': len(self.reviews)
            },
            fetched_at=datetime.now()
        )


class StateConfig(BaseModel):
    """Modelo para configuração do estado (carregado de indiana.yaml)."""
    
    model_config = ConfigDict(from_attributes=True)
    
    state: Dict[str, Any]
    geography: Dict[str, Any]
    data_sources: Dict[str, Any]
    scraping: Dict[str, Any]
    processing: Dict[str, Any]
    
    @property
    def bbox(self) -> Dict[str, float]:
        """Retorna bounding box do estado."""
        return self.geography.get('bbox', {})
    
    @property
    def center(self) -> Dict[str, float]:
        """Retorna centro geográfico do estado."""
        return self.geography.get('center', {})
    
    @property
    def srid(self) -> int:
        """Retorna SRID para coordenadas geográficas."""
        return self.geography.get('srid', 4326)
