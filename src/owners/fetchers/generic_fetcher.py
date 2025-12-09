"""
Generic Web Search Fetcher
===========================

Fetcher genérico que usa Google Custom Search API como fallback para condados
sem implementação específica ou quando sistemas especializados falham.

⚠️ LIMITAÇÕES:
--------------
- Google Custom Search API: 100 queries grátis/dia, depois $5/1000 queries
- Resultados podem ser imprecisos (requer validação manual)
- Não substitui acesso direto aos County Assessor systems
- Usar apenas como ÚLTIMO RECURSO

QUANDO USAR:
------------
1. Condados sem sistema online (12 de 92)
2. Fallback quando Beacon/Vanguard falham
3. Validação cruzada de dados obtidos

ALTERNATIVAS MELHORES:
----------------------
1. API comercial: DataTree by First American ($$$$)
2. API comercial: CoreLogic PropertyInfo API ($$$$)
3. FOIA Request em lote (gratuito mas lento - 30+ dias)

Author: BellaTerra Intelligence Team
Date: December 2025
"""

import os
import time
from typing import Optional, Dict, List
import re

import requests
from loguru import logger

from src.owners.base_fetcher import (
    CountyAssessorFetcher,
    FetchResult,
    OwnerRecord,
    PropertyClassCode,
    RateLimiter
)


class GenericWebSearchFetcher(CountyAssessorFetcher):
    """
    Fetcher genérico usando Google Custom Search API.
    
    Busca por padrões como:
    - "[Address] county assessor indiana owner"
    - "[Address] parcel owner indiana"
    - "[County] assessor [Address]"
    
    Depois extrai informações dos resultados usando regex.
    """
    
    def __init__(
        self,
        county_name: str,
        google_api_key: Optional[str] = None,
        search_engine_id: Optional[str] = None
    ):
        """
        Args:
            county_name: Nome do condado
            google_api_key: API key do Google Custom Search (ou usa .env)
            search_engine_id: ID do Custom Search Engine (ou usa .env)
        """
        super().__init__(county_name=county_name, system_type="Generic Web Search")
        
        # Carregar credenciais
        self.api_key = google_api_key or os.getenv('GOOGLE_CUSTOM_SEARCH_API_KEY')
        self.search_engine_id = search_engine_id or os.getenv('GOOGLE_CUSTOM_SEARCH_ENGINE_ID')
        
        if not self.api_key or not self.search_engine_id:
            logger.warning(
                "⚠️ Google Custom Search API não configurado!\n"
                "Defina em .env:\n"
                "  GOOGLE_CUSTOM_SEARCH_API_KEY=sua_chave\n"
                "  GOOGLE_CUSTOM_SEARCH_ENGINE_ID=seu_id\n"
                "Para obter: https://developers.google.com/custom-search/v1/overview"
            )
        
        # Rate limiter (100 queries/dia = ~4/hora para durar 24h)
        self.rate_limiter = RateLimiter(requests_per_minute=4)
        
        # Cache de buscas (evitar queries duplicadas)
        self._search_cache: Dict[str, List[Dict]] = {}
    
    def _get_base_url(self) -> str:
        """URL base da API do Google Custom Search."""
        return "https://www.googleapis.com/customsearch/v1"
    
    def lookup_owner(
        self,
        address: str,
        lat: float,
        lon: float,
        parcel_id: Optional[str] = None
    ) -> FetchResult:
        """
        Busca proprietário usando Google Custom Search.
        
        Estratégia:
        1. Construir query otimizada
        2. Buscar no Google
        3. Parsear resultados para extrair nome/endereço
        4. Validar e retornar
        """
        if not self.api_key or not self.search_engine_id:
            return FetchResult(
                success=False,
                error_message="Google Custom Search API não configurado"
            )
        
        logger.info(f"🔍 Buscando proprietário para: {address} ({self.county_name})")
        
        # Respeitar rate limit
        self.rate_limiter.wait()
        
        # Construir query
        query = self._build_search_query(address, parcel_id)
        
        # Buscar (com cache)
        search_results = self._search(query)
        
        if not search_results:
            self._increment_stats(success=False)
            return FetchResult(
                success=False,
                error_message="Nenhum resultado encontrado no Google"
            )
        
        # Parsear resultados
        owner_record = self._parse_search_results(
            search_results,
            address,
            lat,
            lon,
            parcel_id
        )
        
        if owner_record:
            self._increment_stats(success=True)
            return FetchResult(success=True, records=[owner_record])
        else:
            self._increment_stats(success=False)
            return FetchResult(
                success=False,
                error_message="Não foi possível extrair dados de proprietário dos resultados"
            )
    
    def search_by_parcel_id(self, parcel_id: str) -> FetchResult:
        """
        Busca por Parcel ID.
        
        Como não temos acesso direto ao sistema do condado, usamos busca genérica.
        """
        normalized_id = self.normalize_parcel_id(parcel_id)
        
        query = f'"{normalized_id}" "{self.county_name}" indiana owner assessor'
        
        logger.info(f"🔍 Buscando por Parcel ID: {normalized_id}")
        
        self.rate_limiter.wait()
        search_results = self._search(query)
        
        if not search_results:
            self._increment_stats(success=False)
            return FetchResult(
                success=False,
                error_message=f"Parcel ID {parcel_id} não encontrado"
            )
        
        # Tentar parsear
        owner_record = self._parse_search_results(
            search_results,
            address="",  # Não sabemos o endereço ainda
            lat=0.0,
            lon=0.0,
            parcel_id=parcel_id
        )
        
        if owner_record:
            self._increment_stats(success=True)
            return FetchResult(success=True, records=[owner_record])
        else:
            self._increment_stats(success=False)
            return FetchResult(
                success=False,
                error_message="Dados de proprietário não encontrados nos resultados"
            )
    
    # ========================================================================
    # MÉTODOS AUXILIARES
    # ========================================================================
    
    def _build_search_query(self, address: str, parcel_id: Optional[str] = None) -> str:
        """
        Constrói query otimizada para Google Custom Search.
        
        Estratégias:
        - Usar aspas para termos exatos
        - Incluir "indiana" para localizar
        - Usar operador site: se conhecemos URL do condado
        """
        # Limpar endereço (remover vírgulas, etc)
        clean_address = address.replace(',', ' ').strip()
        
        if parcel_id:
            # Se temos parcel ID, priorizar
            query = f'"{parcel_id}" "{clean_address}" {self.county_name} indiana owner'
        else:
            # Query genérica
            query = f'"{clean_address}" {self.county_name} county assessor indiana property owner'
        
        logger.debug(f"Query construída: {query}")
        return query
    
    def _search(self, query: str) -> List[Dict]:
        """
        Executa busca no Google Custom Search API.
        
        Args:
            query: String de busca
        
        Returns:
            Lista de resultados (dicts com 'title', 'link', 'snippet')
        """
        # Verificar cache
        if query in self._search_cache:
            logger.debug("✅ Usando resultado em cache")
            return self._search_cache[query]
        
        try:
            params = {
                'key': self.api_key,
                'cx': self.search_engine_id,
                'q': query,
                'num': 10  # Top 10 resultados
            }
            
            response = requests.get(
                self.base_url,
                params=params,
                timeout=10
            )
            
            response.raise_for_status()
            data = response.json()
            
            # Extrair itens
            items = data.get('items', [])
            
            # Cachear
            self._search_cache[query] = items
            
            logger.info(f"✅ Encontrados {len(items)} resultados no Google")
            return items
        
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Erro na busca do Google: {e}")
            return []
        
        except Exception as e:
            logger.error(f"❌ Erro inesperado: {e}")
            return []
    
    def _parse_search_results(
        self,
        results: List[Dict],
        address: str,
        lat: float,
        lon: float,
        parcel_id: Optional[str] = None
    ) -> Optional[OwnerRecord]:
        """
        Extrai informações de proprietário dos resultados de busca.
        
        ⚠️ HEURÍSTICA: Busca por padrões comuns em snippets/títulos.
        
        Padrões típicos em County Assessor sites:
        - "Owner: JOHN DOE"
        - "Taxpayer Name: ABC PROPERTY LLC"
        - "Owner's Mailing Address: 123 Main St, City, ST ZIP"
        """
        for result in results:
            snippet = result.get('snippet', '')
            title = result.get('title', '')
            link = result.get('link', '')
            
            # Tentar extrair nome do proprietário
            owner_name = self._extract_owner_name(snippet + " " + title)
            
            if owner_name:
                # Tentar extrair endereço
                mailing_address = self._extract_mailing_address(snippet)
                
                # Criar registro
                record = OwnerRecord(
                    owner_name_1=owner_name,
                    mailing_address_line1=mailing_address.get('line1', ''),
                    mailing_city=mailing_address.get('city', ''),
                    mailing_state=mailing_address.get('state', 'IN'),
                    mailing_zip=mailing_address.get('zip', ''),
                    parcel_id=parcel_id or '',
                    property_address=address,
                    source=f"{self.county_name} (Google Search)",
                    source_url=link,
                    notes="⚠️ Extraído via Google Search - REQUER VALIDAÇÃO MANUAL"
                )
                
                # Calcular confidence (sempre baixo para web search)
                record.confidence_score = self.calculate_confidence_score(record) * 0.5  # Penalizar por ser web search
                record.needs_manual_review = True  # SEMPRE requer revisão
                
                logger.info(f"✅ Proprietário extraído: {owner_name}")
                return record
        
        logger.warning("❌ Não foi possível extrair proprietário dos resultados")
        return None
    
    def _extract_owner_name(self, text: str) -> Optional[str]:
        """
        Extrai nome do proprietário usando regex.
        
        Padrões comuns:
        - "Owner: JOHN DOE"
        - "Taxpayer: ABC LLC"
        - "Property Owner: SMITH FAMILY TRUST"
        """
        patterns = [
            r'Owner[:\s]+([A-Z\s&,\.]+)',
            r'Taxpayer[:\s]+([A-Z\s&,\.]+)',
            r'Property Owner[:\s]+([A-Z\s&,\.]+)',
            r'Mailing Name[:\s]+([A-Z\s&,\.]+)',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                name = match.group(1).strip()
                # Validar que não é muito curto
                if len(name) > 3:
                    return name
        
        return None
    
    def _extract_mailing_address(self, text: str) -> Dict[str, str]:
        """
        Extrai endereço de correspondência do snippet.
        
        Padrões:
        - "123 Main St, Indianapolis, IN 46204"
        - "PO Box 123, Fort Wayne, IN 46802"
        """
        # Pattern básico para endereços
        # Formato: [Número] [Rua], [Cidade], [Estado] [ZIP]
        pattern = r'(\d+\s+[A-Za-z\s]+),\s*([A-Za-z\s]+),\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)'
        
        match = re.search(pattern, text)
        
        if match:
            return {
                'line1': match.group(1).strip(),
                'city': match.group(2).strip(),
                'state': match.group(3).strip(),
                'zip': match.group(4).strip()
            }
        
        return {
            'line1': '',
            'city': '',
            'state': 'IN',
            'zip': ''
        }


# ============================================================================
# MOCK FETCHER (para desenvolvimento sem consumir API quota)
# ============================================================================

class MockFetcher(CountyAssessorFetcher):
    """
    Fetcher MOCK para testes e desenvolvimento.
    
    Retorna dados fictícios sem fazer requests reais.
    
    ⚠️ APENAS PARA DESENVOLVIMENTO!
    """
    
    def __init__(self, county_name: str):
        super().__init__(county_name=county_name, system_type="Mock (Development)")
    
    def _get_base_url(self) -> str:
        return "http://mock.local"
    
    def lookup_owner(
        self,
        address: str,
        lat: float,
        lon: float,
        parcel_id: Optional[str] = None
    ) -> FetchResult:
        """Retorna proprietário fictício."""
        import random
        
        # Simular delay de rede
        time.sleep(random.uniform(0.5, 1.5))
        
        # 80% de sucesso
        if random.random() < 0.8:
            record = OwnerRecord(
                owner_name_1="MOCK PROPERTY OWNER LLC",
                mailing_address_line1="123 FAKE ST STE 100",
                mailing_city="MOCKVILLE",
                mailing_state="IN",
                mailing_zip="46000",
                parcel_id=parcel_id or "00-00-00-000-000.000-000",
                property_address=address,
                property_class_code=PropertyClassCode.MOBILE_HOME.value,
                assessed_value=1500000.00,
                tax_year=2024,
                source=f"{self.county_name} (MOCK)",
                source_url="http://mock.local/property/123",
                notes="⚠️ DADOS FICTÍCIOS - MOCK FETCHER"
            )
            
            record.confidence_score = self.calculate_confidence_score(record)
            
            self._increment_stats(success=True)
            return FetchResult(success=True, records=[record])
        else:
            self._increment_stats(success=False)
            return FetchResult(
                success=False,
                error_message="Mock: Proprietário não encontrado (20% chance)"
            )
    
    def search_by_parcel_id(self, parcel_id: str) -> FetchResult:
        """Retorna proprietário fictício por parcel ID."""
        return self.lookup_owner(
            address="MOCK ADDRESS FROM PARCEL",
            lat=39.7684,
            lon=-86.1581,
            parcel_id=parcel_id
        )


# ============================================================================
# FACTORY FUNCTION
# ============================================================================

def get_fetcher_for_county(county_name: str, use_mock: bool = False) -> CountyAssessorFetcher:
    """
    Factory function que retorna o fetcher apropriado para um condado.
    
    Args:
        county_name: Nome do condado (ex: "Marion County")
        use_mock: Se True, retorna MockFetcher (para desenvolvimento)
    
    Returns:
        Instância de CountyAssessorFetcher apropriada
    
    Example:
        >>> fetcher = get_fetcher_for_county("Marion County")
        >>> result = fetcher.lookup_owner("123 Main St, Indianapolis, IN", 39.7684, -86.1581)
    """
    if use_mock:
        logger.warning(f"⚠️ Usando MOCK fetcher para {county_name}")
        return MockFetcher(county_name)
    
    # TODO: Implementar fetchers específicos por sistema
    # Mapeamento de condados para sistemas
    BEACON_COUNTIES = [
        'Marion County', 'Lake County', 'Hamilton County', 
        'St. Joseph County', 'Elkhart County', 'Tippecanoe County'
        # ... adicionar todos os ~40 condados Beacon
    ]
    
    VANGUARD_COUNTIES = [
        'Brown County', 'Daviess County', 'Dubois County'
        # ... adicionar todos os ~15 condados Vanguard
    ]
    
    # Selecionar fetcher baseado no condado
    if county_name in BEACON_COUNTIES:
        # TODO: from src.owners.fetchers.beacon_fetcher import BeaconFetcher
        # return BeaconFetcher(county_name)
        logger.warning(f"BeaconFetcher não implementado para {county_name}, usando GenericWebSearchFetcher")
        return GenericWebSearchFetcher(county_name)
    
    elif county_name in VANGUARD_COUNTIES:
        # TODO: from src.owners.fetchers.vanguard_fetcher import VanguardFetcher
        # return VanguardFetcher(county_name)
        logger.warning(f"VanguardFetcher não implementado para {county_name}, usando GenericWebSearchFetcher")
        return GenericWebSearchFetcher(county_name)
    
    else:
        # Fallback genérico
        logger.info(f"Usando GenericWebSearchFetcher para {county_name}")
        return GenericWebSearchFetcher(county_name)


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    """
    Teste do fetcher genérico.
    """
    import sys
    from loguru import logger
    
    logger.remove()
    logger.add(sys.stderr, level="DEBUG")
    
    print("=" * 80)
    print("GENERIC FETCHER - Teste")
    print("=" * 80)
    
    # Usar MOCK para não consumir quota do Google
    print("\n🧪 Testando MockFetcher...")
    print("-" * 80)
    
    fetcher = MockFetcher("Test County")
    
    result = fetcher.lookup_owner(
        address="123 Test Lane, Indianapolis, IN",
        lat=39.7684,
        lon=-86.1581,
        parcel_id="99-99-99-999-999.999-999"
    )
    
    if result.success:
        print("\n✅ Proprietário encontrado!")
        record = result.records[0]
        print(f"  Nome: {record.owner_name_1}")
        print(f"  Endereço: {record.mailing_address_line1}, {record.mailing_city}, {record.mailing_state}")
        print(f"  Parcel ID: {record.parcel_id}")
        print(f"  Confidence: {record.confidence_score:.2f}")
        print(f"  Notas: {record.notes}")
    else:
        print(f"\n❌ Falha: {result.error_message}")
    
    # Estatísticas
    print("\n" + "-" * 80)
    print("📊 Estatísticas:")
    stats = fetcher.get_statistics()
    for key, value in stats.items():
        print(f"  {key}: {value}")
    
    print("\n" + "=" * 80)
    print("💡 Para usar GenericWebSearchFetcher real:")
    print("   1. Obter API key: https://developers.google.com/custom-search/v1/overview")
    print("   2. Configurar em .env:")
    print("      GOOGLE_CUSTOM_SEARCH_API_KEY=sua_chave")
    print("      GOOGLE_CUSTOM_SEARCH_ENGINE_ID=seu_id")
    print("   3. Executar: fetcher = GenericWebSearchFetcher('County Name')")
    print("=" * 80)
