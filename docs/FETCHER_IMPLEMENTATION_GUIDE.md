# 🔧 Implementando Fetchers Específicos

Este guia mostra como implementar fetchers para sistemas específicos de County Assessor.

## 📋 Índice

1. [Estrutura Base](#estrutura-base)
2. [Beacon/Schneider Corp Fetcher](#beaconschneider-corp-fetcher)
3. [Vanguard Fetcher](#vanguard-fetcher)
4. [Custom GIS Fetcher](#custom-gis-fetcher)
5. [Testando Fetchers](#testando-fetchers)

---

## Estrutura Base

Todos os fetchers devem:

1. Herdar de `CountyAssessorFetcher`
2. Implementar `lookup_owner()` e `search_by_parcel_id()`
3. Retornar `FetchResult` com lista de `OwnerRecord`
4. Respeitar rate limits
5. Tratar erros apropriadamente

---

## Beacon/Schneider Corp Fetcher

### Template Básico

```python
"""
Beacon/Schneider Corp Fetcher
==============================

Scraper para ~40 condados de Indiana que usam Beacon.

URL Pattern: https://beacon.schneidercorp.com/Application.aspx?AppID=XXX

PROTEÇÕES:
- Rate limit: 10-20 req/min
- CAPTCHA após ~50 requests
- Session cookies necessários

Author: BellaTerra Intelligence Team
"""

import time
import re
from typing import Optional
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from loguru import logger

from src.owners.base_fetcher import (
    CountyAssessorFetcher,
    FetchResult,
    OwnerRecord,
    PropertyClassCode,
    RateLimiter,
    get_random_user_agent
)


class BeaconFetcher(CountyAssessorFetcher):
    """
    Fetcher para condados que usam sistema Beacon/Schneider Corp.

    Condados suportados:
    - Marion County (AppID=231)
    - Lake County (AppID=1018)
    - Hamilton County (AppID=163)
    - St. Joseph County (AppID=1008)
    ... adicionar outros
    """

    # Mapeamento de condados para AppID do Beacon
    BEACON_APP_IDS = {
        'Marion County': 231,
        'Lake County': 1018,
        'Hamilton County': 163,
        'St. Joseph County': 1008,
        'Elkhart County': 144,
        'Tippecanoe County': 1021,
        # TODO: Adicionar todos os ~40 condados
    }

    def __init__(self, county_name: str):
        """
        Args:
            county_name: Nome do condado (ex: "Marion County")
        """
        if county_name not in self.BEACON_APP_IDS:
            raise ValueError(
                f"Condado {county_name} não suportado. "
                f"Condados disponíveis: {list(self.BEACON_APP_IDS.keys())}"
            )

        super().__init__(county_name=county_name, system_type="Beacon/Schneider Corp")

        self.app_id = self.BEACON_APP_IDS[county_name]

        # Rate limiter: 10 req/min para segurança
        self.rate_limiter = RateLimiter(requests_per_minute=10)

        # Session para cookies
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml',
            'Accept-Language': 'en-US,en;q=0.9',
        })

    def _get_base_url(self) -> str:
        """URL base do Beacon para este condado."""
        return f"https://beacon.schneidercorp.com/Application.aspx?AppID={self.app_id}"

    def lookup_owner(
        self,
        address: str,
        lat: float,
        lon: float,
        parcel_id: Optional[str] = None
    ) -> FetchResult:
        """
        Busca proprietário no sistema Beacon.

        Estratégia:
        1. Se parcel_id disponível, busca direta
        2. Senão, busca por endereço
        3. Parsear HTML da página de resultados
        4. Extrair dados do proprietário
        """
        self.rate_limiter.wait()

        if parcel_id:
            return self.search_by_parcel_id(parcel_id)

        # Busca por endereço
        logger.info(f"🔍 Buscando no Beacon ({self.county_name}): {address}")

        try:
            # Passo 1: GET da página inicial (para cookies/session)
            response = self.session.get(self.base_url, timeout=10)
            response.raise_for_status()

            # Passo 2: POST do formulário de busca
            search_url = f"{self.base_url}&PageTypeID=2&PageID=1"

            # Extrair __VIEWSTATE e __EVENTVALIDATION (ASP.NET)
            soup = BeautifulSoup(response.text, 'html.parser')
            viewstate = soup.find('input', {'name': '__VIEWSTATE'})
            eventvalidation = soup.find('input', {'name': '__EVENTVALIDATION'})

            form_data = {
                '__VIEWSTATE': viewstate['value'] if viewstate else '',
                '__EVENTVALIDATION': eventvalidation['value'] if eventvalidation else '',
                'txtAddress': address,
                'btnSearch': 'Search'
            }

            search_response = self.session.post(
                search_url,
                data=form_data,
                timeout=10
            )
            search_response.raise_for_status()

            # Passo 3: Parsear resultados
            records = self._parse_search_results(search_response.text)

            if records:
                self._increment_stats(success=True)
                return FetchResult(success=True, records=records)
            else:
                self._increment_stats(success=False)
                return FetchResult(
                    success=False,
                    error_message="Nenhum resultado encontrado no Beacon"
                )

        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de rede no Beacon: {e}")
            self._increment_stats(success=False)
            return FetchResult(
                success=False,
                error_message=f"Erro de rede: {e}"
            )

        except Exception as e:
            logger.error(f"Erro inesperado no Beacon: {e}")
            self._increment_stats(success=False)
            return FetchResult(
                success=False,
                error_message=f"Erro: {e}"
            )

    def search_by_parcel_id(self, parcel_id: str) -> FetchResult:
        """
        Busca direta por Parcel ID (mais rápido e preciso).

        URL: .../Application.aspx?AppID=XXX&...&ParcelID=12345
        """
        self.rate_limiter.wait()

        normalized_id = self.normalize_parcel_id(parcel_id)
        logger.info(f"🔍 Buscando Parcel ID no Beacon: {normalized_id}")

        try:
            # URL direta para parcel
            parcel_url = f"{self.base_url}&PageTypeID=4&ParcelID={normalized_id}"

            response = self.session.get(parcel_url, timeout=10)
            response.raise_for_status()

            # Parsear página do parcel
            record = self._parse_parcel_page(response.text, parcel_id)

            if record:
                self._increment_stats(success=True)
                return FetchResult(success=True, records=[record])
            else:
                self._increment_stats(success=False)
                return FetchResult(
                    success=False,
                    error_message=f"Parcel {parcel_id} não encontrado"
                )

        except Exception as e:
            logger.error(f"Erro ao buscar parcel: {e}")
            self._increment_stats(success=False)
            return FetchResult(
                success=False,
                error_message=f"Erro: {e}"
            )

    def _parse_search_results(self, html: str) -> list[OwnerRecord]:
        """
        Parsear HTML da página de resultados de busca.

        Beacon retorna uma tabela com resultados.
        """
        soup = BeautifulSoup(html, 'html.parser')
        records = []

        # Beacon usa tabelas para resultados
        # TODO: Ajustar seletores baseado no HTML real do Beacon
        results_table = soup.find('table', {'id': re.compile('.*SearchResults.*')})

        if not results_table:
            return records

        rows = results_table.find_all('tr')[1:]  # Pular header

        for row in rows:
            cells = row.find_all('td')

            if len(cells) < 4:
                continue

            # Extrair dados (ajustar índices baseado no HTML real)
            parcel_id = cells[0].get_text(strip=True)
            property_address = cells[1].get_text(strip=True)
            owner_name = cells[2].get_text(strip=True)

            # Link para página de detalhes
            detail_link = cells[0].find('a')
            detail_url = detail_link['href'] if detail_link else ''

            # Criar registro básico (sem mailing address ainda)
            # Precisaria fazer request adicional para página de detalhes
            record = OwnerRecord(
                owner_name_1=owner_name,
                mailing_address_line1='',  # Requer página de detalhes
                mailing_city='',
                mailing_state='IN',
                mailing_zip='',
                parcel_id=parcel_id,
                property_address=property_address,
                source=f"{self.county_name} Beacon",
                source_url=f"{self.base_url}&{detail_url}",
                notes="Mailing address requer fetch adicional"
            )

            record.confidence_score = self.calculate_confidence_score(record)
            record.needs_manual_review = True  # Sem mailing address completo

            records.append(record)

        return records

    def _parse_parcel_page(self, html: str, parcel_id: str) -> Optional[OwnerRecord]:
        """
        Parsear página de detalhes de um parcel.

        Aqui temos TODOS os dados incluindo mailing address.
        """
        soup = BeautifulSoup(html, 'html.parser')

        # TODO: Ajustar seletores baseado no HTML real
        # Beacon geralmente usa spans com IDs específicos

        try:
            # Proprietário
            owner_elem = soup.find('span', {'id': re.compile('.*Owner.*Name.*')})
            owner_name = owner_elem.get_text(strip=True) if owner_elem else ''

            # Mailing Address
            addr_line1_elem = soup.find('span', {'id': re.compile('.*Mailing.*Address.*Line1.*')})
            addr_city_elem = soup.find('span', {'id': re.compile('.*Mailing.*City.*')})
            addr_state_elem = soup.find('span', {'id': re.compile('.*Mailing.*State.*')})
            addr_zip_elem = soup.find('span', {'id': re.compile('.*Mailing.*Zip.*')})

            mailing_line1 = addr_line1_elem.get_text(strip=True) if addr_line1_elem else ''
            mailing_city = addr_city_elem.get_text(strip=True) if addr_city_elem else ''
            mailing_state = addr_state_elem.get_text(strip=True) if addr_state_elem else 'IN'
            mailing_zip = addr_zip_elem.get_text(strip=True) if addr_zip_elem else ''

            # Property Address
            prop_addr_elem = soup.find('span', {'id': re.compile('.*Property.*Address.*')})
            property_address = prop_addr_elem.get_text(strip=True) if prop_addr_elem else ''

            # Property Class Code
            class_elem = soup.find('span', {'id': re.compile('.*Property.*Class.*')})
            property_class = class_elem.get_text(strip=True) if class_elem else PropertyClassCode.UNKNOWN.value

            # Assessed Value
            value_elem = soup.find('span', {'id': re.compile('.*Assessed.*Value.*')})
            assessed_value_str = value_elem.get_text(strip=True).replace('$', '').replace(',', '') if value_elem else '0'
            assessed_value = float(assessed_value_str) if assessed_value_str else 0.0

            # Criar registro completo
            record = OwnerRecord(
                owner_name_1=owner_name,
                mailing_address_line1=mailing_line1,
                mailing_city=mailing_city,
                mailing_state=mailing_state,
                mailing_zip=mailing_zip,
                parcel_id=parcel_id,
                property_address=property_address,
                property_class_code=property_class,
                assessed_value=assessed_value,
                tax_year=2024,  # TODO: Extrair do HTML
                source=f"{self.county_name} Beacon",
                source_url=f"{self.base_url}&ParcelID={parcel_id}"
            )

            record.confidence_score = self.calculate_confidence_score(record)

            return record

        except Exception as e:
            logger.error(f"Erro ao parsear parcel page: {e}")
            return None


# ============================================================================
# FACTORY: Atualizar para incluir BeaconFetcher
# ============================================================================

def get_fetcher_for_county(county_name: str, use_mock: bool = False):
    """
    Factory atualizada com BeaconFetcher.
    """
    if use_mock:
        from src.owners.fetchers.generic_fetcher import MockFetcher
        return MockFetcher(county_name)

    # Tentar Beacon primeiro
    if county_name in BeaconFetcher.BEACON_APP_IDS:
        logger.info(f"Usando BeaconFetcher para {county_name}")
        return BeaconFetcher(county_name)

    # Fallback para generic
    else:
        from src.owners.fetchers.generic_fetcher import GenericWebSearchFetcher
        logger.info(f"Usando GenericWebSearchFetcher para {county_name}")
        return GenericWebSearchFetcher(county_name)
```

### Passos para Implementação Real

1. **Inspecionar HTML real do Beacon**:

   ```python
   import requests
   from bs4 import BeautifulSoup

   url = "https://beacon.schneidercorp.com/Application.aspx?AppID=231&PageTypeID=4&ParcelID=49-07-15-203-017.000-006"
   response = requests.get(url)
   soup = BeautifulSoup(response.text, 'html.parser')

   # Identificar IDs/classes dos elementos
   print(soup.prettify())
   ```

2. **Ajustar seletores CSS/regex** baseado no HTML

3. **Testar com parcel IDs conhecidos**

4. **Validar parsing de todos os campos**

---

## Vanguard Fetcher

```python
"""
Vanguard Appraisals Fetcher
============================

Scraper para ~15 condados que usam Vanguard.

URL Pattern: http://www.vanguardappraisals.com/{county}/

PROTEÇÕES:
- Rate limit: ~30 req/min (mais relaxado)
- Sem CAPTCHA geralmente

Author: BellaTerra Intelligence Team
"""

import requests
from bs4 import BeautifulSoup

from src.owners.base_fetcher import (
    CountyAssessorFetcher,
    FetchResult,
    OwnerRecord,
    RateLimiter
)


class VanguardFetcher(CountyAssessorFetcher):
    """
    Fetcher para condados que usam Vanguard Appraisals.

    Condados suportados:
    - Brown County
    - Daviess County
    - Dubois County
    ... adicionar outros
    """

    VANGUARD_COUNTIES = {
        'Brown County': 'brown',
        'Daviess County': 'daviess',
        'Dubois County': 'dubois',
        # TODO: Adicionar todos os ~15 condados
    }

    def __init__(self, county_name: str):
        if county_name not in self.VANGUARD_COUNTIES:
            raise ValueError(f"Condado {county_name} não suportado")

        super().__init__(county_name=county_name, system_type="Vanguard Appraisals")

        self.county_slug = self.VANGUARD_COUNTIES[county_name]
        self.rate_limiter = RateLimiter(requests_per_minute=20)

    def _get_base_url(self) -> str:
        return f"http://www.vanguardappraisals.com/{self.county_slug}/"

    def lookup_owner(self, address, lat, lon, parcel_id=None):
        self.rate_limiter.wait()

        # TODO: Implementar lógica específica do Vanguard
        # Similar ao BeaconFetcher mas com seletores diferentes

        pass

    def search_by_parcel_id(self, parcel_id):
        self.rate_limiter.wait()

        # TODO: Implementar
        pass
```

---

## Custom GIS Fetcher

Para condados com sistemas únicos (ex: Allen County):

```python
"""
Allen County Custom GIS Fetcher
================================

Scraper específico para Allen County GIS.

URL: https://maps.acgov.org/Html5Viewer/?viewer=public

CARACTERÍSTICAS:
- Sistema GIS interativo baseado em mapa
- Requer coordenadas lat/lon para busca
- JavaScript-heavy (considerar Selenium)

Author: BellaTerra Intelligence Team
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from src.owners.base_fetcher import (
    CountyAssessorFetcher,
    FetchResult,
    OwnerRecord
)


class AllenCountyGISFetcher(CountyAssessorFetcher):
    """
    Fetcher para Allen County usando Selenium.
    """

    def __init__(self):
        super().__init__(
            county_name="Allen County",
            system_type="Custom GIS"
        )

        # Configurar Selenium
        options = webdriver.ChromeOptions()
        options.add_argument('--headless')  # Sem GUI
        options.add_argument('--no-sandbox')

        self.driver = webdriver.Chrome(options=options)

    def _get_base_url(self) -> str:
        return "https://maps.acgov.org/Html5Viewer/?viewer=public"

    def lookup_owner(self, address, lat, lon, parcel_id=None):
        try:
            self.driver.get(self.base_url)

            # Aguardar mapa carregar
            WebDriverWait(self.driver, 10).until(
                EC.presence_of_element_located((By.ID, "map"))
            )

            # TODO: Interagir com mapa JavaScript
            # - Clicar no ponto lat/lon
            # - Aguardar popup
            # - Extrair dados

            pass

        finally:
            # Cleanup
            pass

    def search_by_parcel_id(self, parcel_id):
        # TODO: Implementar
        pass

    def __del__(self):
        """Fechar driver ao destruir objeto."""
        if hasattr(self, 'driver'):
            self.driver.quit()
```

---

## Testando Fetchers

### Teste Unitário

```python
# tests/test_beacon_fetcher.py

import pytest
from src.owners.fetchers.beacon_fetcher import BeaconFetcher
from src.owners.base_fetcher import PropertyClassCode


def test_beacon_marion_county():
    """Testa BeaconFetcher para Marion County."""
    fetcher = BeaconFetcher("Marion County")

    # Parcel ID conhecido (exemplo)
    parcel_id = "49-07-15-203-017.000-006"

    result = fetcher.search_by_parcel_id(parcel_id)

    assert result.success
    assert len(result.records) > 0

    record = result.records[0]
    assert record.owner_name_1 != ''
    assert record.mailing_zip != ''
    assert record.parcel_id == parcel_id


def test_beacon_invalid_county():
    """Testa erro para condado não suportado."""
    with pytest.raises(ValueError):
        BeaconFetcher("Fake County")


@pytest.mark.parametrize("county", [
    "Marion County",
    "Lake County",
    "Hamilton County"
])
def test_beacon_multiple_counties(county):
    """Testa múltiplos condados Beacon."""
    fetcher = BeaconFetcher(county)
    assert fetcher.app_id > 0
    assert fetcher.base_url.startswith("https://beacon.schneidercorp.com")
```

### Teste de Integração

```python
# tests/integration/test_owner_lookup_flow.py

from src.owners.orchestrator import OwnerLookupOrchestrator
from src.database import get_db_session
from sqlalchemy import text


def test_full_owner_lookup_flow():
    """
    Teste end-to-end do fluxo de identificação de proprietários.
    """
    # Usar MOCK para não consumir APIs
    orchestrator = OwnerLookupOrchestrator(
        use_mock=True,
        delay_between_requests=0.1
    )

    # Processar apenas 3 parques
    orchestrator.process_all_parks(limit=3)

    # Verificar que proprietários foram salvos
    with get_db_session() as session:
        result = session.execute(
            text("SELECT COUNT(*) FROM owners")
        ).fetchone()

        owner_count = result[0]
        assert owner_count >= 1, "Pelo menos 1 proprietário deveria ser salvo"


def test_owner_park_relationship():
    """Testa relacionamento entre owners e parks_master."""
    with get_db_session() as session:
        # Buscar parque com proprietário
        result = session.execute(text("""
            SELECT p.id, p.name, o.full_name, o.mailing_address
            FROM parks_master p
            JOIN owners o ON p.owner_id = o.id
            LIMIT 1
        """)).fetchone()

        if result:
            park_id, park_name, owner_name, mailing_addr = result

            assert park_id > 0
            assert park_name != ''
            assert owner_name != ''

            print(f"✅ Parque: {park_name}")
            print(f"   Proprietário: {owner_name}")
```

---

## 📚 Recursos Adicionais

### Ferramentas Úteis

1. **BeautifulSoup**: Parsing de HTML
2. **Selenium**: Sites JavaScript-heavy
3. **Scrapy**: Framework completo de scraping
4. **requests-html**: Combina requests + parsing

### Debugging

```python
# Salvar HTML para análise
with open('debug_beacon.html', 'w', encoding='utf-8') as f:
    f.write(response.text)

# Inspecionar elementos
soup = BeautifulSoup(html, 'html.parser')
print(soup.find_all('span'))  # Todos os spans
print(soup.find_all(id=re.compile('Owner')))  # IDs com "Owner"
```

### Anti-Bloqueio

```python
# Headers completos
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.5',
    'Accept-Encoding': 'gzip, deflate',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1'
}

# Delays variáveis
import random
time.sleep(random.uniform(2.0, 5.0))
```

---

**Próximo Passo**: Implementar `beacon_fetcher.py` completo após analisar HTML real do sistema Beacon.
