"""
Base Fetcher para County Assessor Records
==========================================

Classe abstrata que define a interface para buscar proprietários em registros
fiscais de condados (County Assessor Records).

Indiana possui 92 condados com sistemas diversos:

SISTEMAS PRINCIPAIS:
--------------------
1. Beacon/Schneider Corp (~40 condados)
   - URL padrão: https://beacon.schneidercorp.com/Application.aspx?AppID=XXX
   - Proteções: Rate limiting (10-20 req/min), CAPTCHA após muitos requests
   - Estratégia: Delays de 3-5 segundos, rotação de User-Agent
   - Alternativa: Selenium com perfil humanizado

2. Vanguard Appraisals (~15 condados)
   - URL padrão: http://www.vanguardappraisals.com/{county}/
   - Proteções: Moderadas, permite scraping com delays
   - Estratégia: Delays de 2-3 segundos suficientes

3. GIS Customizados (~25 condados)
   - Cada condado tem implementação única
   - Proteções: Variam (alguns sem proteção, outros com WAF)
   - Estratégia: Análise individual por condado

4. Sem sistema online (~12 condados)
   - Requer contato telefônico ou visita presencial
   - Alternativa: Solicitar registros em lote via FOIA (Freedom of Information Act)

⚠️ CONSIDERAÇÕES LEGAIS:
-------------------------
- Todos os County Assessor Records são PÚBLICOS por lei de Indiana (IC 36-2-9)
- Web scraping de dados públicos é geralmente legal (hiQ Labs v. LinkedIn)
- PORÉM: Respeitar robots.txt e Terms of Service
- Implementar delays para não sobrecarregar servidores públicos
- Considerar compra de dados de provedores comerciais para grandes volumes

Author: BellaTerra Intelligence Team
Date: December 2025
"""

from abc import ABC, abstractmethod
from typing import Dict, Optional, List
from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from loguru import logger


class PropertyClassCode(Enum):
    """
    Códigos de classificação de propriedades em Indiana.
    
    Fonte: Indiana State Board of Tax Commissioners
    """
    RESIDENTIAL = "100"  # Residencial padrão
    MOBILE_HOME = "102"  # Mobile Home (nosso foco!)
    COMMERCIAL = "300"   # Comercial
    INDUSTRIAL = "400"   # Industrial
    AGRICULTURAL = "500" # Agrícola
    EXEMPT = "600"       # Isento (igreja, governo, etc)
    UNKNOWN = "999"      # Não classificado


@dataclass
class OwnerRecord:
    """
    Registro padronizado de proprietário retornado pelos fetchers.
    
    Todos os fetchers devem retornar este formato, independente da fonte.
    """
    # Identificação do proprietário
    owner_name_1: str  # Nome principal (pessoa ou empresa)
    owner_name_2: Optional[str] = None  # Co-proprietário (se existir)
    
    # Endereço para mala direta
    mailing_address_line1: str = ""
    mailing_address_line2: Optional[str] = None
    mailing_city: str = ""
    mailing_state: str = ""
    mailing_zip: str = ""
    mailing_country: str = "USA"
    
    # Informações da propriedade
    parcel_id: str = ""  # Número da parcela (PIN - Parcel Identification Number)
    property_address: str = ""
    property_class_code: str = PropertyClassCode.UNKNOWN.value
    
    # Dados fiscais (opcionais mas úteis)
    assessed_value: Optional[float] = None
    tax_year: Optional[int] = None
    
    # Metadados da busca
    source: str = ""  # Ex: "Marion County Beacon"
    source_url: str = ""  # URL onde foi encontrado
    fetched_at: datetime = None
    confidence_score: float = 0.0  # 0.0 a 1.0
    
    # Flags de validação
    is_valid_mailing_address: bool = False
    is_commercial_property: bool = False
    needs_manual_review: bool = False
    notes: str = ""
    
    def __post_init__(self):
        """Validações após inicialização."""
        if self.fetched_at is None:
            self.fetched_at = datetime.now()
        
        # Detectar se é propriedade comercial
        if self.property_class_code in ["300", "400"]:
            self.is_commercial_property = True
        
        # Validar endereço para mala direta
        self._validate_mailing_address()
    
    def _validate_mailing_address(self):
        """
        Valida se o endereço é adequado para mala direta.
        
        Critérios:
        - Nome não vazio
        - Endereço linha 1 não vazio
        - Cidade não vazia
        - CEP não vazio (formato: 12345 ou 12345-6789)
        """
        has_name = bool(self.owner_name_1 and self.owner_name_1.strip())
        has_address = bool(self.mailing_address_line1 and self.mailing_address_line1.strip())
        has_city = bool(self.mailing_city and self.mailing_city.strip())
        has_zip = bool(self.mailing_zip and self.mailing_zip.strip())
        
        # Validar formato do ZIP
        valid_zip = False
        if has_zip:
            zip_clean = self.mailing_zip.replace("-", "").strip()
            valid_zip = len(zip_clean) in [5, 9] and zip_clean.isdigit()
        
        self.is_valid_mailing_address = (
            has_name and has_address and has_city and has_zip and valid_zip
        )
        
        # Marcar para revisão manual se endereço incompleto
        if not self.is_valid_mailing_address:
            self.needs_manual_review = True
            if not has_name:
                self.notes += "Nome ausente. "
            if not has_address:
                self.notes += "Endereço ausente. "
            if not has_city:
                self.notes += "Cidade ausente. "
            if not valid_zip:
                self.notes += "CEP inválido. "
    
    def to_dict(self) -> Dict:
        """Converte para dicionário (útil para inserção no banco)."""
        return {
            'owner_name_1': self.owner_name_1,
            'owner_name_2': self.owner_name_2,
            'mailing_address_line1': self.mailing_address_line1,
            'mailing_address_line2': self.mailing_address_line2,
            'mailing_city': self.mailing_city,
            'mailing_state': self.mailing_state,
            'mailing_zip': self.mailing_zip,
            'mailing_country': self.mailing_country,
            'parcel_id': self.parcel_id,
            'property_address': self.property_address,
            'property_class_code': self.property_class_code,
            'assessed_value': self.assessed_value,
            'tax_year': self.tax_year,
            'source': self.source,
            'source_url': self.source_url,
            'fetched_at': self.fetched_at.isoformat() if self.fetched_at else None,
            'confidence_score': self.confidence_score,
            'is_valid_mailing_address': self.is_valid_mailing_address,
            'is_commercial_property': self.is_commercial_property,
            'needs_manual_review': self.needs_manual_review,
            'notes': self.notes.strip()
        }


class FetchResult:
    """
    Resultado de uma busca de proprietário.
    
    Encapsula o sucesso/falha e possíveis múltiplos registros encontrados.
    """
    
    def __init__(
        self,
        success: bool,
        records: List[OwnerRecord] = None,
        error_message: str = "",
        retry_after_seconds: Optional[int] = None
    ):
        self.success = success
        self.records = records or []
        self.error_message = error_message
        self.retry_after_seconds = retry_after_seconds  # Para rate limiting
    
    @property
    def found_owner(self) -> bool:
        """Retorna True se encontrou pelo menos um proprietário."""
        return self.success and len(self.records) > 0
    
    @property
    def multiple_matches(self) -> bool:
        """Retorna True se encontrou múltiplas parcelas/proprietários."""
        return len(self.records) > 1


class CountyAssessorFetcher(ABC):
    """
    Classe abstrata base para todos os fetchers de County Assessor.
    
    Cada condado/sistema deve implementar sua própria subclasse.
    
    Exemplo de implementação:
        class MarionCountyBeaconFetcher(CountyAssessorFetcher):
            def lookup_owner(self, address, lat, lon):
                # Implementação específica para Marion County (Beacon)
                ...
    """
    
    def __init__(self, county_name: str, system_type: str):
        """
        Inicializa o fetcher.
        
        Args:
            county_name: Nome do condado (ex: "Marion County")
            system_type: Tipo de sistema (ex: "Beacon", "Vanguard", "Custom GIS")
        """
        self.county_name = county_name
        self.system_type = system_type
        self.base_url = self._get_base_url()
        
        # Estatísticas de uso
        self.total_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.rate_limited_count = 0
    
    @abstractmethod
    def _get_base_url(self) -> str:
        """
        Retorna a URL base do sistema de registros do condado.
        
        Deve ser implementado por cada subclasse.
        """
        pass
    
    @abstractmethod
    def lookup_owner(
        self,
        address: str,
        lat: float,
        lon: float,
        parcel_id: Optional[str] = None
    ) -> FetchResult:
        """
        Busca o proprietário de uma propriedade.
        
        Args:
            address: Endereço da propriedade (ex: "123 Main St, Indianapolis, IN")
            lat: Latitude da propriedade
            lon: Longitude da propriedade
            parcel_id: ID da parcela se já conhecido (otimiza busca)
        
        Returns:
            FetchResult com lista de OwnerRecord encontrados
        
        ⚠️ IMPLEMENTAÇÕES DEVEM:
        - Respeitar rate limits (delays apropriados)
        - Tratar erros de rede (retry com backoff exponencial)
        - Detectar CAPTCHAs e retornar erro apropriado
        - Logar todas as tentativas
        - Rotacionar User-Agent se necessário
        """
        pass
    
    @abstractmethod
    def search_by_parcel_id(self, parcel_id: str) -> FetchResult:
        """
        Busca proprietário diretamente pelo Parcel ID (mais rápido e preciso).
        
        Args:
            parcel_id: ID da parcela (ex: "49-07-15-203-017.000-006")
        
        Returns:
            FetchResult com o proprietário da parcela
        """
        pass
    
    def get_statistics(self) -> Dict:
        """Retorna estatísticas de uso do fetcher."""
        success_rate = (
            (self.successful_requests / self.total_requests * 100)
            if self.total_requests > 0 else 0
        )
        
        return {
            'county': self.county_name,
            'system': self.system_type,
            'total_requests': self.total_requests,
            'successful': self.successful_requests,
            'failed': self.failed_requests,
            'rate_limited': self.rate_limited_count,
            'success_rate': f"{success_rate:.1f}%"
        }
    
    def _increment_stats(self, success: bool, rate_limited: bool = False):
        """Incrementa estatísticas de uso."""
        self.total_requests += 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        if rate_limited:
            self.rate_limited_count += 1
    
    # ========================================================================
    # MÉTODOS AUXILIARES COMPARTILHADOS
    # ========================================================================
    
    @staticmethod
    def normalize_parcel_id(parcel_id: str) -> str:
        """
        Normaliza formato de Parcel ID.
        
        Diferentes condados usam formatos diferentes:
        - Marion: 49-07-15-203-017.000-006
        - Lake: 45-27-35-300-012.000-018
        - Allen: 02-12-26-201-005.000-008
        
        Remove espaços, converte para maiúsculas.
        """
        return parcel_id.strip().upper().replace(" ", "")
    
    @staticmethod
    def parse_owner_name(raw_name: str) -> tuple[str, Optional[str]]:
        """
        Separa nome principal de co-proprietário.
        
        Formatos comuns:
        - "JOHN DOE & JANE DOE" → ("JOHN DOE", "JANE DOE")
        - "ABC PROPERTY LLC" → ("ABC PROPERTY LLC", None)
        - "SMITH FAMILY TRUST" → ("SMITH FAMILY TRUST", None)
        
        Args:
            raw_name: Nome bruto extraído do site
        
        Returns:
            Tupla (owner_name_1, owner_name_2)
        """
        raw_name = raw_name.strip()
        
        # Separadores comuns de co-proprietários
        separators = [' & ', ' AND ', ' + ']
        
        for sep in separators:
            if sep in raw_name.upper():
                parts = raw_name.upper().split(sep, 1)
                return (parts[0].strip(), parts[1].strip() if len(parts) > 1 else None)
        
        return (raw_name, None)
    
    @staticmethod
    def calculate_confidence_score(record: OwnerRecord) -> float:
        """
        Calcula score de confiança baseado na completude dos dados.
        
        Critérios (soma = 1.0):
        - Tem nome (0.2)
        - Tem endereço completo (0.3)
        - Tem parcel ID (0.2)
        - Tem property class code (0.1)
        - Tem assessed value (0.1)
        - É endereço válido para mala direta (0.1)
        
        Returns:
            Float entre 0.0 e 1.0
        """
        score = 0.0
        
        if record.owner_name_1:
            score += 0.2
        
        if (record.mailing_address_line1 and 
            record.mailing_city and 
            record.mailing_zip):
            score += 0.3
        
        if record.parcel_id:
            score += 0.2
        
        if record.property_class_code != PropertyClassCode.UNKNOWN.value:
            score += 0.1
        
        if record.assessed_value is not None and record.assessed_value > 0:
            score += 0.1
        
        if record.is_valid_mailing_address:
            score += 0.1
        
        return min(score, 1.0)  # Cap em 1.0


# ============================================================================
# RATE LIMITING HELPERS
# ============================================================================

class RateLimiter:
    """
    Implementa rate limiting para respeitar limites dos sites.
    
    ⚠️ CRÍTICO para evitar bloqueios!
    
    Uso:
        limiter = RateLimiter(requests_per_minute=10)
        
        for park in parks:
            limiter.wait()  # Aguarda se necessário
            result = fetcher.lookup_owner(...)
    """
    
    def __init__(self, requests_per_minute: int = 10):
        """
        Args:
            requests_per_minute: Máximo de requests por minuto
        """
        self.requests_per_minute = requests_per_minute
        self.min_delay_seconds = 60.0 / requests_per_minute
        self.last_request_time = None
    
    def wait(self):
        """
        Aguarda o tempo necessário para respeitar o rate limit.
        
        Deve ser chamado ANTES de cada request.
        """
        import time
        
        if self.last_request_time is not None:
            elapsed = time.time() - self.last_request_time
            if elapsed < self.min_delay_seconds:
                sleep_time = self.min_delay_seconds - elapsed
                logger.debug(f"⏳ Rate limiting: aguardando {sleep_time:.2f}s")
                time.sleep(sleep_time)
        
        self.last_request_time = time.time()


# ============================================================================
# USER AGENT ROTATION
# ============================================================================

def get_random_user_agent() -> str:
    """
    Retorna um User-Agent aleatório para evitar detecção de bots.
    
    ⚠️ Usar com moderação! Alguns sites consideram isso violação de ToS.
    
    Returns:
        String de User-Agent
    """
    import random
    
    user_agents = [
        # Chrome on Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        # Firefox on Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        # Chrome on Mac
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        # Safari on Mac
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        # Edge on Windows
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0'
    ]
    
    return random.choice(user_agents)


# ============================================================================
# TESTING
# ============================================================================

if __name__ == "__main__":
    """
    Testes básicos da estrutura.
    """
    print("=" * 80)
    print("BASE FETCHER - Teste de Estruturas")
    print("=" * 80)
    
    # Criar um registro de exemplo
    record = OwnerRecord(
        owner_name_1="SUNSET MOBILE HOME PARK LLC",
        mailing_address_line1="123 CORPORATE BLVD STE 100",
        mailing_city="NAPLES",
        mailing_state="FL",
        mailing_zip="34102",
        parcel_id="49-07-15-203-017.000-006",
        property_address="456 PARK LANE, INDIANAPOLIS, IN",
        property_class_code=PropertyClassCode.MOBILE_HOME.value,
        assessed_value=2500000.00,
        tax_year=2024,
        source="Marion County Beacon",
        source_url="https://beacon.schneidercorp.com/..."
    )
    
    # Calcular confidence score
    record.confidence_score = CountyAssessorFetcher.calculate_confidence_score(record)
    
    print("\n📋 Registro de Exemplo:")
    print("-" * 80)
    for key, value in record.to_dict().items():
        print(f"  {key}: {value}")
    
    print("\n" + "=" * 80)
    print("✅ Estrutura validada!")
    print("=" * 80)
    
    print("\n💡 Próximo passo: Implementar fetchers específicos em src/owners/fetchers/")
    print("   - beacon_fetcher.py (para condados com Beacon/Schneider)")
    print("   - vanguard_fetcher.py (para condados com Vanguard)")
    print("   - generic_fetcher.py (fallback com Google Custom Search)")
