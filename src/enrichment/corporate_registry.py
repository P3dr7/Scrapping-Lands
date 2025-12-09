"""
Corporate Registry Enrichment - Indiana Secretary of State (SOS)

Este módulo é responsável por:
1. Identificar se um proprietário é uma entidade corporativa (LLC, Corp, etc.)
2. Buscar informações no registro comercial de Indiana (INBiz/OpenCorporates)
3. Extrair dados do Registered Agent e Principals
4. Persistir dados na tabela companies

Fontes de dados (em ordem de prioridade):
1. OpenCorporates API (gratuita, até 500 requests/mês)
   - URL: https://api.opencorporates.com/v0.4/companies/search
   - Não requer autenticação para uso básico
   
2. Indiana INBiz (portal oficial, requer Playwright)
   - URL: https://inbiz.in.gov/BOS/BusinessSearch
   - Bloqueado para requests simples, requer browser

Autor: BellaTerra Intelligence
Data: 2025-12
"""

import re
import time
import random
import json
from typing import Optional, Dict, List, Any, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum

import requests
from bs4 import BeautifulSoup
from loguru import logger
from sqlalchemy import text
from sqlalchemy.orm import Session

# Tentar importar playwright para sites dinâmicos
try:
    from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False
    logger.warning("Playwright não disponível. Usando apenas requests/BeautifulSoup.")


# =============================================================================
# CONSTANTES E PADRÕES
# =============================================================================

# Sufixos corporativos comuns (regex patterns)
CORPORATE_SUFFIXES = [
    # LLCs
    r'\bLLC\b',
    r'\bL\.L\.C\.?\b',
    r'\bLimited\s+Liability\s+Company\b',
    
    # Corporations
    r'\bInc\.?\b',
    r'\bIncorporated\b',
    r'\bCorp\.?\b',
    r'\bCorporation\b',
    
    # Limited Companies
    r'\bLtd\.?\b',
    r'\bLimited\b',
    r'\bLC\b',
    r'\bL\.C\.?\b',
    
    # Partnerships
    r'\bLP\b',
    r'\bL\.P\.?\b',
    r'\bLLP\b',
    r'\bL\.L\.P\.?\b',
    r'\bLimited\s+Partnership\b',
    r'\bGeneral\s+Partnership\b',
    
    # Trusts
    r'\bTrust\b',
    r'\bLiving\s+Trust\b',
    r'\bFamily\s+Trust\b',
    r'\bRevocable\s+Trust\b',
    r'\bIrrevocable\s+Trust\b',
    
    # Holdings/Groups
    r'\bHoldings?\b',
    r'\bGroup\b',
    r'\bEnterprises?\b',
    r'\bProperties\b',
    r'\bInvestments?\b',
    r'\bVentures?\b',
    r'\bPartners\b',
    r'\bAssociates?\b',
    r'\bManagement\b',
    r'\bDevelopment\b',
    r'\bReal\s+Estate\b',
    
    # REITs
    r'\bREIT\b',
    r'\bR\.E\.I\.T\.?\b',
    
    # Outros
    r'\bCompany\b',
    r'\bCo\.?\b',
    r'\bPC\b',  # Professional Corporation
    r'\bPLLC\b',  # Professional LLC
    r'\bPLC\b',  # Public Limited Company
]

# Padrões que indicam pessoa física (para exclusão)
INDIVIDUAL_PATTERNS = [
    # Nomes com "ESTATE OF"
    r'^ESTATE\s+OF\b',
    r'\bESTATE$',
    
    # Nomes com formatos típicos de pessoa física
    # Ex: "SMITH JOHN A", "JOHN A SMITH", "JOHN SMITH JR"
    r'^[A-Z]+\s+[A-Z]+\s*[A-Z]?\.?\s*(JR\.?|SR\.?|II|III|IV)?$',
]

# Palavras que indicam fortemente entidade corporativa
STRONG_CORPORATE_INDICATORS = [
    'LLC', 'INC', 'CORP', 'LTD', 'TRUST', 'LP', 'LLP',
    'HOLDINGS', 'PROPERTIES', 'INVESTMENTS', 'ENTERPRISES',
    'MANAGEMENT', 'DEVELOPMENT', 'GROUP', 'PARTNERS', 'REIT'
]


class SOSLookupStatus(str, Enum):
    """Status da busca no SOS."""
    PENDING = 'pending'
    SUCCESS = 'success'
    NOT_FOUND = 'not_found'
    FAILED = 'failed'
    SKIPPED = 'skipped'  # Para nomes de pessoa física


@dataclass
class RegisteredAgent:
    """Dados do Registered Agent (quem recebe notificações legais)."""
    name: str
    address_line1: Optional[str] = None
    address_line2: Optional[str] = None
    city: Optional[str] = None
    state: Optional[str] = None
    zip_code: Optional[str] = None
    
    def full_address(self) -> str:
        """Retorna endereço completo formatado."""
        parts = [self.address_line1]
        if self.address_line2:
            parts.append(self.address_line2)
        if self.city or self.state or self.zip_code:
            city_state_zip = f"{self.city or ''}, {self.state or ''} {self.zip_code or ''}".strip()
            parts.append(city_state_zip)
        return '\n'.join(p for p in parts if p)


@dataclass
class Principal:
    """Dados de um principal/officer da empresa."""
    name: str
    title: Optional[str] = None  # President, Secretary, Member, etc.
    address: Optional[str] = None


@dataclass
class SOSBusinessRecord:
    """Registro de empresa encontrado no SOS."""
    # Identificação
    business_id: str  # ID do estado
    business_name: str
    entity_type: Optional[str] = None  # LLC, Corporation, etc.
    status: Optional[str] = None  # Active, Inactive, Dissolved
    
    # Datas
    formation_date: Optional[str] = None
    expiration_date: Optional[str] = None
    
    # Registered Agent
    registered_agent: Optional[RegisteredAgent] = None
    
    # Principal Office
    principal_office_address: Optional[str] = None
    
    # Officers/Members
    principals: List[Principal] = field(default_factory=list)
    
    # Metadata
    raw_data: Dict[str, Any] = field(default_factory=dict)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    
    def to_dict(self) -> Dict[str, Any]:
        """Converte para dicionário."""
        return {
            'business_id': self.business_id,
            'business_name': self.business_name,
            'entity_type': self.entity_type,
            'status': self.status,
            'formation_date': self.formation_date,
            'expiration_date': self.expiration_date,
            'registered_agent': {
                'name': self.registered_agent.name if self.registered_agent else None,
                'address': self.registered_agent.full_address() if self.registered_agent else None,
            },
            'principal_office': self.principal_office_address,
            'principals': [
                {'name': p.name, 'title': p.title, 'address': p.address}
                for p in self.principals
            ],
            'fetched_at': self.fetched_at.isoformat(),
        }


# =============================================================================
# FUNÇÕES DE IDENTIFICAÇÃO
# =============================================================================

def is_corporate_entity(name: str) -> bool:
    """
    Determina se um nome representa uma entidade corporativa vs. pessoa física.
    
    Args:
        name: Nome do proprietário a verificar
        
    Returns:
        True se parece ser entidade corporativa, False se parece pessoa física
        
    Examples:
        >>> is_corporate_entity("SMITH MOBILE HOME PARK LLC")
        True
        >>> is_corporate_entity("JOHN SMITH")
        False
        >>> is_corporate_entity("ABC HOLDINGS INC")
        True
        >>> is_corporate_entity("MARY JONES TRUST")
        True
    """
    if not name:
        return False
    
    # Normalizar para uppercase
    name_upper = name.upper().strip()
    
    # Verificar sufixos corporativos (mais confiável)
    for pattern in CORPORATE_SUFFIXES:
        if re.search(pattern, name_upper, re.IGNORECASE):
            return True
    
    # Verificar palavras fortes indicadoras
    for word in STRONG_CORPORATE_INDICATORS:
        if word in name_upper.split():
            return True
    
    # Verificar padrões de pessoa física (para exclusão)
    for pattern in INDIVIDUAL_PATTERNS:
        if re.search(pattern, name_upper):
            return False
    
    # Heurística: nomes curtos com 2-3 palavras provavelmente são pessoas
    words = name_upper.split()
    if len(words) <= 3:
        # Verificar se todas as palavras parecem nomes próprios
        # (não contêm números, são capitalizadas normalmente)
        if all(word.isalpha() and len(word) <= 15 for word in words):
            return False
    
    # Default: se tiver mais de 3 palavras ou caracteres especiais, 
    # provavelmente é empresa
    if len(words) > 3 or any(c.isdigit() for c in name):
        return True
    
    return False


def extract_entity_type(name: str) -> Optional[str]:
    """
    Extrai o tipo de entidade do nome.
    
    Args:
        name: Nome da empresa
        
    Returns:
        Tipo da entidade (LLC, Corporation, etc.) ou None
    """
    name_upper = name.upper()
    
    # Ordem de precedência (mais específico primeiro)
    type_patterns = [
        (r'\bPLLC\b', 'Professional LLC'),
        (r'\bLLC\b|\bL\.L\.C\.?\b', 'LLC'),
        (r'\bLLP\b|\bL\.L\.P\.?\b', 'LLP'),
        (r'\bLP\b|\bL\.P\.?\b', 'Limited Partnership'),
        (r'\bInc\.?\b|\bIncorporated\b', 'Corporation'),
        (r'\bCorp\.?\b|\bCorporation\b', 'Corporation'),
        (r'\bLtd\.?\b|\bLimited\b', 'Limited Company'),
        (r'\bTrust\b', 'Trust'),
        (r'\bREIT\b', 'REIT'),
        (r'\bPC\b', 'Professional Corporation'),
    ]
    
    for pattern, entity_type in type_patterns:
        if re.search(pattern, name_upper):
            return entity_type
    
    return None


# =============================================================================
# INDIANA SOS SEARCHER
# =============================================================================

class IndianaSOSSearcher:
    """
    Classe para buscar informações de empresas no Indiana Secretary of State.
    
    O Indiana usa o portal INBiz (https://inbiz.in.gov/) para registro de empresas.
    
    Estratégia de busca:
    1. Tentar endpoint de API direta se disponível
    2. Usar requests + BeautifulSoup para formulários tradicionais
    3. Fallback para Playwright se site for muito dinâmico
    """
    
    # URLs do INBiz
    BASE_URL = "https://inbiz.in.gov"
    SEARCH_URL = f"{BASE_URL}/BOS/BusinessSearch"
    
    # Headers para parecer um navegador real
    DEFAULT_HEADERS = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
    }
    
    def __init__(
        self,
        min_delay: float = 2.0,
        max_delay: float = 5.0,
        max_retries: int = 3,
        timeout: int = 30,
        use_playwright: bool = False,
        mock_mode: bool = False
    ):
        """
        Inicializa o searcher.
        
        Args:
            min_delay: Delay mínimo entre requests (segundos)
            max_delay: Delay máximo entre requests (segundos)
            max_retries: Número máximo de tentativas
            timeout: Timeout para requests (segundos)
            use_playwright: Forçar uso do Playwright
            mock_mode: Se True, retorna dados mock para testes
        """
        self.min_delay = min_delay
        self.max_delay = max_delay
        self.max_retries = max_retries
        self.timeout = timeout
        self.use_playwright = use_playwright and PLAYWRIGHT_AVAILABLE
        self.mock_mode = mock_mode
        
        # Session para manter cookies
        self.session = requests.Session()
        self.session.headers.update(self.DEFAULT_HEADERS)
        
        # Estatísticas
        self.stats = {
            'total_searches': 0,
            'successful': 0,
            'not_found': 0,
            'failed': 0,
            'rate_limited': 0,
        }
        
        logger.info(f"IndianaSOSSearcher inicializado")
        logger.info(f"  Min delay: {min_delay}s, Max delay: {max_delay}s")
        logger.info(f"  Playwright: {'Disponível' if PLAYWRIGHT_AVAILABLE else 'Não disponível'}")
        logger.info(f"  Mock Mode: {'Ativado' if mock_mode else 'Desativado'}")
    
    def _random_delay(self):
        """Aplica delay aleatório entre requests."""
        delay = random.uniform(self.min_delay, self.max_delay)
        logger.debug(f"Aguardando {delay:.2f}s...")
        time.sleep(delay)
    
    def _get_csrf_token(self, html: str) -> Optional[str]:
        """Extrai token CSRF do HTML se presente."""
        soup = BeautifulSoup(html, 'html.parser')
        
        # Tentar encontrar em diferentes formatos
        # __RequestVerificationToken (ASP.NET)
        token_input = soup.find('input', {'name': '__RequestVerificationToken'})
        if token_input:
            return token_input.get('value')
        
        # csrf_token genérico
        token_input = soup.find('input', {'name': 'csrf_token'})
        if token_input:
            return token_input.get('value')
        
        # Meta tag
        meta = soup.find('meta', {'name': 'csrf-token'})
        if meta:
            return meta.get('content')
        
        return None
    
    def _generate_mock_data(self, business_name: str) -> Optional[SOSBusinessRecord]:
        """
        Gera dados mock realistas para demonstração.
        
        Usado quando mock_mode=True para testar o pipeline sem
        acessar APIs externas.
        """
        import hashlib
        
        # Verificar se é uma entidade corporativa
        if not is_corporate_entity(business_name):
            return None
        
        # Gerar ID determinístico baseado no nome
        hash_val = int(hashlib.md5(business_name.encode()).hexdigest()[:8], 16)
        
        # Determinar tipo de entidade
        entity_type = extract_entity_type(business_name) or "Limited Liability Company"
        
        # Lista de nomes de agentes mock
        agents = [
            ("CT Corporation System", "150 W Market St Ste 800", "Indianapolis", "IN", "46204"),
            ("Registered Agents Inc.", "55 E Washington St Ste 1900", "Indianapolis", "IN", "46204"),
            ("Indiana Registered Agent LLC", "251 E Ohio St Ste 400", "Indianapolis", "IN", "46204"),
            ("Corporation Service Company", "135 N Pennsylvania St Ste 1100", "Indianapolis", "IN", "46204"),
            ("National Registered Agents Inc", "100 N Senate Ave", "Indianapolis", "IN", "46204"),
        ]
        
        # Lista de nomes de pessoas mock
        first_names = ["John", "Mary", "Robert", "Patricia", "Michael", "Jennifer", "William", "Linda", "David", "Elizabeth"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis", "Rodriguez", "Martinez"]
        titles = ["President", "CEO", "Managing Member", "Manager", "Secretary", "Treasurer", "Director"]
        
        # Selecionar dados baseado no hash
        agent_idx = hash_val % len(agents)
        agent_data = agents[agent_idx]
        
        # Gerar principals (1-3 pessoas)
        num_principals = (hash_val % 3) + 1
        principals = []
        for i in range(num_principals):
            first_name = first_names[(hash_val + i) % len(first_names)]
            last_name = last_names[(hash_val + i * 2) % len(last_names)]
            title = titles[(hash_val + i) % len(titles)]
            principals.append(Principal(
                name=f"{first_name} {last_name}",
                title=title,
                address=f"{100 + (hash_val % 900) + i * 100} Main St, Indianapolis, IN 46200"
            ))
        
        # Status (maioria ativa)
        statuses = ["Active", "Active", "Active", "Active", "Inactive", "Dissolved"]
        status = statuses[hash_val % len(statuses)]
        
        # Data de formação (entre 1990 e 2023)
        year = 1990 + (hash_val % 34)
        month = (hash_val % 12) + 1
        day = (hash_val % 28) + 1
        
        # Criar registro
        return SOSBusinessRecord(
            business_id=f"IN-{hash_val % 9000000 + 1000000}",
            business_name=business_name.upper(),
            entity_type=entity_type,
            status=status,
            formation_date=f"{year}-{month:02d}-{day:02d}",
            registered_agent=RegisteredAgent(
                name=agent_data[0],
                address_line1=agent_data[1],
                city=agent_data[2],
                state=agent_data[3],
                zip_code=agent_data[4],
            ),
            principals=principals,
            raw_data={'source': 'mock', 'generated_at': datetime.now(timezone.utc).isoformat()},
        )
    
    def search_business(self, business_name: str) -> Optional[SOSBusinessRecord]:
        """
        Busca empresa pelo nome no Indiana SOS.
        
        Ordem de tentativas:
        1. Mock mode (se ativado - para testes)
        2. OpenCorporates API (mais confiável)
        3. INBiz com Playwright (se disponível)
        4. INBiz com requests (fallback)
        
        Args:
            business_name: Nome exato da empresa
            
        Returns:
            SOSBusinessRecord se encontrada, None caso contrário
        """
        self.stats['total_searches'] += 1
        logger.info(f"Buscando empresa: {business_name}")
        
        # Aplicar delay (mesmo em mock para simular comportamento real)
        if not self.mock_mode:
            self._random_delay()
        
        # MOCK MODE - para testes e demonstração
        if self.mock_mode:
            result = self._generate_mock_data(business_name)
            if result:
                self.stats['successful'] += 1
                logger.info(f"✅ Empresa encontrada (MOCK): {result.business_name}")
                return result
            else:
                self.stats['not_found'] += 1
                logger.info(f"❌ Não é entidade corporativa (MOCK): {business_name}")
                return None
        
        # Tentar OpenCorporates primeiro (mais confiável)
        result = self._search_opencorporates(business_name)
        if result:
            self.stats['successful'] += 1
            logger.info(f"✅ Empresa encontrada (OpenCorporates): {result.business_name}")
            return result
        
        # Se OpenCorporates falhar, tentar INBiz
        for attempt in range(self.max_retries):
            try:
                if self.use_playwright and PLAYWRIGHT_AVAILABLE:
                    result = self._search_with_playwright(business_name)
                else:
                    result = self._search_with_requests(business_name)
                
                if result:
                    self.stats['successful'] += 1
                    logger.info(f"✅ Empresa encontrada: {result.business_name}")
                    return result
                else:
                    self.stats['not_found'] += 1
                    logger.info(f"❌ Empresa não encontrada: {business_name}")
                    return None
                    
            except requests.exceptions.HTTPError as e:
                if e.response.status_code == 429:
                    self.stats['rate_limited'] += 1
                    logger.warning(f"Rate limited. Aguardando {30 * (attempt + 1)}s...")
                    time.sleep(30 * (attempt + 1))
                    continue
                raise
                
            except Exception as e:
                logger.error(f"Erro na tentativa {attempt + 1}: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(5 * (attempt + 1))
                    continue
                raise
        
        self.stats['failed'] += 1
        logger.error(f"Falha após {self.max_retries} tentativas: {business_name}")
        return None
    
    def _search_opencorporates(self, business_name: str) -> Optional[SOSBusinessRecord]:
        """
        Busca empresa na OpenCorporates API.
        
        OpenCorporates é uma base de dados global de empresas que agrega
        informações de registros corporativos de vários países/estados.
        
        API gratuita com limite de 500 requests/mês sem autenticação.
        https://api.opencorporates.com/documentation/API-Reference
        
        Returns:
            SOSBusinessRecord se encontrada em Indiana, None caso contrário
        """
        OPENCORPORATES_URL = "https://api.opencorporates.com/v0.4/companies/search"
        
        try:
            # Limpar nome para busca - remover sufixos corporativos
            clean_name = re.sub(
                r'\b(LLC|L\.L\.C\.|INC|CORP|CORPORATION|LP|LLP|LIMITED)\b\.?',
                '',
                business_name.upper()
            ).strip()
            clean_name = re.sub(r'\s+', ' ', clean_name)  # Normalizar espaços
            
            # Buscar empresas em Indiana (US_IN)
            params = {
                'q': clean_name if clean_name else business_name,
                'jurisdiction_code': 'us_in',  # Indiana
                'per_page': 10,
            }
            
            logger.debug(f"Buscando no OpenCorporates: {params['q']}")
            
            response = self.session.get(
                OPENCORPORATES_URL,
                params=params,
                timeout=self.timeout,
                headers={'User-Agent': self.DEFAULT_HEADERS['User-Agent']}
            )
            
            if response.status_code == 404:
                logger.debug("OpenCorporates: Nenhum resultado")
                return None
                
            if response.status_code == 403 or response.status_code == 429:
                logger.warning("OpenCorporates: Rate limited ou bloqueado")
                return None
                
            response.raise_for_status()
            
            data = response.json()
            
            # Processar resultados
            results = data.get('results', {}).get('companies', [])
            
            if not results:
                logger.debug("OpenCorporates: Nenhuma empresa encontrada")
                return None
            
            # Encontrar melhor match
            best_match = None
            best_score = 0
            
            for item in results:
                company = item.get('company', {})
                company_name = company.get('name', '').upper()
                
                # Calcular score de similaridade simples
                if company_name == business_name.upper():
                    score = 100
                elif business_name.upper() in company_name:
                    score = 80
                elif company_name in business_name.upper():
                    score = 75
                elif clean_name and clean_name.upper() in company_name:
                    score = 60
                else:
                    # Palavras em comum
                    words1 = set(business_name.upper().split())
                    words2 = set(company_name.split())
                    common = len(words1 & words2)
                    score = common * 15
                
                if score > best_score:
                    best_score = score
                    best_match = company
            
            if not best_match or best_score < 40:
                logger.debug(f"OpenCorporates: Nenhum match bom (melhor score: {best_score})")
                return None
            
            logger.info(f"OpenCorporates: Match encontrado (score: {best_score})")
            
            # Criar RegisteredAgent se houver dados
            registered_agent = None
            if best_match.get('registered_address_in_full'):
                registered_agent = RegisteredAgent(
                    name=best_match.get('agent_name', ''),
                    address_line1=best_match.get('registered_address_in_full', '')
                )
            
            # Extrair dados do melhor match
            record = SOSBusinessRecord(
                business_id=best_match.get('company_number', ''),
                business_name=best_match.get('name', ''),
                status=best_match.get('current_status', 'Unknown'),
                entity_type=best_match.get('company_type', ''),
                formation_date=best_match.get('incorporation_date', ''),
                registered_agent=registered_agent,
                principals=[],  # OpenCorporates básico não inclui officers
                raw_data=best_match,
            )
            
            # Tentar buscar detalhes adicionais
            detail_url = best_match.get('opencorporates_url', '')
            if detail_url:
                record = self._enrich_from_opencorporates_detail(record, detail_url)
            
            return record
            
        except requests.exceptions.RequestException as e:
            logger.warning(f"OpenCorporates API error: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro ao processar OpenCorporates: {e}")
            return None
    
    def _enrich_from_opencorporates_detail(
        self, 
        record: SOSBusinessRecord, 
        detail_url: str
    ) -> SOSBusinessRecord:
        """
        Enriquece registro com dados adicionais do OpenCorporates.
        
        O endpoint de detalhe pode ter mais informações como officers.
        """
        try:
            # Converter URL da web para API
            api_url = detail_url.replace(
                'https://opencorporates.com', 
                'https://api.opencorporates.com/v0.4'
            )
            
            response = self.session.get(api_url, timeout=self.timeout)
            
            if response.status_code != 200:
                return record
            
            data = response.json()
            company = data.get('results', {}).get('company', {})
            
            # Officers/Directors
            officers = company.get('officers', [])
            if officers:
                principals = []
                for officer in officers[:10]:  # Limitar a 10
                    off = officer.get('officer', {})
                    principals.append(Principal(
                        name=off.get('name', ''),
                        title=off.get('position', ''),
                        address=off.get('address', ''),
                    ))
                record.principals = principals
            
            # Registered agent atualizado
            if company.get('registered_agent'):
                ra = company['registered_agent']
                if isinstance(ra, dict):
                    record.registered_agent = RegisteredAgent(
                        name=ra.get('name', ''),
                        address_line1=ra.get('address', '') or '',
                    )
            
            # Dados raw atualizados
            record.raw_data = company
            
            return record
            
        except Exception as e:
            logger.debug(f"Erro ao enriquecer de OpenCorporates: {e}")
            return record
    
    def _search_with_requests(self, business_name: str) -> Optional[SOSBusinessRecord]:
        """
        Busca usando requests + BeautifulSoup.
        
        O INBiz usa um formulário ASP.NET com ViewState.
        """
        try:
            # Primeiro, fazer GET para obter cookies e tokens
            logger.debug("Obtendo página de busca...")
            response = self.session.get(self.SEARCH_URL, timeout=self.timeout)
            response.raise_for_status()
            
            # Parse da página
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Verificar se é uma página ASP.NET WebForms
            viewstate = soup.find('input', {'name': '__VIEWSTATE'})
            viewstate_gen = soup.find('input', {'name': '__VIEWSTATEGENERATOR'})
            event_validation = soup.find('input', {'name': '__EVENTVALIDATION'})
            
            # Encontrar o campo de busca
            search_input = soup.find('input', {'id': lambda x: x and 'BusinessName' in x})
            if not search_input:
                search_input = soup.find('input', {'name': lambda x: x and 'BusinessName' in x})
            
            # Montar dados do POST
            post_data = {}
            
            if viewstate:
                post_data['__VIEWSTATE'] = viewstate.get('value', '')
            if viewstate_gen:
                post_data['__VIEWSTATEGENERATOR'] = viewstate_gen.get('value', '')
            if event_validation:
                post_data['__EVENTVALIDATION'] = event_validation.get('value', '')
            
            # Campo de busca (varia conforme o site)
            post_data['BusinessName'] = business_name
            post_data['SearchType'] = 'Contains'  # ou 'StartsWith', 'ExactMatch'
            
            # Botão de submit
            submit_btn = soup.find('input', {'type': 'submit', 'value': lambda x: x and 'Search' in str(x)})
            if submit_btn and submit_btn.get('name'):
                post_data[submit_btn['name']] = submit_btn['value']
            
            logger.debug(f"Enviando busca para: {business_name}")
            
            # POST da busca
            search_response = self.session.post(
                self.SEARCH_URL,
                data=post_data,
                timeout=self.timeout,
                headers={**self.DEFAULT_HEADERS, 'Referer': self.SEARCH_URL}
            )
            search_response.raise_for_status()
            
            # Parse dos resultados
            return self._parse_search_results(search_response.text, business_name)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Erro de request: {e}")
            # Se falhar com requests, tentar playwright
            if PLAYWRIGHT_AVAILABLE and not self.use_playwright:
                logger.info("Tentando com Playwright...")
                return self._search_with_playwright(business_name)
            return None
    
    def _search_with_playwright(self, business_name: str) -> Optional[SOSBusinessRecord]:
        """
        Busca usando Playwright para sites com JavaScript pesado.
        """
        if not PLAYWRIGHT_AVAILABLE:
            logger.error("Playwright não está disponível")
            return None
        
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=True)
                context = browser.new_context(
                    user_agent=self.DEFAULT_HEADERS['User-Agent']
                )
                page = context.new_page()
                
                # Navegar para página de busca
                logger.debug("Navegando para página de busca...")
                page.goto(self.SEARCH_URL, timeout=self.timeout * 1000)
                
                # Esperar carregar
                page.wait_for_load_state('networkidle')
                
                # Encontrar e preencher campo de busca
                search_input = page.locator('input[name*="BusinessName"], input[id*="BusinessName"]')
                if search_input.count() > 0:
                    search_input.first.fill(business_name)
                else:
                    # Tentar outros seletores
                    search_input = page.locator('input[type="text"]').first
                    search_input.fill(business_name)
                
                # Clicar no botão de busca
                search_btn = page.locator('input[type="submit"][value*="Search"], button:has-text("Search")')
                if search_btn.count() > 0:
                    search_btn.first.click()
                else:
                    # Submeter com Enter
                    search_input.press('Enter')
                
                # Esperar resultados
                page.wait_for_load_state('networkidle')
                time.sleep(2)  # Extra wait para conteúdo dinâmico
                
                # Obter HTML
                html = page.content()
                
                browser.close()
                
                return self._parse_search_results(html, business_name)
                
        except PlaywrightTimeout as e:
            logger.error(f"Timeout do Playwright: {e}")
            return None
        except Exception as e:
            logger.error(f"Erro do Playwright: {e}")
            return None
    
    def _parse_search_results(
        self,
        html: str,
        search_name: str
    ) -> Optional[SOSBusinessRecord]:
        """
        Parse dos resultados da busca.
        
        Esta função precisa ser adaptada ao HTML específico do INBiz.
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Salvar HTML para debug
        logger.debug(f"Parsing resultados (tamanho HTML: {len(html)} chars)")
        
        # Procurar tabela de resultados
        # O INBiz tipicamente usa tabelas ou grids para resultados
        results_table = soup.find('table', {'id': lambda x: x and 'result' in str(x).lower()})
        if not results_table:
            results_table = soup.find('table', {'class': lambda x: x and 'result' in str(x).lower()})
        if not results_table:
            # Tentar encontrar qualquer tabela com dados
            tables = soup.find_all('table')
            for table in tables:
                if table.find('td') and len(table.find_all('tr')) > 1:
                    results_table = table
                    break
        
        if not results_table:
            # Verificar se há mensagem de "não encontrado"
            no_results = soup.find(string=lambda x: x and ('no results' in x.lower() or 'not found' in x.lower()))
            if no_results:
                return None
            
            logger.warning("Não foi possível encontrar tabela de resultados")
            return None
        
        # Procurar linha que corresponde ao nome buscado
        rows = results_table.find_all('tr')
        
        for row in rows[1:]:  # Pular cabeçalho
            cells = row.find_all('td')
            if not cells:
                continue
            
            # Primeira célula geralmente tem o nome
            row_name = cells[0].get_text(strip=True)
            
            # Verificar se corresponde (busca flexível)
            if self._names_match(row_name, search_name):
                return self._extract_business_details(row, cells, soup, html)
        
        # Não encontrou correspondência exata
        return None
    
    def _names_match(self, name1: str, name2: str) -> bool:
        """Verifica se dois nomes de empresa correspondem."""
        # Normalizar
        n1 = re.sub(r'[^\w\s]', '', name1.upper())
        n2 = re.sub(r'[^\w\s]', '', name2.upper())
        
        # Match exato
        if n1 == n2:
            return True
        
        # Um contém o outro
        if n1 in n2 or n2 in n1:
            return True
        
        # Similaridade de palavras (80%+)
        words1 = set(n1.split())
        words2 = set(n2.split())
        
        if not words1 or not words2:
            return False
        
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        return (intersection / union) >= 0.8
    
    def _extract_business_details(
        self,
        row,
        cells: List,
        soup: BeautifulSoup,
        full_html: str
    ) -> SOSBusinessRecord:
        """
        Extrai detalhes completos da empresa.
        
        Pode precisar fazer request adicional para página de detalhes.
        """
        # Tentar extrair informações básicas da tabela
        business_name = cells[0].get_text(strip=True) if len(cells) > 0 else ""
        business_id = cells[1].get_text(strip=True) if len(cells) > 1 else ""
        entity_type = cells[2].get_text(strip=True) if len(cells) > 2 else ""
        status = cells[3].get_text(strip=True) if len(cells) > 3 else ""
        
        # Verificar se há link para detalhes
        detail_link = row.find('a', href=True)
        
        record = SOSBusinessRecord(
            business_id=business_id or f"IN-{hash(business_name) % 1000000:06d}",
            business_name=business_name,
            entity_type=entity_type,
            status=status,
        )
        
        # Se houver link de detalhes, buscar mais informações
        if detail_link:
            try:
                detail_url = detail_link['href']
                if not detail_url.startswith('http'):
                    detail_url = f"{self.BASE_URL}{detail_url}"
                
                self._random_delay()
                
                detail_response = self.session.get(detail_url, timeout=self.timeout)
                if detail_response.ok:
                    record = self._parse_detail_page(detail_response.text, record)
                    
            except Exception as e:
                logger.warning(f"Erro ao buscar detalhes: {e}")
        
        return record
    
    def _parse_detail_page(
        self,
        html: str,
        record: SOSBusinessRecord
    ) -> SOSBusinessRecord:
        """
        Parse da página de detalhes da empresa.
        
        Extrai Registered Agent, Principals, etc.
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Salvar HTML raw
        record.raw_data['detail_html_size'] = len(html)
        
        # Procurar seções comuns
        sections = {
            'registered_agent': ['Registered Agent', 'Agent', 'RA'],
            'principal_office': ['Principal Office', 'Principal Address', 'Business Address'],
            'officers': ['Officers', 'Principals', 'Members', 'Directors'],
            'formation': ['Formation Date', 'Date of Incorporation', 'Filing Date'],
        }
        
        # Buscar Registered Agent
        for label in sections['registered_agent']:
            agent_section = soup.find(string=lambda x: x and label.lower() in x.lower())
            if agent_section:
                agent_data = self._extract_agent_from_section(agent_section)
                if agent_data:
                    record.registered_agent = agent_data
                    break
        
        # Buscar Officers/Principals
        for label in sections['officers']:
            officers_section = soup.find(string=lambda x: x and label.lower() in x.lower())
            if officers_section:
                principals = self._extract_principals_from_section(officers_section)
                if principals:
                    record.principals = principals
                    break
        
        # Buscar Formation Date
        for label in sections['formation']:
            date_elem = soup.find(string=lambda x: x and label.lower() in x.lower())
            if date_elem:
                # Tentar extrair data do próximo elemento
                parent = date_elem.parent
                if parent:
                    sibling = parent.find_next_sibling()
                    if sibling:
                        date_text = sibling.get_text(strip=True)
                        # Extrair data com regex
                        date_match = re.search(r'\d{1,2}[/-]\d{1,2}[/-]\d{2,4}', date_text)
                        if date_match:
                            record.formation_date = date_match.group()
                            break
        
        return record
    
    def _extract_agent_from_section(self, section_text) -> Optional[RegisteredAgent]:
        """Extrai dados do Registered Agent de uma seção."""
        try:
            # Navegar para o container pai
            parent = section_text.parent
            if not parent:
                return None
            
            # Buscar próximo conteúdo (pode ser sibling ou child)
            container = parent.find_next(['div', 'td', 'p', 'dd'])
            if not container:
                return None
            
            # Obter texto completo
            full_text = container.get_text('\n', strip=True)
            lines = [l.strip() for l in full_text.split('\n') if l.strip()]
            
            if not lines:
                return None
            
            # Primeira linha geralmente é o nome
            agent_name = lines[0]
            
            # Parse do endereço
            agent = RegisteredAgent(name=agent_name)
            
            if len(lines) > 1:
                agent.address_line1 = lines[1]
            if len(lines) > 2:
                # Tentar parse de cidade, estado, CEP
                city_state_zip = lines[-1]
                match = re.match(r'(.+?),?\s*([A-Z]{2})\s*(\d{5}(?:-\d{4})?)?', city_state_zip)
                if match:
                    agent.city = match.group(1).strip().rstrip(',')
                    agent.state = match.group(2)
                    agent.zip_code = match.group(3)
                else:
                    agent.address_line2 = lines[2] if len(lines) > 2 else None
            
            return agent
            
        except Exception as e:
            logger.debug(f"Erro ao extrair agent: {e}")
            return None
    
    def _extract_principals_from_section(self, section_text) -> List[Principal]:
        """Extrai lista de principals/officers."""
        principals = []
        
        try:
            parent = section_text.parent
            if not parent:
                return principals
            
            # Buscar tabela ou lista de officers
            table = parent.find_next('table')
            if table:
                rows = table.find_all('tr')
                for row in rows:
                    cells = row.find_all(['td', 'th'])
                    if len(cells) >= 2:
                        name = cells[0].get_text(strip=True)
                        title = cells[1].get_text(strip=True) if len(cells) > 1 else None
                        
                        if name and not any(h in name.lower() for h in ['name', 'title', 'officer']):
                            principals.append(Principal(name=name, title=title))
            else:
                # Tentar formato de lista
                list_elem = parent.find_next(['ul', 'ol', 'dl'])
                if list_elem:
                    items = list_elem.find_all(['li', 'dd'])
                    for item in items:
                        text = item.get_text(strip=True)
                        if text:
                            # Tentar separar nome e título
                            parts = re.split(r'\s*[-–:]\s*', text, 1)
                            name = parts[0]
                            title = parts[1] if len(parts) > 1 else None
                            principals.append(Principal(name=name, title=title))
            
        except Exception as e:
            logger.debug(f"Erro ao extrair principals: {e}")
        
        return principals
    
    def get_stats(self) -> Dict[str, Any]:
        """Retorna estatísticas de uso."""
        return {
            **self.stats,
            'success_rate': (
                f"{100 * self.stats['successful'] / self.stats['total_searches']:.1f}%"
                if self.stats['total_searches'] > 0 else "N/A"
            )
        }


# =============================================================================
# CORPORATE ENRICHER
# =============================================================================

class CorporateEnricher:
    """
    Orquestrador de enriquecimento corporativo.
    
    1. Busca owners que são entidades corporativas
    2. Consulta Indiana SOS para cada um
    3. Salva dados em companies
    4. Atualiza referência em owners
    """
    
    def __init__(
        self,
        sos_searcher: Optional[IndianaSOSSearcher] = None,
        db_engine=None,
        batch_size: int = 10,
    ):
        """
        Inicializa o enricher.
        
        Args:
            sos_searcher: Instância do searcher (ou cria um novo)
            db_engine: Engine SQLAlchemy
            batch_size: Número de registros por batch
        """
        self.searcher = sos_searcher or IndianaSOSSearcher()
        self.batch_size = batch_size
        
        if db_engine is None:
            from ..database import get_engine
            self.engine = get_engine()
        else:
            self.engine = db_engine
        
        self.stats = {
            'total_processed': 0,
            'corporate_entities': 0,
            'individuals_skipped': 0,
            'sos_found': 0,
            'sos_not_found': 0,
            'sos_failed': 0,
            'companies_created': 0,
            'companies_updated': 0,
        }
        
        logger.info("CorporateEnricher inicializado")
    
    def process_pending_owners(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Processa owners pendentes de enriquecimento corporativo.
        
        Args:
            limit: Número máximo de owners a processar
            
        Returns:
            Estatísticas do processamento
        """
        logger.info("="*70)
        logger.info("INICIANDO ENRIQUECIMENTO CORPORATIVO")
        logger.info("="*70)
        
        from ..database import get_db_session
        
        with get_db_session() as session:
            # Buscar owners pendentes (entidades corporativas sem company_id)
            owners = self._get_pending_owners(session, limit)
            
            logger.info(f"Encontrados {len(owners)} owners para processar")
            
            for i, owner in enumerate(owners, 1):
                logger.info(f"\n[{i}/{len(owners)}] Processando: {owner['full_name']}")
                
                try:
                    self._process_single_owner(session, owner)
                    session.commit()
                    
                except Exception as e:
                    logger.error(f"Erro ao processar owner {owner['id']}: {e}")
                    session.rollback()
                    self._mark_owner_failed(session, owner['id'], str(e))
                    session.commit()
        
        self._print_summary()
        
        return self.stats
    
    def _get_pending_owners(
        self,
        session: Session,
        limit: Optional[int]
    ) -> List[Dict[str, Any]]:
        """Busca owners pendentes de enriquecimento."""
        query = """
            SELECT 
                id,
                full_name,
                is_individual,
                sos_lookup_status
            FROM owners
            WHERE company_id IS NULL
              AND (sos_lookup_status IS NULL OR sos_lookup_status = 'pending')
            ORDER BY id
        """
        
        if limit:
            query += f" LIMIT {limit}"
        
        result = session.execute(text(query))
        
        return [
            {
                'id': row[0],
                'full_name': row[1],
                'is_individual': row[2],
                'sos_lookup_status': row[3],
            }
            for row in result
        ]
    
    def _process_single_owner(self, session: Session, owner: Dict[str, Any]):
        """Processa um único owner."""
        self.stats['total_processed'] += 1
        
        owner_name = owner['full_name']
        
        # Verificar se é entidade corporativa
        if not is_corporate_entity(owner_name):
            logger.info(f"  → Identificado como pessoa física, pulando")
            self._mark_owner_skipped(session, owner['id'])
            self.stats['individuals_skipped'] += 1
            return
        
        self.stats['corporate_entities'] += 1
        logger.info(f"  → Entidade corporativa identificada")
        
        # Buscar no SOS
        entity_type = extract_entity_type(owner_name)
        logger.info(f"  → Tipo: {entity_type or 'Desconhecido'}")
        
        sos_record = self.searcher.search_business(owner_name)
        
        if sos_record:
            self.stats['sos_found'] += 1
            
            # Criar/atualizar company
            company_id = self._upsert_company(session, sos_record)
            
            # Linkar owner à company
            self._link_owner_to_company(session, owner['id'], company_id)
            
            # Marcar como sucesso
            self._mark_owner_success(session, owner['id'])
            
            logger.info(f"  ✅ Empresa vinculada: Company ID {company_id}")
            
        else:
            self.stats['sos_not_found'] += 1
            self._mark_owner_not_found(session, owner['id'])
            logger.info(f"  ❌ Empresa não encontrada no SOS")
    
    def _upsert_company(self, session: Session, sos_record: SOSBusinessRecord) -> int:
        """Cria ou atualiza registro de company."""
        # Verificar se já existe
        check_query = """
            SELECT id FROM companies
            WHERE state_registration = :business_id
              AND registration_state = 'IN'
        """
        
        existing = session.execute(text(check_query), {
            'business_id': sos_record.business_id
        }).fetchone()
        
        if existing:
            self.stats['companies_updated'] += 1
            return self._update_company(session, existing[0], sos_record)
        else:
            self.stats['companies_created'] += 1
            return self._create_company(session, sos_record)
    
    def _create_company(self, session: Session, sos_record: SOSBusinessRecord) -> int:
        """Cria novo registro de company."""
        # Preparar dados do registered agent
        agent = sos_record.registered_agent
        agent_data = {}
        if agent:
            agent_data = {
                'name': agent.name,
                'address_line1': agent.address_line1,
                'address_line2': agent.address_line2,
                'city': agent.city,
                'state': agent.state,
                'zip_code': agent.zip_code,
            }
        
        # Preparar dados dos principals
        principals_data = [
            {'name': p.name, 'title': p.title, 'address': p.address}
            for p in sos_record.principals
        ]
        
        query = """
            INSERT INTO companies (
                legal_name,
                company_type,
                entity_type,
                state_registration,
                registration_state,
                registered_agent_name,
                registered_agent_address,
                principals,
                sos_status,
                sos_formation_date,
                sos_raw_data,
                source,
                source_reference,
                created_at,
                updated_at
            ) VALUES (
                :legal_name,
                :company_type,
                :entity_type,
                :state_registration,
                'IN',
                :registered_agent_name,
                :registered_agent_address,
                :principals,
                :sos_status,
                :sos_formation_date,
                :sos_raw_data,
                'indiana_sos',
                :source_reference,
                NOW(),
                NOW()
            )
            RETURNING id
        """
        
        result = session.execute(text(query), {
            'legal_name': sos_record.business_name,
            'company_type': sos_record.entity_type,
            'entity_type': 'private' if sos_record.entity_type != 'REIT' else 'public',
            'state_registration': sos_record.business_id,
            'registered_agent_name': agent.name if agent else None,
            'registered_agent_address': json.dumps(agent_data) if agent_data else None,
            'principals': json.dumps(principals_data) if principals_data else None,
            'sos_status': sos_record.status,
            'sos_formation_date': sos_record.formation_date,
            'sos_raw_data': json.dumps(sos_record.to_dict()),
            'source_reference': f"INBiz Business ID: {sos_record.business_id}",
        })
        
        return result.fetchone()[0]
    
    def _update_company(self, session: Session, company_id: int, sos_record: SOSBusinessRecord) -> int:
        """Atualiza registro existente de company."""
        agent = sos_record.registered_agent
        agent_data = {}
        if agent:
            agent_data = {
                'name': agent.name,
                'address_line1': agent.address_line1,
                'city': agent.city,
                'state': agent.state,
                'zip_code': agent.zip_code,
            }
        
        principals_data = [
            {'name': p.name, 'title': p.title}
            for p in sos_record.principals
        ]
        
        query = """
            UPDATE companies SET
                registered_agent_name = :registered_agent_name,
                registered_agent_address = :registered_agent_address,
                principals = :principals,
                sos_status = :sos_status,
                sos_raw_data = :sos_raw_data,
                updated_at = NOW(),
                last_verified_at = NOW()
            WHERE id = :company_id
        """
        
        session.execute(text(query), {
            'company_id': company_id,
            'registered_agent_name': agent.name if agent else None,
            'registered_agent_address': json.dumps(agent_data) if agent_data else None,
            'principals': json.dumps(principals_data) if principals_data else None,
            'sos_status': sos_record.status,
            'sos_raw_data': json.dumps(sos_record.to_dict()),
        })
        
        return company_id
    
    def _link_owner_to_company(self, session: Session, owner_id: int, company_id: int):
        """Vincula owner a uma company."""
        query = """
            UPDATE owners SET
                company_id = :company_id,
                updated_at = NOW()
            WHERE id = :owner_id
        """
        session.execute(text(query), {
            'owner_id': owner_id,
            'company_id': company_id,
        })
    
    def _mark_owner_success(self, session: Session, owner_id: int):
        """Marca owner como processado com sucesso."""
        query = """
            UPDATE owners SET
                sos_lookup_status = 'success',
                updated_at = NOW()
            WHERE id = :owner_id
        """
        session.execute(text(query), {'owner_id': owner_id})
    
    def _mark_owner_not_found(self, session: Session, owner_id: int):
        """Marca owner como não encontrado no SOS."""
        query = """
            UPDATE owners SET
                sos_lookup_status = 'not_found',
                updated_at = NOW()
            WHERE id = :owner_id
        """
        session.execute(text(query), {'owner_id': owner_id})
    
    def _mark_owner_skipped(self, session: Session, owner_id: int):
        """Marca owner como pulado (pessoa física)."""
        query = """
            UPDATE owners SET
                sos_lookup_status = 'skipped',
                updated_at = NOW()
            WHERE id = :owner_id
        """
        session.execute(text(query), {'owner_id': owner_id})
    
    def _mark_owner_failed(self, session: Session, owner_id: int, error: str):
        """Marca owner como falhou."""
        query = """
            UPDATE owners SET
                sos_lookup_status = 'failed',
                updated_at = NOW()
            WHERE id = :owner_id
        """
        session.execute(text(query), {'owner_id': owner_id})
        self.stats['sos_failed'] += 1
    
    def _print_summary(self):
        """Imprime resumo do processamento."""
        logger.info("\n" + "="*70)
        logger.info("RESUMO DO ENRIQUECIMENTO CORPORATIVO")
        logger.info("="*70)
        logger.info(f"Total processados: {self.stats['total_processed']}")
        logger.info(f"  Entidades corporativas: {self.stats['corporate_entities']}")
        logger.info(f"  Pessoas físicas (puladas): {self.stats['individuals_skipped']}")
        logger.info(f"")
        logger.info(f"Resultados SOS:")
        logger.info(f"  Encontradas: {self.stats['sos_found']}")
        logger.info(f"  Não encontradas: {self.stats['sos_not_found']}")
        logger.info(f"  Falhas: {self.stats['sos_failed']}")
        logger.info(f"")
        logger.info(f"Companies:")
        logger.info(f"  Criadas: {self.stats['companies_created']}")
        logger.info(f"  Atualizadas: {self.stats['companies_updated']}")
        logger.info("="*70)
        
        # Estatísticas do SOS Searcher
        sos_stats = self.searcher.get_stats()
        logger.info(f"\nEstatísticas do SOS Searcher:")
        logger.info(f"  Total buscas: {sos_stats['total_searches']}")
        logger.info(f"  Taxa sucesso: {sos_stats['success_rate']}")
        logger.info(f"  Rate limited: {sos_stats['rate_limited']}")


# =============================================================================
# MOCK SEARCHER (para testes)
# =============================================================================

class MockSOSSearcher(IndianaSOSSearcher):
    """
    Mock searcher para testes sem acessar o site real.
    """
    
    def __init__(self, success_rate: float = 0.7, **kwargs):
        super().__init__(**kwargs)
        self.success_rate = success_rate
        self._mock_database = {}
    
    def search_business(self, business_name: str) -> Optional[SOSBusinessRecord]:
        """Retorna dados mock."""
        self.stats['total_searches'] += 1
        self._random_delay()
        
        # Simular taxa de sucesso
        if random.random() > self.success_rate:
            self.stats['not_found'] += 1
            return None
        
        self.stats['successful'] += 1
        
        # Gerar dados mock
        entity_type = extract_entity_type(business_name) or 'LLC'
        business_id = f"IN-{abs(hash(business_name)) % 10000000:07d}"
        
        return SOSBusinessRecord(
            business_id=business_id,
            business_name=business_name.upper(),
            entity_type=entity_type,
            status='Active',
            formation_date='01/15/2010',
            registered_agent=RegisteredAgent(
                name=f"CORPORATE AGENT SERVICES INC",
                address_line1="123 CORPORATE PLAZA STE 500",
                city="INDIANAPOLIS",
                state="IN",
                zip_code="46204",
            ),
            principals=[
                Principal(name="JOHN DOE", title="President"),
                Principal(name="JANE SMITH", title="Secretary"),
            ],
        )


# =============================================================================
# FUNÇÕES UTILITÁRIAS
# =============================================================================

def test_entity_detection():
    """Testa a detecção de entidades corporativas."""
    test_cases = [
        # (nome, esperado_corporativo)
        ("SMITH MOBILE HOME PARK LLC", True),
        ("ABC HOLDINGS INC", True),
        ("JOHN SMITH", False),
        ("MARY JONES TRUST", True),
        ("GOLDEN PROPERTIES LLC", True),
        ("J & M INVESTMENTS", True),
        ("ESTATE OF JOHN DOE", False),
        ("ROBERT JOHNSON JR", False),
        ("MIDWEST MANUFACTURED HOUSING CORP", True),
        ("SUNSHINE RV PARK LP", True),
        ("THOMAS FAMILY TRUST", True),
        ("WILLIAMS ENTERPRISES LLC", True),
        ("BOB MILLER", False),
    ]
    
    print("\n" + "="*60)
    print("TESTE DE DETECÇÃO DE ENTIDADES")
    print("="*60)
    
    passed = 0
    failed = 0
    
    for name, expected in test_cases:
        result = is_corporate_entity(name)
        status = "PASS" if result == expected else "FAIL"
        
        if result == expected:
            passed += 1
        else:
            failed += 1
        
        entity_type = extract_entity_type(name) if result else "N/A"
        print(f"  {status}: {name[:40]:40} -> {result} (tipo: {entity_type})")
    
    print(f"\nResultado: {passed}/{len(test_cases)} passaram")
    return failed == 0


if __name__ == "__main__":
    # Executar testes
    test_entity_detection()
