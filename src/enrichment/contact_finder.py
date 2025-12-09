"""
Contact Finder - Fase 5: Enriquecimento de Contatos Digitais

Este módulo implementa duas estratégias para encontrar contatos:

1. WebsiteContactScraper: Scraping ético de websites de parques
   - Respeita robots.txt
   - Extrai emails e telefones de páginas públicas
   - Timeouts curtos para não travar em sites lentos

2. ContactEnrichmentService: Interface para APIs comerciais
   - HunterIoService: Busca emails via Hunter.io
   - ApolloService: Busca via Apollo.io
   - Fail-safe: funciona mesmo sem API keys

Autor: BellaTerra Intelligence
Data: 2025-12
"""

import re
import os
import time
import json
import random
from abc import ABC, abstractmethod
from typing import Optional, List, Dict, Any, Tuple, Set
from dataclasses import dataclass, field
from datetime import datetime, timezone
from urllib.parse import urljoin, urlparse
from enum import Enum

import requests
from bs4 import BeautifulSoup
from loguru import logger


# =============================================================================
# CONFIGURAÇÃO
# =============================================================================

# Timeouts curtos para não travar
DEFAULT_TIMEOUT = 5  # segundos
ROBOTS_TIMEOUT = 3   # segundos para robots.txt

# Rate limiting
MIN_DELAY = 1.0  # segundos entre requests
MAX_DELAY = 3.0

# User-Agent para parecer um navegador real
DEFAULT_USER_AGENT = (
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
    'AppleWebKit/537.36 (KHTML, like Gecko) '
    'Chrome/120.0.0.0 Safari/537.36'
)


# =============================================================================
# REGEX PATTERNS
# =============================================================================

# Padrão de email (RFC 5322 simplificado)
EMAIL_PATTERN = re.compile(
    r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}',
    re.IGNORECASE
)

# Emails a ignorar (genéricos, spam traps, etc.)
EMAIL_BLACKLIST = {
    'example.com', 'test.com', 'email.com', 'domain.com',
    'yoursite.com', 'yourdomain.com', 'company.com',
    'noreply', 'no-reply', 'donotreply', 'do-not-reply',
    'mailer-daemon', 'postmaster', 'webmaster',
    'sentry.io', 'wixpress.com', 'sentry.wixpress.com',
}

# Padrões de telefone dos EUA
PHONE_PATTERNS = [
    # (XXX) XXX-XXXX
    re.compile(r'\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'),
    # XXX-XXX-XXXX
    re.compile(r'\d{3}[-.\s]\d{3}[-.\s]\d{4}'),
    # 1-XXX-XXX-XXXX
    re.compile(r'1?[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'),
    # +1 XXX XXX XXXX
    re.compile(r'\+1[-.\s]?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}'),
]


# =============================================================================
# DATA CLASSES
# =============================================================================

class ContactType(Enum):
    """Tipos de contato."""
    PARK_OFFICE = 'park_office'
    REGISTERED_AGENT = 'registered_agent'
    PRINCIPAL = 'principal'
    CORPORATE = 'corporate'
    PERSONAL = 'personal'
    GENERAL = 'general'


class ContactSource(Enum):
    """Fontes de contato."""
    WEBSITE_SCRAPE = 'website_scrape'
    GOOGLE_PLACES = 'google_places'
    HUNTER_IO = 'hunter_io'
    APOLLO = 'apollo'
    MANUAL = 'manual'
    INBIZ = 'inbiz'


@dataclass
class ExtractedContact:
    """Contato extraído de uma fonte."""
    email: Optional[str] = None
    phone: Optional[str] = None
    phone_type: Optional[str] = None
    person_name: Optional[str] = None
    person_title: Optional[str] = None
    contact_type: ContactType = ContactType.GENERAL
    source: ContactSource = ContactSource.WEBSITE_SCRAPE
    source_url: Optional[str] = None
    confidence: float = 0.5
    raw_data: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ScrapeResult:
    """Resultado de scraping de um website."""
    url: str
    success: bool
    status_code: Optional[int] = None
    response_time_ms: Optional[int] = None
    robots_allowed: Optional[bool] = None
    contacts: List[ExtractedContact] = field(default_factory=list)
    error_message: Optional[str] = None
    pages_scraped: int = 0


# =============================================================================
# WEBSITE CONTACT SCRAPER
# =============================================================================

class WebsiteContactScraper:
    """
    Scraper ético para extrair contatos de websites de parques.
    
    Características:
    - Respeita robots.txt
    - Timeouts curtos (5 segundos)
    - Rate limiting entre requests
    - Extrai emails e telefones via regex
    """
    
    def __init__(
        self,
        timeout: int = DEFAULT_TIMEOUT,
        respect_robots: bool = True,
        min_delay: float = MIN_DELAY,
        max_delay: float = MAX_DELAY,
        user_agent: str = DEFAULT_USER_AGENT
    ):
        """
        Inicializa o scraper.
        
        Args:
            timeout: Timeout em segundos para requests
            respect_robots: Se deve respeitar robots.txt
            min_delay: Delay mínimo entre requests
            max_delay: Delay máximo entre requests
            user_agent: User-Agent para requests
        """
        self.timeout = timeout
        self.respect_robots = respect_robots
        self.min_delay = min_delay
        self.max_delay = max_delay
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': user_agent,
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'Connection': 'keep-alive',
        })
        
        # Cache de robots.txt
        self._robots_cache: Dict[str, bool] = {}
        
        # Estatísticas
        self.stats = {
            'sites_scraped': 0,
            'successful': 0,
            'failed': 0,
            'blocked_by_robots': 0,
            'timeouts': 0,
            'emails_found': 0,
            'phones_found': 0,
        }
        
        logger.info(f"WebsiteContactScraper inicializado (timeout={timeout}s)")
    
    def _random_delay(self):
        """Aplica delay aleatório entre requests."""
        delay = random.uniform(self.min_delay, self.max_delay)
        time.sleep(delay)
    
    def _normalize_url(self, url: str) -> str:
        """Normaliza URL adicionando scheme se necessário."""
        if not url:
            return ''
        
        url = url.strip()
        
        if not url.startswith(('http://', 'https://')):
            url = 'https://' + url
        
        return url
    
    def _get_domain(self, url: str) -> str:
        """Extrai domínio de uma URL."""
        try:
            parsed = urlparse(url)
            return f"{parsed.scheme}://{parsed.netloc}"
        except Exception:
            return url
    
    def _check_robots_txt(self, url: str) -> bool:
        """
        Verifica se scraping é permitido pelo robots.txt.
        
        Args:
            url: URL a verificar
            
        Returns:
            True se permitido, False se bloqueado
        """
        if not self.respect_robots:
            return True
        
        domain = self._get_domain(url)
        
        # Verificar cache
        if domain in self._robots_cache:
            return self._robots_cache[domain]
        
        robots_url = f"{domain}/robots.txt"
        
        try:
            response = self.session.get(
                robots_url,
                timeout=ROBOTS_TIMEOUT,
                allow_redirects=True
            )
            
            if response.status_code == 404:
                # Sem robots.txt = permitido
                self._robots_cache[domain] = True
                return True
            
            if response.status_code != 200:
                # Erro ao acessar = assumir permitido
                self._robots_cache[domain] = True
                return True
            
            content = response.text.lower()
            
            # Verificação simplificada
            # Procura por "user-agent: *" seguido de "disallow: /"
            if 'user-agent: *' in content:
                # Verifica se há bloqueio geral
                lines = content.split('\n')
                in_star_block = False
                
                for line in lines:
                    line = line.strip()
                    if line.startswith('user-agent:'):
                        in_star_block = '*' in line
                    elif in_star_block and line.startswith('disallow:'):
                        path = line.replace('disallow:', '').strip()
                        if path == '/' or path == '/*':
                            self._robots_cache[domain] = False
                            logger.debug(f"robots.txt bloqueia scraping: {domain}")
                            return False
            
            self._robots_cache[domain] = True
            return True
            
        except requests.exceptions.Timeout:
            # Timeout = assumir permitido
            self._robots_cache[domain] = True
            return True
        except Exception as e:
            logger.debug(f"Erro ao verificar robots.txt: {e}")
            self._robots_cache[domain] = True
            return True
    
    def _extract_emails(self, text: str, url: str) -> List[str]:
        """
        Extrai emails de texto.
        
        Args:
            text: Texto para extrair emails
            url: URL de origem (para filtrar emails do próprio domínio)
            
        Returns:
            Lista de emails únicos encontrados
        """
        emails = set()
        
        # Encontrar todos os matches
        matches = EMAIL_PATTERN.findall(text)
        
        for email in matches:
            email = email.lower().strip()
            
            # Filtrar emails inválidos
            if len(email) < 6 or len(email) > 254:
                continue
            
            # Filtrar blacklist
            domain = email.split('@')[1] if '@' in email else ''
            local = email.split('@')[0] if '@' in email else email
            
            if any(bl in domain for bl in EMAIL_BLACKLIST):
                continue
            if any(bl in local for bl in EMAIL_BLACKLIST):
                continue
            
            # Filtrar extensões de arquivo confundidas com emails
            if email.endswith(('.png', '.jpg', '.gif', '.css', '.js', '.php')):
                continue
            
            emails.add(email)
        
        return list(emails)
    
    def _extract_phones(self, text: str) -> List[Tuple[str, str]]:
        """
        Extrai telefones de texto.
        
        Args:
            text: Texto para extrair telefones
            
        Returns:
            Lista de tuplas (telefone_normalizado, tipo)
        """
        phones = set()
        
        for pattern in PHONE_PATTERNS:
            matches = pattern.findall(text)
            for match in matches:
                # Normalizar: remover tudo exceto dígitos
                digits = re.sub(r'\D', '', match)
                
                # Validar comprimento (10-11 dígitos para EUA)
                if len(digits) < 10 or len(digits) > 11:
                    continue
                
                # Remover código de país 1 se presente
                if len(digits) == 11 and digits.startswith('1'):
                    digits = digits[1:]
                
                # Formatar como (XXX) XXX-XXXX
                if len(digits) == 10:
                    formatted = f"({digits[:3]}) {digits[3:6]}-{digits[6:]}"
                    phones.add(formatted)
        
        # Determinar tipo (heurística simples)
        result = []
        for phone in phones:
            # Por padrão, assumir office
            phone_type = 'office'
            result.append((phone, phone_type))
        
        return result
    
    def _find_contact_page(self, soup: BeautifulSoup, base_url: str) -> Optional[str]:
        """
        Encontra link para página de contato.
        
        Args:
            soup: BeautifulSoup da página principal
            base_url: URL base para resolver links relativos
            
        Returns:
            URL da página de contato ou None
        """
        contact_keywords = [
            'contact', 'contato', 'contact-us', 'contact_us',
            'contacto', 'reach-us', 'get-in-touch', 'about',
            'about-us', 'info', 'information'
        ]
        
        for link in soup.find_all('a', href=True):
            href = link.get('href', '').lower()
            text = link.get_text().lower()
            
            for keyword in contact_keywords:
                if keyword in href or keyword in text:
                    full_url = urljoin(base_url, link['href'])
                    # Verificar se é do mesmo domínio
                    if urlparse(full_url).netloc == urlparse(base_url).netloc:
                        return full_url
        
        return None
    
    def _scrape_page(self, url: str) -> Tuple[Optional[str], Optional[int], Optional[int]]:
        """
        Faz scraping de uma página.
        
        Args:
            url: URL para scraping
            
        Returns:
            Tupla (html_content, status_code, response_time_ms)
        """
        try:
            start_time = time.time()
            response = self.session.get(url, timeout=self.timeout, allow_redirects=True)
            response_time = int((time.time() - start_time) * 1000)
            
            if response.status_code != 200:
                return None, response.status_code, response_time
            
            return response.text, response.status_code, response_time
            
        except requests.exceptions.Timeout:
            self.stats['timeouts'] += 1
            raise TimeoutError(f"Timeout ao acessar {url}")
        except requests.exceptions.RequestException as e:
            raise ConnectionError(f"Erro de conexão: {e}")
    
    def scrape_website(self, url: str, park_name: Optional[str] = None) -> ScrapeResult:
        """
        Faz scraping de um website para extrair contatos.
        
        Args:
            url: URL do website
            park_name: Nome do parque (para logging)
            
        Returns:
            ScrapeResult com contatos encontrados
        """
        self.stats['sites_scraped'] += 1
        
        url = self._normalize_url(url)
        if not url:
            return ScrapeResult(
                url=url,
                success=False,
                error_message="URL inválida"
            )
        
        logger.info(f"Scraping: {url}" + (f" ({park_name})" if park_name else ""))
        
        # Verificar robots.txt
        robots_allowed = self._check_robots_txt(url)
        if not robots_allowed:
            self.stats['blocked_by_robots'] += 1
            return ScrapeResult(
                url=url,
                success=False,
                robots_allowed=False,
                error_message="Bloqueado por robots.txt"
            )
        
        contacts: List[ExtractedContact] = []
        all_emails: Set[str] = set()
        all_phones: Set[str] = set()
        pages_scraped = 0
        last_status_code = None
        last_response_time = None
        
        try:
            # Delay antes do request
            self._random_delay()
            
            # 1. Scrape da página principal
            html, status_code, response_time = self._scrape_page(url)
            last_status_code = status_code
            last_response_time = response_time
            
            if not html:
                self.stats['failed'] += 1
                return ScrapeResult(
                    url=url,
                    success=False,
                    status_code=status_code,
                    response_time_ms=response_time,
                    robots_allowed=True,
                    error_message=f"HTTP {status_code}"
                )
            
            pages_scraped += 1
            soup = BeautifulSoup(html, 'html.parser')
            
            # Extrair texto visível
            # Remover scripts e styles
            for tag in soup(['script', 'style', 'noscript', 'iframe']):
                tag.decompose()
            
            text = soup.get_text(separator=' ')
            
            # Extrair emails e phones da página principal
            emails = self._extract_emails(text, url)
            phones = self._extract_phones(text)
            
            all_emails.update(emails)
            for phone, _ in phones:
                all_phones.add(phone)
            
            # 2. Tentar página de contato
            contact_url = self._find_contact_page(soup, url)
            if contact_url and contact_url != url:
                try:
                    self._random_delay()
                    contact_html, _, _ = self._scrape_page(contact_url)
                    
                    if contact_html:
                        pages_scraped += 1
                        contact_soup = BeautifulSoup(contact_html, 'html.parser')
                        
                        for tag in contact_soup(['script', 'style', 'noscript', 'iframe']):
                            tag.decompose()
                        
                        contact_text = contact_soup.get_text(separator=' ')
                        
                        contact_emails = self._extract_emails(contact_text, contact_url)
                        contact_phones = self._extract_phones(contact_text)
                        
                        all_emails.update(contact_emails)
                        for phone, _ in contact_phones:
                            all_phones.add(phone)
                            
                except Exception as e:
                    logger.debug(f"Erro ao scrape página de contato: {e}")
            
            # Criar ExtractedContact para cada email/phone
            for email in all_emails:
                contacts.append(ExtractedContact(
                    email=email,
                    contact_type=ContactType.PARK_OFFICE,
                    source=ContactSource.WEBSITE_SCRAPE,
                    source_url=url,
                    confidence=0.7,  # Website próprio = boa confiança
                ))
            
            for phone in all_phones:
                # Verificar se já existe contato com esse email que pode receber o phone
                matched = False
                for contact in contacts:
                    if contact.phone is None and contact.email:
                        contact.phone = phone
                        contact.phone_type = 'office'
                        matched = True
                        break
                
                if not matched:
                    contacts.append(ExtractedContact(
                        phone=phone,
                        phone_type='office',
                        contact_type=ContactType.PARK_OFFICE,
                        source=ContactSource.WEBSITE_SCRAPE,
                        source_url=url,
                        confidence=0.7,
                    ))
            
            self.stats['successful'] += 1
            self.stats['emails_found'] += len(all_emails)
            self.stats['phones_found'] += len(all_phones)
            
            logger.info(f"  -> Encontrados: {len(all_emails)} emails, {len(all_phones)} telefones")
            
            return ScrapeResult(
                url=url,
                success=True,
                status_code=last_status_code,
                response_time_ms=last_response_time,
                robots_allowed=True,
                contacts=contacts,
                pages_scraped=pages_scraped,
            )
            
        except TimeoutError as e:
            self.stats['failed'] += 1
            return ScrapeResult(
                url=url,
                success=False,
                robots_allowed=True,
                error_message=str(e),
            )
        except ConnectionError as e:
            self.stats['failed'] += 1
            return ScrapeResult(
                url=url,
                success=False,
                robots_allowed=True,
                error_message=str(e),
            )
        except Exception as e:
            self.stats['failed'] += 1
            logger.error(f"Erro inesperado ao scrape {url}: {e}")
            return ScrapeResult(
                url=url,
                success=False,
                robots_allowed=True,
                error_message=str(e),
            )


# =============================================================================
# CONTACT ENRICHMENT SERVICE (ABSTRACT)
# =============================================================================

class ContactEnrichmentService(ABC):
    """
    Interface abstrata para serviços de enriquecimento de contatos.
    
    Implementações:
    - HunterIoService: Busca emails via Hunter.io API
    - ApolloService: Busca via Apollo.io API
    """
    
    @property
    @abstractmethod
    def service_name(self) -> str:
        """Nome do serviço."""
        pass
    
    @property
    @abstractmethod
    def is_configured(self) -> bool:
        """Verifica se o serviço está configurado (tem API key)."""
        pass
    
    @abstractmethod
    def find_email(
        self,
        company_name: str,
        person_name: Optional[str] = None,
        domain: Optional[str] = None
    ) -> Optional[ExtractedContact]:
        """
        Busca email para uma pessoa/empresa.
        
        Args:
            company_name: Nome da empresa
            person_name: Nome da pessoa (opcional)
            domain: Domínio da empresa (opcional)
            
        Returns:
            ExtractedContact se encontrado, None caso contrário
        """
        pass
    
    @abstractmethod
    def find_company_contacts(
        self,
        company_name: str,
        limit: int = 5
    ) -> List[ExtractedContact]:
        """
        Busca múltiplos contatos de uma empresa.
        
        Args:
            company_name: Nome da empresa
            limit: Número máximo de contatos
            
        Returns:
            Lista de ExtractedContact
        """
        pass


# =============================================================================
# HUNTER.IO SERVICE
# =============================================================================

class HunterIoService(ContactEnrichmentService):
    """
    Serviço de enriquecimento usando Hunter.io API.
    
    Hunter.io oferece:
    - Domain Search: encontra emails de um domínio
    - Email Finder: encontra email de pessoa específica
    - Email Verifier: verifica se email é válido
    
    Plano gratuito: 25 buscas/mês
    Docs: https://hunter.io/api-documentation
    """
    
    API_BASE = "https://api.hunter.io/v2"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inicializa o serviço.
        
        Args:
            api_key: API key do Hunter.io (ou usa HUNTER_API_KEY do ambiente)
        """
        self.api_key = api_key or os.environ.get('HUNTER_API_KEY')
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': DEFAULT_USER_AGENT,
        })
        
        self.stats = {
            'requests': 0,
            'successful': 0,
            'failed': 0,
            'emails_found': 0,
        }
        
        if self.is_configured:
            logger.info("HunterIoService configurado com API key")
        else:
            logger.warning("HunterIoService sem API key - funcionalidade desabilitada")
    
    @property
    def service_name(self) -> str:
        return "Hunter.io"
    
    @property
    def is_configured(self) -> bool:
        return bool(self.api_key)
    
    def _make_request(self, endpoint: str, params: Dict[str, Any]) -> Optional[Dict]:
        """Faz request à API."""
        if not self.is_configured:
            return None
        
        params['api_key'] = self.api_key
        url = f"{self.API_BASE}/{endpoint}"
        
        self.stats['requests'] += 1
        
        try:
            response = self.session.get(url, params=params, timeout=10)
            
            if response.status_code == 401:
                logger.error("Hunter.io: API key inválida")
                return None
            
            if response.status_code == 429:
                logger.warning("Hunter.io: Rate limit atingido")
                return None
            
            if response.status_code != 200:
                logger.warning(f"Hunter.io: HTTP {response.status_code}")
                return None
            
            data = response.json()
            self.stats['successful'] += 1
            return data.get('data')
            
        except Exception as e:
            self.stats['failed'] += 1
            logger.error(f"Hunter.io error: {e}")
            return None
    
    def find_email(
        self,
        company_name: str,
        person_name: Optional[str] = None,
        domain: Optional[str] = None
    ) -> Optional[ExtractedContact]:
        """
        Busca email para uma pessoa/empresa usando Email Finder.
        """
        if not self.is_configured:
            logger.debug("Hunter.io não configurado - pulando busca")
            return None
        
        if not domain:
            # Tentar inferir domínio do nome da empresa
            domain = self._guess_domain(company_name)
        
        if not domain:
            logger.debug(f"Não foi possível determinar domínio para: {company_name}")
            return None
        
        # Se temos nome da pessoa, usar Email Finder
        if person_name:
            # Separar primeiro e último nome
            parts = person_name.strip().split()
            if len(parts) >= 2:
                first_name = parts[0]
                last_name = parts[-1]
                
                data = self._make_request('email-finder', {
                    'domain': domain,
                    'first_name': first_name,
                    'last_name': last_name,
                })
                
                if data and data.get('email'):
                    self.stats['emails_found'] += 1
                    return ExtractedContact(
                        email=data['email'],
                        person_name=person_name,
                        contact_type=ContactType.CORPORATE,
                        source=ContactSource.HUNTER_IO,
                        confidence=data.get('score', 50) / 100,
                        raw_data=data,
                    )
        
        # Fallback: Domain Search para pegar qualquer email
        return self._domain_search_first(domain, company_name)
    
    def find_company_contacts(
        self,
        company_name: str,
        limit: int = 5
    ) -> List[ExtractedContact]:
        """
        Busca múltiplos contatos de uma empresa via Domain Search.
        """
        if not self.is_configured:
            logger.debug("Hunter.io não configurado - pulando busca")
            return []
        
        domain = self._guess_domain(company_name)
        if not domain:
            return []
        
        data = self._make_request('domain-search', {
            'domain': domain,
            'limit': limit,
        })
        
        if not data:
            return []
        
        contacts = []
        for email_data in data.get('emails', [])[:limit]:
            self.stats['emails_found'] += 1
            
            # Determinar tipo de contato baseado no cargo
            contact_type = ContactType.CORPORATE
            position = email_data.get('position', '').lower()
            if any(word in position for word in ['ceo', 'owner', 'president', 'director']):
                contact_type = ContactType.PRINCIPAL
            
            contacts.append(ExtractedContact(
                email=email_data.get('value'),
                person_name=f"{email_data.get('first_name', '')} {email_data.get('last_name', '')}".strip(),
                person_title=email_data.get('position'),
                contact_type=contact_type,
                source=ContactSource.HUNTER_IO,
                confidence=email_data.get('confidence', 50) / 100,
                raw_data=email_data,
            ))
        
        return contacts
    
    def _domain_search_first(
        self,
        domain: str,
        company_name: str
    ) -> Optional[ExtractedContact]:
        """Busca primeiro email disponível de um domínio."""
        data = self._make_request('domain-search', {
            'domain': domain,
            'limit': 1,
        })
        
        if data and data.get('emails'):
            email_data = data['emails'][0]
            self.stats['emails_found'] += 1
            return ExtractedContact(
                email=email_data.get('value'),
                person_name=f"{email_data.get('first_name', '')} {email_data.get('last_name', '')}".strip() or None,
                person_title=email_data.get('position'),
                contact_type=ContactType.CORPORATE,
                source=ContactSource.HUNTER_IO,
                confidence=email_data.get('confidence', 50) / 100,
                raw_data=email_data,
            )
        
        return None
    
    def _guess_domain(self, company_name: str) -> Optional[str]:
        """
        Tenta inferir domínio a partir do nome da empresa.
        
        Exemplo: "Acme Holdings LLC" -> "acmeholdings.com"
        """
        if not company_name:
            return None
        
        # Remover sufixos corporativos
        name = company_name.upper()
        suffixes = [
            ' LLC', ' L.L.C.', ' INC', ' INC.', ' CORP', ' CORP.',
            ' CORPORATION', ' LP', ' L.P.', ' LLP', ' LIMITED',
            ' LTD', ' CO', ' CO.', ' COMPANY', ' HOLDINGS',
            ' PROPERTIES', ' INVESTMENTS', ' TRUST', ' ENTERPRISES',
        ]
        
        for suffix in suffixes:
            if name.endswith(suffix):
                name = name[:-len(suffix)]
        
        # Limpar e formatar
        name = re.sub(r'[^a-zA-Z0-9\s]', '', name)
        name = name.strip().lower()
        name = name.replace(' ', '')
        
        if not name:
            return None
        
        return f"{name}.com"


# =============================================================================
# APOLLO SERVICE
# =============================================================================

class ApolloService(ContactEnrichmentService):
    """
    Serviço de enriquecimento usando Apollo.io API.
    
    Apollo oferece:
    - People Search: busca pessoas por empresa
    - Organization Search: busca empresas
    - Email enrichment
    
    Plano gratuito: 10,000 créditos/mês
    Docs: https://apolloio.github.io/apollo-api-docs/
    """
    
    API_BASE = "https://api.apollo.io/v1"
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Inicializa o serviço.
        
        Args:
            api_key: API key do Apollo (ou usa APOLLO_API_KEY do ambiente)
        """
        self.api_key = api_key or os.environ.get('APOLLO_API_KEY')
        
        self.session = requests.Session()
        self.session.headers.update({
            'Content-Type': 'application/json',
            'Cache-Control': 'no-cache',
        })
        
        self.stats = {
            'requests': 0,
            'successful': 0,
            'failed': 0,
            'emails_found': 0,
        }
        
        if self.is_configured:
            logger.info("ApolloService configurado com API key")
        else:
            logger.warning("ApolloService sem API key - funcionalidade desabilitada")
    
    @property
    def service_name(self) -> str:
        return "Apollo.io"
    
    @property
    def is_configured(self) -> bool:
        return bool(self.api_key)
    
    def _make_request(
        self,
        endpoint: str,
        data: Dict[str, Any],
        method: str = 'POST'
    ) -> Optional[Dict]:
        """Faz request à API."""
        if not self.is_configured:
            return None
        
        data['api_key'] = self.api_key
        url = f"{self.API_BASE}/{endpoint}"
        
        self.stats['requests'] += 1
        
        try:
            if method == 'POST':
                response = self.session.post(url, json=data, timeout=10)
            else:
                response = self.session.get(url, params=data, timeout=10)
            
            if response.status_code == 401:
                logger.error("Apollo: API key inválida")
                return None
            
            if response.status_code == 429:
                logger.warning("Apollo: Rate limit atingido")
                return None
            
            if response.status_code not in (200, 201):
                logger.warning(f"Apollo: HTTP {response.status_code}")
                return None
            
            self.stats['successful'] += 1
            return response.json()
            
        except Exception as e:
            self.stats['failed'] += 1
            logger.error(f"Apollo error: {e}")
            return None
    
    def find_email(
        self,
        company_name: str,
        person_name: Optional[str] = None,
        domain: Optional[str] = None
    ) -> Optional[ExtractedContact]:
        """
        Busca email para uma pessoa/empresa.
        """
        if not self.is_configured:
            logger.debug("Apollo não configurado - pulando busca")
            return None
        
        # Usar People Search
        search_params = {
            'q_organization_name': company_name,
            'page': 1,
            'per_page': 1,
        }
        
        if person_name:
            search_params['q_person_name'] = person_name
        
        data = self._make_request('mixed_people/search', search_params)
        
        if data and data.get('people'):
            person = data['people'][0]
            email = person.get('email')
            
            if email:
                self.stats['emails_found'] += 1
                return ExtractedContact(
                    email=email,
                    person_name=person.get('name'),
                    person_title=person.get('title'),
                    contact_type=ContactType.CORPORATE,
                    source=ContactSource.APOLLO,
                    confidence=0.8 if person.get('email_status') == 'verified' else 0.5,
                    raw_data=person,
                )
        
        return None
    
    def find_company_contacts(
        self,
        company_name: str,
        limit: int = 5
    ) -> List[ExtractedContact]:
        """
        Busca múltiplos contatos de uma empresa.
        """
        if not self.is_configured:
            logger.debug("Apollo não configurado - pulando busca")
            return []
        
        search_params = {
            'q_organization_name': company_name,
            'page': 1,
            'per_page': limit,
        }
        
        data = self._make_request('mixed_people/search', search_params)
        
        if not data:
            return []
        
        contacts = []
        for person in data.get('people', [])[:limit]:
            email = person.get('email')
            if email:
                self.stats['emails_found'] += 1
                
                # Determinar tipo
                contact_type = ContactType.CORPORATE
                title = (person.get('title') or '').lower()
                if any(word in title for word in ['owner', 'ceo', 'president', 'founder']):
                    contact_type = ContactType.PRINCIPAL
                
                contacts.append(ExtractedContact(
                    email=email,
                    person_name=person.get('name'),
                    person_title=person.get('title'),
                    contact_type=contact_type,
                    source=ContactSource.APOLLO,
                    confidence=0.8 if person.get('email_status') == 'verified' else 0.5,
                    raw_data=person,
                ))
        
        return contacts


# =============================================================================
# CONTACT ENRICHMENT ORCHESTRATOR
# =============================================================================

class ContactEnrichmentOrchestrator:
    """
    Orquestrador de enriquecimento de contatos.
    
    Combina múltiplas estratégias:
    1. Scraping de websites de parques
    2. APIs de enriquecimento (Hunter, Apollo)
    """
    
    def __init__(
        self,
        website_scraper: Optional[WebsiteContactScraper] = None,
        enrichment_services: Optional[List[ContactEnrichmentService]] = None,
        db_engine=None,
    ):
        """
        Inicializa o orquestrador.
        
        Args:
            website_scraper: Scraper de websites
            enrichment_services: Lista de serviços de API
            db_engine: Engine SQLAlchemy
        """
        self.scraper = website_scraper or WebsiteContactScraper()
        
        # Inicializar serviços de API (mesmo sem keys, para fail-safe)
        if enrichment_services is None:
            self.services = [
                HunterIoService(),
                ApolloService(),
            ]
        else:
            self.services = enrichment_services
        
        # Filtrar serviços configurados
        self.active_services = [s for s in self.services if s.is_configured]
        
        if db_engine is None:
            from ..database import get_engine
            self.engine = get_engine()
        else:
            self.engine = db_engine
        
        self.stats = {
            'parks_processed': 0,
            'companies_processed': 0,
            'contacts_from_scrape': 0,
            'contacts_from_api': 0,
            'contacts_saved': 0,
        }
        
        logger.info(f"ContactEnrichmentOrchestrator inicializado")
        logger.info(f"  Serviços ativos: {[s.service_name for s in self.active_services] or ['Nenhum']}")
    
    def enrich_park_contacts(
        self,
        park_id: int,
        website: str,
        park_name: Optional[str] = None
    ) -> List[ExtractedContact]:
        """
        Enriquece contatos de um parque via scraping do website.
        
        Args:
            park_id: ID do parque
            website: URL do website
            park_name: Nome do parque (para logging)
            
        Returns:
            Lista de contatos extraídos
        """
        self.stats['parks_processed'] += 1
        
        result = self.scraper.scrape_website(website, park_name)
        
        # Logar resultado no banco
        self._log_scrape_result(park_id, None, result)
        
        if result.success and result.contacts:
            self.stats['contacts_from_scrape'] += len(result.contacts)
            
            # Salvar contatos
            for contact in result.contacts:
                self._save_contact(contact, park_id=park_id)
        
        return result.contacts if result.success else []
    
    def enrich_company_contacts(
        self,
        company_id: int,
        company_name: str,
        registered_agent: Optional[str] = None,
        principals: Optional[List[Dict]] = None
    ) -> List[ExtractedContact]:
        """
        Enriquece contatos de uma empresa via APIs.
        
        Args:
            company_id: ID da empresa
            company_name: Nome da empresa
            registered_agent: Nome do registered agent
            principals: Lista de principals
            
        Returns:
            Lista de contatos encontrados
        """
        self.stats['companies_processed'] += 1
        
        if not self.active_services:
            logger.debug(f"Nenhum serviço de API configurado para enriquecer: {company_name}")
            return []
        
        contacts = []
        
        # Tentar cada serviço ativo
        for service in self.active_services:
            try:
                # 1. Buscar para registered agent
                if registered_agent:
                    contact = service.find_email(
                        company_name=company_name,
                        person_name=registered_agent
                    )
                    if contact:
                        contact.contact_type = ContactType.REGISTERED_AGENT
                        contact.person_name = registered_agent
                        contacts.append(contact)
                
                # 2. Buscar para principals
                if principals:
                    for principal in principals[:3]:  # Limitar a 3
                        p_name = principal.get('name')
                        if p_name:
                            contact = service.find_email(
                                company_name=company_name,
                                person_name=p_name
                            )
                            if contact:
                                contact.contact_type = ContactType.PRINCIPAL
                                contact.person_name = p_name
                                contact.person_title = principal.get('title')
                                contacts.append(contact)
                
                # 3. Fallback: buscar qualquer contato da empresa
                if not contacts:
                    company_contacts = service.find_company_contacts(company_name, limit=2)
                    contacts.extend(company_contacts)
                
                # Se encontrou contatos, não precisa tentar outros serviços
                if contacts:
                    break
                    
            except Exception as e:
                logger.error(f"Erro no serviço {service.service_name}: {e}")
                continue
        
        # Salvar contatos
        for contact in contacts:
            self.stats['contacts_from_api'] += 1
            self._save_contact(contact, company_id=company_id)
        
        return contacts
    
    def _log_scrape_result(
        self,
        park_id: Optional[int],
        company_id: Optional[int],
        result: ScrapeResult
    ):
        """Loga resultado de scraping no banco."""
        from sqlalchemy import text
        
        try:
            with self.engine.connect() as conn:
                status = 'success' if result.success else 'failed'
                if result.error_message:
                    if 'timeout' in result.error_message.lower():
                        status = 'timeout'
                    elif 'robots' in result.error_message.lower():
                        status = 'blocked'
                
                if result.success and not result.contacts:
                    status = 'no_contacts'
                
                conn.execute(text("""
                    INSERT INTO contact_scrape_log (
                        park_id, company_id, url, scrape_status,
                        emails_found, phones_found, error_message,
                        response_code, response_time_ms, robots_allowed
                    ) VALUES (
                        :park_id, :company_id, :url, :status,
                        :emails, :phones, :error,
                        :code, :time_ms, :robots
                    )
                """), {
                    'park_id': park_id,
                    'company_id': company_id,
                    'url': result.url,
                    'status': status,
                    'emails': len([c for c in result.contacts if c.email]),
                    'phones': len([c for c in result.contacts if c.phone]),
                    'error': result.error_message,
                    'code': result.status_code,
                    'time_ms': result.response_time_ms,
                    'robots': result.robots_allowed,
                })
                conn.commit()
        except Exception as e:
            logger.debug(f"Erro ao logar scrape result: {e}")
    
    def _save_contact(
        self,
        contact: ExtractedContact,
        park_id: Optional[int] = None,
        company_id: Optional[int] = None,
        owner_id: Optional[int] = None
    ):
        """Salva contato no banco."""
        from sqlalchemy import text
        
        try:
            with self.engine.connect() as conn:
                # Verificar duplicata
                if contact.email:
                    existing = conn.execute(text("""
                        SELECT id FROM contacts 
                        WHERE LOWER(email) = LOWER(:email)
                        AND (park_id = :park_id OR (park_id IS NULL AND :park_id IS NULL))
                        AND (company_id = :company_id OR (company_id IS NULL AND :company_id IS NULL))
                    """), {
                        'email': contact.email,
                        'park_id': park_id,
                        'company_id': company_id,
                    }).fetchone()
                    
                    if existing:
                        logger.debug(f"Contato duplicado ignorado: {contact.email}")
                        return
                
                conn.execute(text("""
                    INSERT INTO contacts (
                        park_id, company_id, owner_id,
                        contact_type, email, phone, phone_type,
                        person_name, person_title,
                        source, source_url, confidence_level
                    ) VALUES (
                        :park_id, :company_id, :owner_id,
                        :contact_type, :email, :phone, :phone_type,
                        :person_name, :person_title,
                        :source, :source_url, :confidence
                    )
                """), {
                    'park_id': park_id,
                    'company_id': company_id,
                    'owner_id': owner_id,
                    'contact_type': contact.contact_type.value,
                    'email': contact.email,
                    'phone': contact.phone,
                    'phone_type': contact.phone_type,
                    'person_name': contact.person_name,
                    'person_title': contact.person_title,
                    'source': contact.source.value,
                    'source_url': contact.source_url,
                    'confidence': contact.confidence,
                })
                conn.commit()
                self.stats['contacts_saved'] += 1
                
        except Exception as e:
            logger.error(f"Erro ao salvar contato: {e}")
    
    def process_parks_with_websites(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Processa todos os parques que têm website.
        
        Args:
            limit: Número máximo de parques a processar
            
        Returns:
            Estatísticas do processamento
        """
        from sqlalchemy import text
        
        logger.info("="*70)
        logger.info("ENRIQUECIMENTO DE CONTATOS - WEBSITES DE PARQUES")
        logger.info("="*70)
        
        with self.engine.connect() as conn:
            query = """
                SELECT pm.id, pm.name, pm.website
                FROM parks_master pm
                LEFT JOIN contact_scrape_log csl ON pm.id = csl.park_id
                WHERE pm.website IS NOT NULL 
                  AND pm.website != ''
                  AND csl.id IS NULL  -- Não processado ainda
                ORDER BY pm.id
            """
            if limit:
                query += f" LIMIT {limit}"
            
            parks = conn.execute(text(query)).fetchall()
        
        logger.info(f"Encontrados {len(parks)} parques com websites para processar")
        
        for i, park in enumerate(parks, 1):
            park_id, name, website = park
            logger.info(f"\n[{i}/{len(parks)}] {name}")
            
            try:
                self.enrich_park_contacts(park_id, website, name)
            except Exception as e:
                logger.error(f"Erro ao processar parque {park_id}: {e}")
        
        return {
            'parks_processed': self.stats['parks_processed'],
            'contacts_found': self.stats['contacts_from_scrape'],
            'scraper_stats': self.scraper.stats,
        }
    
    def process_companies(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Processa empresas para enriquecimento via APIs.
        
        Args:
            limit: Número máximo de empresas a processar
            
        Returns:
            Estatísticas do processamento
        """
        from sqlalchemy import text
        
        if not self.active_services:
            logger.warning("Nenhum serviço de API configurado - pulando enriquecimento de empresas")
            return {'skipped': True, 'reason': 'No API services configured'}
        
        logger.info("="*70)
        logger.info("ENRIQUECIMENTO DE CONTATOS - APIs COMERCIAIS")
        logger.info("="*70)
        
        with self.engine.connect() as conn:
            query = """
                SELECT c.id, c.legal_name, c.registered_agent_name, c.principals
                FROM companies c
                LEFT JOIN contacts ct ON c.id = ct.company_id
                WHERE ct.id IS NULL  -- Sem contatos ainda
                ORDER BY c.id
            """
            if limit:
                query += f" LIMIT {limit}"
            
            companies = conn.execute(text(query)).fetchall()
        
        logger.info(f"Encontradas {len(companies)} empresas para processar")
        
        for i, company in enumerate(companies, 1):
            company_id, name, agent, principals_json = company
            logger.info(f"\n[{i}/{len(companies)}] {name}")
            
            principals = None
            if principals_json:
                try:
                    principals = principals_json if isinstance(principals_json, list) else json.loads(principals_json)
                except Exception:
                    pass
            
            try:
                self.enrich_company_contacts(
                    company_id=company_id,
                    company_name=name,
                    registered_agent=agent,
                    principals=principals
                )
            except Exception as e:
                logger.error(f"Erro ao processar empresa {company_id}: {e}")
        
        return {
            'companies_processed': self.stats['companies_processed'],
            'contacts_found': self.stats['contacts_from_api'],
        }
    
    def print_summary(self):
        """Imprime resumo do processamento."""
        logger.info("\n" + "="*70)
        logger.info("RESUMO DO ENRIQUECIMENTO DE CONTATOS")
        logger.info("="*70)
        logger.info(f"Parques processados: {self.stats['parks_processed']}")
        logger.info(f"Empresas processadas: {self.stats['companies_processed']}")
        logger.info(f"Contatos via scraping: {self.stats['contacts_from_scrape']}")
        logger.info(f"Contatos via APIs: {self.stats['contacts_from_api']}")
        logger.info(f"Total contatos salvos: {self.stats['contacts_saved']}")
        logger.info("="*70)
        
        if self.scraper.stats['sites_scraped'] > 0:
            logger.info("\nEstatísticas do Scraper:")
            logger.info(f"  Sites visitados: {self.scraper.stats['sites_scraped']}")
            logger.info(f"  Sucesso: {self.scraper.stats['successful']}")
            logger.info(f"  Falhas: {self.scraper.stats['failed']}")
            logger.info(f"  Timeouts: {self.scraper.stats['timeouts']}")
            logger.info(f"  Bloqueados (robots): {self.scraper.stats['blocked_by_robots']}")


# =============================================================================
# EXPORTS
# =============================================================================

__all__ = [
    'WebsiteContactScraper',
    'ContactEnrichmentService',
    'HunterIoService',
    'ApolloService',
    'ContactEnrichmentOrchestrator',
    'ExtractedContact',
    'ScrapeResult',
    'ContactType',
    'ContactSource',
]
