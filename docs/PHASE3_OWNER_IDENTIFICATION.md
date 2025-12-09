# 🏛️ Fase 3: Identificação de Proprietários via County Assessor Records

## Visão Geral

A Fase 3 identifica os **proprietários legais** e seus **endereços para mala direta** através dos registros fiscais públicos dos condados de Indiana (County Assessor Records).

### Desafio

Indiana possui **92 condados** com sistemas completamente diferentes:

- ~40 condados: **Beacon/Schneider Corp**
- ~25 condados: **GIS customizados**
- ~15 condados: **Vanguard Appraisals**
- ~12 condados: **Sem sistema online** (requer FOIA ou contato telefônico)

### Solução: Arquitetura de Adapters

Implementamos um padrão de design baseado em **Adapters**, permitindo:

- ✅ Adicionar novos condados sem alterar código core
- ✅ Fallback automático para métodos alternativos
- ✅ Reutilização de código para sistemas similares
- ✅ Escalabilidade para outros estados

---

## 📁 Estrutura de Arquivos

```
src/owners/
├── __init__.py                  # Exports públicos
├── county_mapper.py             # 🗺️ Identifica condado por coordenadas
├── base_fetcher.py              # 🏗️ Classe abstrata base
├── orchestrator.py              # 🎯 Coordenador principal
└── fetchers/
    ├── __init__.py
    ├── generic_fetcher.py       # Fallback com Google Search
    ├── beacon_fetcher.py        # TODO: Para ~40 condados Beacon
    └── vanguard_fetcher.py      # TODO: Para ~15 condados Vanguard

data/geo/
└── indiana_counties.geojson     # Limites dos 92 condados

scripts/
└── identify_owners.py           # 🚀 Script de execução
```

---

## 🗺️ County Mapper (`county_mapper.py`)

### Propósito

Identificar o condado de Indiana baseado em coordenadas geográficas (lat/lon).

### Funcionamento

1. **Método Primário**: Point-in-Polygon com GeoJSON

   - Carrega `indiana_counties.geojson` com os limites dos 92 condados
   - Usa `shapely` para verificar se ponto está dentro do polígono
   - **Complexidade**: O(n) onde n=92 (aceitável)

2. **Fallback**: Geopy Reverse Geocoding
   - Se GeoJSON não disponível, usa API do Nominatim
   - ⚠️ Requer internet e respeita rate limit (1 req/sec)

### Uso

```python
from src.owners.county_mapper import CountyMapper

mapper = CountyMapper()

# Indianapolis
county = mapper.identify_county(39.7684, -86.1581)
# Retorna: "Marion County"

# Informações do condado
info = mapper.get_county_info("Marion County")
print(info['assessor_system'])  # "Beacon/Schneider Corp"
print(info['assessor_url'])     # URL do sistema
```

### Download do GeoJSON

```python
from src.owners.county_mapper import download_indiana_counties_geojson

# Baixa e filtra apenas Indiana do US Census
download_indiana_counties_geojson()
# Salvo em: data/geo/indiana_counties.geojson
```

**Fonte**: US Census TIGER/Line Shapefiles (via Plotly/datasets)

---

## 🏗️ Base Fetcher (`base_fetcher.py`)

### Classe Abstrata: `CountyAssessorFetcher`

Define a interface que **todos os fetchers** devem implementar:

```python
from abc import ABC, abstractmethod

class CountyAssessorFetcher(ABC):

    @abstractmethod
    def lookup_owner(
        self,
        address: str,
        lat: float,
        lon: float,
        parcel_id: Optional[str] = None
    ) -> FetchResult:
        """Busca proprietário por endereço/coordenadas."""
        pass

    @abstractmethod
    def search_by_parcel_id(self, parcel_id: str) -> FetchResult:
        """Busca proprietário por Parcel ID (mais rápido)."""
        pass
```

### Estrutura de Dados: `OwnerRecord`

Formato **padronizado** retornado por todos os fetchers:

```python
@dataclass
class OwnerRecord:
    # Proprietário
    owner_name_1: str                    # Nome principal
    owner_name_2: Optional[str] = None   # Co-proprietário

    # Mailing Address (CRITICAL!)
    mailing_address_line1: str
    mailing_address_line2: Optional[str]
    mailing_city: str
    mailing_state: str
    mailing_zip: str
    mailing_country: str = "USA"

    # Propriedade
    parcel_id: str
    property_address: str
    property_class_code: str             # "102" = Mobile Home
    assessed_value: Optional[float]
    tax_year: Optional[int]

    # Metadados
    source: str                          # Ex: "Marion County Beacon"
    source_url: str
    confidence_score: float              # 0.0 a 1.0
    is_valid_mailing_address: bool
    needs_manual_review: bool
```

### Property Class Codes (Indiana)

| Código  | Tipo                          |
| ------- | ----------------------------- |
| **102** | **Mobile Home** (nosso foco!) |
| 100     | Residencial padrão            |
| 300     | Comercial                     |
| 400     | Industrial                    |
| 500     | Agrícola                      |
| 600     | Isento (igreja, governo)      |

---

## 🔍 Generic Fetcher (`generic_fetcher.py`)

### Propósito

Fetcher **fallback** que usa Google Custom Search API quando:

- Condado não tem implementação específica
- Sistema Beacon/Vanguard falha
- Condado não tem sistema online

### Funcionamento

1. **Construir query otimizada**:

   ```
   "[Endereço]" "[Condado]" county assessor indiana property owner
   ```

2. **Buscar no Google** (top 10 resultados)

3. **Parsear resultados** com regex:

   - Padrões: "Owner: JOHN DOE", "Taxpayer: ABC LLC", etc
   - Extrai nome e endereço de correspondência

4. **Validar e retornar** `OwnerRecord`

### Limitações

⚠️ **Google Custom Search API**:

- 100 queries **grátis/dia**
- Depois: **$5 por 1000 queries**
- Resultados podem ser imprecisos
- **Sempre marca `needs_manual_review = TRUE`**

### Configuração

```bash
# .env
GOOGLE_CUSTOM_SEARCH_API_KEY=sua_chave_aqui
GOOGLE_CUSTOM_SEARCH_ENGINE_ID=seu_cx_aqui
```

Obter em: https://developers.google.com/custom-search/v1/overview

### Mock Fetcher (Desenvolvimento)

Para **testes sem consumir APIs**:

```python
from src.owners.fetchers.generic_fetcher import MockFetcher

fetcher = MockFetcher("Test County")
result = fetcher.lookup_owner("123 Main St", 39.7684, -86.1581)

# Retorna dados fictícios
# 80% de sucesso aleatório
```

---

## 🎯 Orchestrator (`orchestrator.py`)

### Propósito

Coordenador principal que **orquestra todo o fluxo**:

```
parks_master → County Mapper → Fetcher → owners table
```

### Fluxo de Execução

```python
for park in parks_master:
    1. Identificar condado (county_mapper)
    2. Selecionar fetcher apropriado (factory)
    3. Buscar proprietário (com retries)
    4. Salvar em owners table
    5. Atualizar parks_master.owner_id
    6. Checkpoint a cada 10 parques
```

### Recursos de Robustez

#### 1. **Retry com Backoff Exponencial**

```python
Tentativa 1: Erro → aguarda 1s
Tentativa 2: Erro → aguarda 2s
Tentativa 3: Erro → aguarda 4s
```

#### 2. **Rate Limiting**

```python
# Delay entre requests (evita bloqueios)
delay_between_requests = 3.0  # segundos

# Beacon/Schneider: 10-20 req/min → 3-6s entre requests
# Vanguard: 30 req/min → 2s entre requests
```

#### 3. **Checkpoints**

```python
checkpoint_interval = 10  # Salva a cada 10 parques

# Se falhar no parque 47, pode retomar de onde parou
```

#### 4. **Estatísticas em Tempo Real**

```python
{
    'total_parks': 1200,
    'processed': 450,
    'successful': 380,
    'failed': 70,
    'owner_found': 380,
    'owner_not_found': 70,
    'county_not_identified': 5
}
```

### Uso Programático

```python
from src.owners.orchestrator import OwnerLookupOrchestrator

# Modo MOCK (desenvolvimento)
orchestrator = OwnerLookupOrchestrator(
    use_mock=True,
    max_retries=3,
    delay_between_requests=0.5,
    checkpoint_interval=10
)

# Processar até 10 parques (teste)
orchestrator.process_all_parks(limit=10)

# Processar parque específico
orchestrator.process_single_park_by_id(park_id=42)
```

---

## 🚀 Script de Execução

### Executar Fase 3

```powershell
python scripts/identify_owners.py
```

### Fluxo Interativo

1. **Verificação de pré-requisitos**:

   - ✅ Conexão com banco
   - ✅ `parks_master` populado
   - ✅ GeoJSON de condados (download se necessário)

2. **Configuração**:

   - Modo: MOCK vs PRODUÇÃO
   - Limite: N parques ou TODOS
   - Delay: 3-5 segundos (produção)

3. **Confirmação** antes de iniciar

4. **Processamento** com logs em tempo real

5. **Relatório final**

### Exemplo de Execução

```
🔍 Verificando pré-requisitos...
  1. Testando conexão com banco de dados...
     ✅ Conexão OK
  2. Verificando parks_master...
     ✅ 1200 parques encontrados
     📊 340 já têm proprietário identificado
     📊 860 precisam ser processados

MODO DE EXECUÇÃO:
  1. MOCK (desenvolvimento) - Dados fictícios
  2. PRODUÇÃO - Acessa County Assessor systems

Escolha o modo (1/2) [1]: 1

LIMITE DE PARQUES:
  Digite um número para processar apenas N parques
  Deixe em branco para processar TODOS

Limite: 10

✅ Processará até 10 parques
✅ Delay configurado: 0.5s

Pressione ENTER para iniciar...

🚀 Iniciando orchestrator...
================================================
PARQUE 1/10
================================================
📍 Parque: Sunset Mobile Home Park
   Endereço: 1234 Main St, Indianapolis, IN
   Coordenadas: (39.7684, -86.1581)
   🏛️ Condado: Marion County
✅ Proprietário encontrado!
   💾 Salvo: SUNSET PROPERTIES LLC

...

================================================
RELATÓRIO FINAL - OWNER LOOKUP
================================================
Total de parques: 10
Processados: 10
Sucessos: 8
Falhas: 2

Proprietários encontrados: 8
Proprietários NÃO encontrados: 2
Condados não identificados: 0

Duração: 12.3s
Tempo médio por parque: 1.23s
Taxa de sucesso: 80.0%
================================================
```

---

## ⚠️ Proteções Anti-Scraping

### Sistemas Beacon/Schneider Corp (~40 condados)

**Proteções**:

- Rate limit: 10-20 req/min
- CAPTCHA após ~50 requests consecutivos
- Detecção de User-Agent de bots
- Bloqueio por IP após abuso

**Estratégias**:

```python
# 1. Delays conservadores
delay_between_requests = 5.0  # 12 req/min

# 2. User-Agent rotation
from src.owners.base_fetcher import get_random_user_agent
headers = {'User-Agent': get_random_user_agent()}

# 3. Selenium com perfil humanizado (futuro)
# - Mouse movements
# - Random scrolling
# - Delays variáveis
```

### Sistemas Vanguard (~15 condados)

**Proteções**:

- Rate limit: ~30 req/min (mais relaxado)
- Geralmente sem CAPTCHA

**Estratégias**:

```python
delay_between_requests = 2.0  # Suficiente
```

### GIS Customizados (~25 condados)

**Proteções**: Variam muito

- Alguns sem proteção
- Outros com WAF (Web Application Firewall)

**Estratégias**:

```python
# Análise individual por condado
# Delays conservadores (5s) como padrão
```

---

## 🔄 Alternativas se Bloqueado

### 1. **Proxy Rotation** ($$)

- **ScraperAPI**: $49/mês (1000 req)
- **Bright Data**: $500+/mês (uso ilimitado)
- **SmartProxy**: $75/mês (5GB)

### 2. **Selenium Humanizado**

```python
from selenium import webdriver
from selenium.webdriver.common.action_chains import ActionChains

# Simular comportamento humano
actions = ActionChains(driver)
actions.move_by_offset(100, 200)  # Mouse movement
actions.perform()
time.sleep(random.uniform(0.5, 2.0))  # Random delays
```

### 3. **CAPTCHA Solving** ($$)

- **2Captcha**: $3/1000 CAPTCHAs
- **Anti-Captcha**: $2/1000 CAPTCHAs

### 4. **Comprar Dados Comerciais** ($$$$)

- **DataTree by First American**: ~$50-200 por condado
- **CoreLogic PropertyInfo API**: Enterprise pricing

### 5. **FOIA Request** (Gratuito mas lento)

```
Indiana Public Records Act (IC 5-14-3)
County Assessor Records são PÚBLICOS

Processo:
1. Solicitar registros em lote por escrito
2. Aguardar 30+ dias
3. Pagar custos de cópia (~$0.10/página)
```

---

## 📊 Estatísticas Esperadas

### Taxa de Sucesso Típica

| Condado | Sistema    | Taxa de Sucesso  | Tempo Médio  |
| ------- | ---------- | ---------------- | ------------ |
| Marion  | Beacon     | 85-90%           | 4-6s/parque  |
| Lake    | Beacon     | 80-85%           | 4-6s/parque  |
| Allen   | Custom GIS | 70-80%           | 6-10s/parque |
| Brown   | Vanguard   | 75-85%           | 3-5s/parque  |
| Orange  | Manual     | 0% (sem sistema) | N/A          |

### Estimativas para Indiana (1200 parques)

```
Cenário Otimista:
- 80% sucesso = 960 proprietários identificados
- Tempo: ~2-3 horas (com delays)
- Custo APIs: $0 (se usar apenas scraping)

Cenário Realista:
- 60% sucesso = 720 proprietários
- 40% requer revisão manual
- Tempo: ~4-6 horas
- Custo: $0-50 (se usar Google Search como fallback)
```

---

## 🧪 Testes e Desenvolvimento

### Testar County Mapper

```powershell
python src/owners/county_mapper.py
```

**Output**:

```
================================================
COUNTY MAPPER - Teste de Identificação
================================================

📍 Indianapolis - Marion County
   Coordenadas: (39.7684, -86.1581)
   ✅ Condado: Marion County
   Sistema: Beacon/Schneider Corp
   URL: https://beacon.schneidercorp.com/...
```

### Testar Generic Fetcher

```powershell
python src/owners/fetchers/generic_fetcher.py
```

**Output** (MOCK):

```
================================================
GENERIC FETCHER - Teste
================================================

🧪 Testando MockFetcher...

✅ Proprietário encontrado!
  Nome: MOCK PROPERTY OWNER LLC
  Endereço: 123 FAKE ST STE 100, MOCKVILLE, IN
  Parcel ID: 00-00-00-000-000.000-000
  Confidence: 0.90
  Notas: ⚠️ DADOS FICTÍCIOS - MOCK FETCHER
```

### Testar Orchestrator

```powershell
python src/owners/orchestrator.py --mock --limit 5
```

---

## 🔐 Considerações Legais

### ✅ Legal

- County Assessor Records são **PÚBLICOS** por lei de Indiana (IC 36-2-9)
- Web scraping de dados públicos é geralmente legal (**hiQ Labs v. LinkedIn**)
- Uso de dados para **mala direta comercial** é permitido

### ⚠️ Cuidados

- **Respeitar robots.txt** dos sites
- **Respeitar Terms of Service**
- **Não sobrecarregar** servidores públicos (rate limiting)
- **Compliance com CAN-SPAM Act** para mailing
- **Opt-out mechanism** obrigatório em malas diretas

### 📋 Compliance

```python
# Marcar proprietários que optaram por não receber correspondência
UPDATE owners
SET do_not_contact = TRUE
WHERE id = 123;

# Registrar bounces (correspondência devolvida)
UPDATE owners
SET bounce_count = bounce_count + 1
WHERE id = 456;

# Desabilitar mailing após 3 bounces
UPDATE owners
SET mail_eligible = FALSE
WHERE bounce_count >= 3;
```

---

## 📚 Próximos Passos

### Implementações Futuras

1. **BeaconFetcher** (`src/owners/fetchers/beacon_fetcher.py`)

   - Scraper específico para Beacon/Schneider Corp
   - Covers ~40 condados
   - Parsing de HTML + formulários

2. **VanguardFetcher** (`src/owners/fetchers/vanguard_fetcher.py`)

   - Scraper para Vanguard Appraisals
   - Covers ~15 condados

3. **Selenium Integration**

   - Para sites com heavy JavaScript
   - CAPTCHA handling

4. **Proxy Rotation**
   - Integração com ScraperAPI/Bright Data
   - Para alto volume

### Melhorias no Orchestrator

1. **Parallel Processing**

   ```python
   # ProcessPoolExecutor para processar múltiplos condados em paralelo
   # (diferentes IPs por condado)
   ```

2. **Retry Queue**

   ```python
   # Registros que falharam vão para fila de retry
   # Processados mais tarde com estratégia diferente
   ```

3. **Dashboard em Tempo Real**
   ```python
   # WebSocket para acompanhar progresso em UI
   # Gráficos de sucesso/falha por condado
   ```

---

## 🆘 Troubleshooting

### Problema: "County not identified"

**Causa**: Coordenadas fora de Indiana ou GeoJSON ausente

**Solução**:

```python
# Download GeoJSON
from src.owners.county_mapper import download_indiana_counties_geojson
download_indiana_counties_geojson()
```

### Problema: "Rate limited"

**Causa**: Muitos requests em pouco tempo

**Solução**:

```python
# Aumentar delay
orchestrator = OwnerLookupOrchestrator(
    delay_between_requests=10.0  # Mais conservador
)
```

### Problema: "CAPTCHA detected"

**Causa**: Sistema Beacon detectou bot

**Solução**:

1. Aguardar 1-2 horas
2. Usar proxy diferente
3. Implementar Selenium com perfil humanizado
4. Considerar CAPTCHA solving service

### Problema: "Owner not found"

**Causas possíveis**:

- Propriedade não está registrada
- Parcel ID incorreto
- Sistema do condado offline
- Endereço muito impreciso

**Solução**:

```sql
-- Marcar para revisão manual
UPDATE parks_master
SET needs_manual_review = TRUE
WHERE owner_id IS NULL;
```

---

**Última Atualização**: Dezembro 2025  
**Versão**: 1.0  
**Status**: ✅ Fase 3 Implementada (arquitetura de adapters com MockFetcher)
