# MHP Intelligence - Sistema de Mapeamento de Parques

Sistema de inteligência de negócios para mapeamento completo de Mobile Home Parks (MHP) e RV Parks em Indiana, com foco em identificação de proprietários para campanhas de mala direta.

## 📋 Visão Geral

Este projeto implementa um pipeline de dados escalável e em conformidade legal (TOS/robots.txt) para:

1. **Coletar** dados de múltiplas fontes (OpenStreetMap, Google Places API, registros governamentais)
2. **Consolidar** e deduplica informações em um banco de dados mestre
3. **Identificar** proprietários legais de parques
4. **Exportar** listas para campanhas de mala direta

## 🏗️ Estrutura do Projeto

```
scrappingLands/
├── config/                    # Arquivos de configuração
│   └── indiana.yaml          # Configuração específica de Indiana
├── data/                     # Dados e cache
│   └── cache/               # Cache de API calls
├── logs/                    # Arquivos de log
├── scripts/                 # Scripts de execução
│   └── populate_parks_raw.py # Popular tabela parks_raw
├── src/                     # Código fonte
│   ├── ingestion/          # Módulos de ingestão de dados
│   │   ├── osm_query.py    # OpenStreetMap via Overpass API
│   │   └── google_places.py # Google Places API
│   ├── database.py         # Conexão PostgreSQL/PostGIS
│   ├── models.py           # Modelos Pydantic
│   └── schema.sql          # Schema do banco de dados
├── tests/                  # Testes unitários
├── .env.example           # Template de variáveis de ambiente
├── pyproject.toml        # Dependências Poetry
├── requirements.txt      # Dependências pip
└── README.md            # Esta documentação
```

## 🗄️ Schema do Banco de Dados

### Tabelas Principais

1. **`parks_raw`** - Dados brutos de todas as fontes
2. **`parks_master`** - Dados consolidados e deduplicados
3. **`owners`** - Proprietários individuais
4. **`companies`** - Empresas proprietárias (LLCs, REITs, etc)

## 🚀 Instalação e Configuração

### Pré-requisitos

- Python 3.10+
- PostgreSQL 14+ com extensão PostGIS
- Poetry (opcional, recomendado)

### Passo 1: Instalar Dependências

**Opção A: Usando Poetry (recomendado)**

```powershell
poetry install
poetry shell
```

**Opção B: Usando pip**

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

### Passo 2: Configurar Banco de Dados

1. Criar banco PostgreSQL:

```sql
CREATE DATABASE mhp_intelligence;
\c mhp_intelligence
CREATE EXTENSION postgis;
```

2. Executar schema:

```powershell
psql -U postgres -d mhp_intelligence -f src/schema.sql
```

### Passo 3: Configurar Variáveis de Ambiente

1. Copiar template:

```powershell
copy .env.example .env
```

2. Editar `.env` com suas credenciais:

```env
# Banco de dados
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mhp_intelligence
DB_USER=postgres
DB_PASSWORD=sua_senha_aqui

# APIs (opcional)
GOOGLE_PLACES_API_KEY=sua_chave_api_google
```

### Passo 4: Testar Conexão

```powershell
python -c "from src.database import test_connection; test_connection()"
```

## 📊 Uso - Ingestão de Dados

### Método 1: Script Interativo (Recomendado)

```powershell
python scripts/populate_parks_raw.py
```

Este script oferece opções para:

- Buscar dados do OpenStreetMap (gratuito)
- Buscar dados do Google Places (requer API key)
- Combinar ambas as fontes

### Método 2: Uso Programático

#### OpenStreetMap (Gratuito)

```python
import yaml
from src.models import StateConfig
from src.ingestion.osm_query import fetch_osm_parks

# Carregar configuração
with open('config/indiana.yaml') as f:
    config = StateConfig(**yaml.safe_load(f))

# Buscar parques
parks = fetch_osm_parks(config)
print(f"Encontrados {len(parks)} parques no OSM")
```

#### Google Places API

```python
from src.ingestion.google_places import fetch_google_parks

# Buscar parques (usa cache automático)
parks = fetch_google_parks(
    state_config=config,
    grid_spacing_km=50  # Espaçamento da grade
)
print(f"Encontrados {len(parks)} parques no Google Places")
```

## 🔧 Módulos Principais

### `src/ingestion/osm_query.py`

Busca dados do OpenStreetMap via Overpass API.

**Características:**

- ✅ Totalmente gratuito
- ✅ Respeita rate limits (1 req/segundo padrão)
- ✅ Busca por tags: `tourism=camp_site`, `tourism=caravan_site`, `landuse=residential`
- ✅ Cobertura completa de Indiana via bounding box

**Uso:**

```python
from src.ingestion.osm_query import fetch_osm_parks

parks = fetch_osm_parks(state_config)
# Retorna List[ParkRawData]
```

### `src/ingestion/google_places.py`

Busca dados do Google Places API com cobertura em grade.

**Características:**

- ✅ **Caching inteligente** - nunca chama Place Details duas vezes para o mesmo `place_id`
- ✅ **Cobertura em grade** - divide Indiana em grid de ~40-50km para não perder parques rurais
- ✅ **Rate limiting** - respeita quotas da API
- ✅ **Quota tracking** - monitora uso diário
- ✅ **Enriquecimento de dados** - busca telefone, website, avaliações, etc

**Lógica da Grade:**

1. Gera pontos espaçados em ~40-50km cobrindo Indiana
2. Para cada ponto, executa Nearby Search com múltiplos keywords
3. Coleta `place_id` únicos (evita duplicatas)
4. Para cada `place_id`, busca Place Details (com cache!)

**Cache:**

- Armazena em `data/cache/place_details/{place_id}.json`
- Expira após 7 dias
- Reduz drasticamente custos de API

**Uso:**

```python
from src.ingestion.google_places import fetch_google_parks

parks = fetch_google_parks(
    state_config=config,
    keywords=["rv park", "mobile home park"],
    grid_spacing_km=50
)
```

## 📈 Próximos Passos (Fase 1)

Após popular `parks_raw`, os próximos módulos a desenvolver:

1. **Deduplicação** - Identificar parques duplicados entre fontes
2. **Consolidação** - Popular `parks_master` com dados limpos
3. **Enriquecimento** - Buscar dados adicionais (registros de condado, etc)
4. **Identificação de Proprietários** - Popular tabelas `owners` e `companies`

## 🔒 Conformidade Legal

Este projeto é construído com foco em conformidade:

- ✅ **Respeita robots.txt** - Verifica antes de fazer scraping
- ✅ **Rate limiting** - Não sobrecarrega servidores
- ✅ **User-Agent** identificado - Transparência nas requisições
- ✅ **TOS compliance** - Segue termos de serviço de cada fonte
- ✅ **Caching** - Reduz requisições desnecessárias

## 🌍 Replicação para Outros Estados

Para replicar este sistema para outro estado:

1. Criar arquivo de configuração (ex: `config/ohio.yaml`)
2. Ajustar bounding box e parâmetros geográficos
3. Executar os mesmos scripts de ingestão
4. Os dados serão isolados por `state` na tabela

## 📝 Logging

Todos os módulos geram logs detalhados em `logs/`:

- `osm_ingestion_{time}.log` - Ingestão OSM
- `google_places_{time}.log` - Ingestão Google Places
- `populate_db_{time}.log` - População do banco

## 🐛 Solução de Problemas

### Erro: "Import could not be resolved"

Os erros de import no IDE são normais antes de instalar as dependências. Execute:

```powershell
poetry install
# ou
pip install -r requirements.txt
```

### Erro: "GOOGLE_PLACES_API_KEY não definida"

Configure a chave no arquivo `.env`:

```env
GOOGLE_PLACES_API_KEY=sua_chave_aqui
```

### Erro: "Quota diária atingida"

Ajuste o limite no `.env` ou espere até o dia seguinte:

```env
MAX_API_CALLS_PER_DAY=20000
```

## 📧 Contato

Para dúvidas ou sugestões sobre o projeto, consulte a documentação ou revise os logs.

---

**Versão:** 0.1.0  
**Fase Atual:** Fase 0/1 - Ingestão de Dados Brutos  
**Última Atualização:** Dezembro 2025
