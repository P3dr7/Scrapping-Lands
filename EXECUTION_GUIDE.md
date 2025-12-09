# 📋 Guia de Execução - Pipeline Completo

## 🎯 Resumo Rápido das Fases

| Fase       | Objetivo                        | Status       | Tempo Estimado |
| ---------- | ------------------------------- | ------------ | -------------- |
| **Fase 0** | Setup inicial (banco, config)   | ✅ COMPLETO  | 10 min         |
| **Fase 1** | Ingestão de dados (`parks_raw`) | ✅ COMPLETO  | 5-15 min       |
| **Fase 2** | Deduplicação (`parks_master`)   | ✅ COMPLETO  | 2-5 min        |
| **Fase 3** | Identificação de proprietários  | ✅ COMPLETO  | Variável\*     |
| **Fase 4** | Exportação para mala direta     | 🔜 PLANEJADO | -              |

\* _Tempo varia por condado: 5-30 seg/parque_

---

## ✅ Arquivos Criados

### Fase 0 - Configuração Base

- ✅ `pyproject.toml` - Configuração Poetry com todas as dependências
- ✅ `requirements.txt` - Alternativa pip para instalação
- ✅ `.env.example` - Template de variáveis de ambiente
- ✅ `config/indiana.yaml` - Configuração geográfica e de APIs para Indiana
- ✅ `src/database.py` - Módulo de conexão PostgreSQL/PostGIS
- ✅ `src/schema.sql` - Schema completo (4 tabelas + extensões Fase 3)
- ✅ `src/models.py` - Modelos Pydantic para validação de dados
- ✅ `.gitignore` - Configuração Git

### Fase 1 - Módulos de Ingestão

- ✅ `src/ingestion/osm_query.py` - Ingestão OpenStreetMap
- ✅ `src/ingestion/google_places.py` - Ingestão Google Places com cache
- ✅ `scripts/populate_parks_raw.py` - Script interativo para popular banco

### Fase 2 - Deduplicação

- ✅ `src/processing/deduplication.py` - Algoritmo completo de deduplicação
- ✅ `scripts/process_to_master.py` - Script de processamento para parks_master
- ✅ `docs/DEDUPLICATION_ALGORITHM.md` - Documentação técnica

### Fase 3 - Identificação de Proprietários

- ✅ `src/owners/county_mapper.py` - Mapeamento geográfico de condados
- ✅ `src/owners/base_fetcher.py` - Interface abstrata para fetchers
- ✅ `src/owners/orchestrator.py` - Orquestrador principal
- ✅ `src/owners/fetchers/generic_fetcher.py` - Fetcher genérico (Google Search)
- ✅ `scripts/identify_owners.py` - Script de execução da Fase 3
- ✅ `scripts/test_phase3.py` - Script de testes rápidos
- ✅ `docs/PHASE3_OWNER_IDENTIFICATION.md` - Documentação completa
- ✅ `docs/FETCHER_IMPLEMENTATION_GUIDE.md` - Guia para implementar fetchers
- ✅ `docs/PHASE3_SUMMARY.md` - Resumo executivo

---

## 🚀 EXECUÇÃO DO PIPELINE COMPLETO

### 📋 FASE 0: Setup Inicial (10 minutos)

#### Passo 1: Instalar Dependências

```powershell
# Opção A: Poetry (recomendado)
poetry install
poetry shell

# Opção B: pip
pip install -r requirements.txt
```

#### Passo 2: Configurar Variáveis de Ambiente

```powershell
# Copiar template
copy .env.example .env

# Editar .env com suas credenciais
notepad .env
```

**Variáveis obrigatórias:**

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/mhp_intelligence

# APIs (se for usar Google Places e Google Custom Search)
GOOGLE_PLACES_API_KEY=sua_chave_aqui
GOOGLE_CUSTOM_SEARCH_API_KEY=sua_chave_aqui
GOOGLE_CUSTOM_SEARCH_ENGINE_ID=seu_engine_id_aqui

# Rate Limiting
OSM_RATE_LIMIT_SECONDS=1.0
GOOGLE_PLACES_RATE_LIMIT=10
MAX_API_CALLS_PER_DAY=20000
```

#### Passo 3: Criar Schema do Banco

```powershell
python scripts/create_schema.py
```

**O que este script faz:**

- ✅ Cria extensão PostGIS
- ✅ Cria 4 tabelas principais: `companies`, `owners`, `parks_raw`, `parks_master`
- ✅ Adiciona índices geográficos e índices de busca
- ✅ Cria triggers para `updated_at`
- ✅ Valida que tudo foi criado corretamente

**Saída esperada:**

```
✅ PostGIS extension created successfully
✅ Schema created successfully
✅ Tables created: companies, owners, parks_raw, parks_master, spatial_ref_sys
```

---

### 📥 FASE 1: Ingestão de Dados (5-15 minutos)

#### Executar Script Interativo

```powershell
python scripts/populate_parks_raw.py
```

**Menu interativo:**

```
Escolha a fonte de dados:
1. OpenStreetMap (OSM) - GRATUITO
2. Google Places API - PAGO (~$16-25 para Indiana)
3. Ambas (Recomendado)
Digite sua escolha (1/2/3):
```

#### Opção 1: OpenStreetMap (Recomendado para começar)

**Vantagens:**

- ✅ Totalmente gratuito
- ✅ Sem necessidade de API keys
- ✅ Boa cobertura de parques maiores
- ✅ Execução rápida (~2-3 minutos)

**Limitações:**

- ⚠️ Pode ter gaps em parques pequenos/rurais
- ⚠️ Dados podem estar desatualizados

**Estimativa:** 200-400 parques em Indiana

#### Opção 2: Google Places API (Cobertura completa)

**Vantagens:**

- ✅ Cobertura mais completa
- ✅ Dados enriquecidos (telefone, website, reviews)
- ✅ Informações atualizadas
- ✅ Sistema de cache (economiza em re-execuções)

**Custos:**

- 💰 Nearby Search: $32 por 1000 requests
- 💰 Place Details: $17 por 1000 requests (campos básicos)
- 💰 **Total estimado para Indiana: $16-25**

**Estimativa:** 800-1500 parques em Indiana

#### Opção 3: Ambas (RECOMENDADO)

- Executa OSM primeiro (gratuito)
- Depois Google Places (complementa)
- Deduplicação automática via `external_id`
- **Melhor cobertura:** ~1000-1800 parques únicos

**Saída esperada:**

```
📊 Resumo da Ingestão:
   OpenStreetMap: 324 parques inseridos
   Google Places: 1,142 parques inseridos
   Total em parks_raw: 1,466 registros
```

---

### 🧹 FASE 2: Deduplicação (2-5 minutos)

#### Executar Processamento para Master

```powershell
python scripts/process_to_master.py
```

**O que este script faz:**

1. **Normalização de Endereços**

   - Usa biblioteca `usaddress` para parsing
   - Padroniza abreviações (Street → St, Avenue → Ave)
   - Remove caracteres especiais

2. **Blocking Geográfico** (O(n) ao invés de O(n²))

   - Agrupa por ZIP code
   - Cria blocos por proximidade de 500m
   - Reduz 100x o número de comparações

3. **Detecção de Duplicatas**

   - Fuzzy matching com RapidFuzz (>85% similaridade)
   - Validação geográfica (distância < 500m)
   - Considera variações de nome

4. **Consolidação Multi-Fonte**
   - Prioridade: Google Places > OSM > Yelp > Manual
   - Média de coordenadas de todas as fontes
   - Soma de reviews e avaliações
   - Escolhe dados mais completos

**Confirmação interativa:**

```
Encontrados 1,466 registros em parks_raw
Processar todos para parks_master? (s/n):
```

**Saída esperada:**

```
✅ Processamento concluído com sucesso!

📊 Estatísticas:
   Total processado: 1,466 registros
   Registros únicos em parks_master: 1,042
   Taxa de deduplicação: 28.9%
   Registros para revisão manual: 23 (2.2%)
```

**Tipos de registros que precisam revisão:**

- Sem coordenadas geográficas
- Sem endereço completo
- Confidence score < 0.5
- Conflitos entre fontes

---

### 👥 FASE 3: Identificação de Proprietários (Variável)

#### Pré-requisito: Escolher Método

**Opções disponíveis:**

| Método                   | Precisão       | Velocidade | Custo        | Cobertura              |
| ------------------------ | -------------- | ---------- | ------------ | ---------------------- |
| **Generic Web Search**   | Baixa (30-50%) | Rápida     | $0.005/busca | Todos condados         |
| **Fetchers Específicos** | Alta (85-95%)  | Média      | Gratuito     | Condados implementados |

#### Método 1: Generic Web Search (Para começar)

```powershell
python scripts/identify_owners.py
```

**Menu interativo:**

```
Escolha o modo de execução:
1. Processar todos os parques
2. Processar por condado específico
3. Processar apenas N parques (teste)
Digite sua escolha (1/2/3):
```

**Para teste inicial (recomendado):**

```
Digite sua escolha (1/2/3): 3
Quantos parques processar? 10

Processando 10 parques...
[1/10] Marion County - Sunshine MHP... ✅ Proprietário encontrado
[2/10] Lake County - Lakeview Estates... ⚠️ Não encontrado
...
```

**Saída esperada:**

```
📊 Resumo da Identificação:
   Total processado: 10 parques
   Proprietários encontrados: 6 (60%)
   Necessitam revisão manual: 4 (40%)
   Tempo total: 2m 15s
   Tempo médio/parque: 13.5s
```

#### Método 2: Fetchers Específicos (Maior precisão)

**Condados com fetchers implementados:**

- 🔨 Nenhum ainda - use o guia abaixo para implementar

**Para implementar um fetcher específico:**

1. Consulte o guia: `docs/FETCHER_IMPLEMENTATION_GUIDE.md`
2. Crie arquivo em: `src/owners/fetchers/{county_name}_fetcher.py`
3. Herde de `CountyAssessorFetcher`
4. Implemente os métodos abstratos
5. Registre no orchestrator

**Exemplo de implementação:**

```python
# src/owners/fetchers/marion_fetcher.py
from src.owners.base_fetcher import CountyAssessorFetcher, FetchResult

class MarionCountyFetcher(CountyAssessorFetcher):
    """Fetcher para Marion County (Beacon system)"""

    def __init__(self):
        super().__init__("Marion County")
        self.base_url = "https://beacon.schneidercorp.com/Application.aspx?AppID=237"

    def lookup_owner(self, address: str, lat: float, lon: float) -> FetchResult:
        # Implementação específica do Beacon
        ...
```

**Documentação completa:**

- 📘 `docs/PHASE3_OWNER_IDENTIFICATION.md` - Visão geral
- 📗 `docs/FETCHER_IMPLEMENTATION_GUIDE.md` - Guia de implementação
- 📙 `docs/PHASE3_SUMMARY.md` - Resumo executivo

---

### 🧪 TESTES RÁPIDOS

#### Testar Componentes Individuais

```powershell
python scripts/test_phase3.py
```

**Menu de testes:**

```
Escolha o teste:
1. Testar County Mapper (identificação de condado)
2. Testar Generic Fetcher (busca web)
3. Testar Orchestrator completo
4. Executar todos os testes
Digite sua escolha (1/2/3/4):
```

**Teste 1: County Mapper**

```python
# Testa com coordenadas conhecidas
Indianapolis (39.7684, -86.1581) → Marion County ✅
Fort Wayne (41.0793, -85.1394) → Allen County ✅
Evansville (37.9716, -87.5711) → Vanderburgh County ✅
```

**Teste 2: Generic Fetcher**

```python
# Testa busca para um endereço
Address: 123 Main St, Indianapolis, IN
Found: Yes ✅
Owner: JONES FAMILY TRUST
Confidence: 0.65
```

**Teste 3: Orchestrator**

```python
# Pipeline completo de ponta a ponta
Park: Sunshine Mobile Home Park
County: Marion County ✅
Fetcher: GenericWebSearchFetcher
Owner: SUNSHINE PROPERTIES LLC ✅
Saved to database: Yes ✅
```

---

## 📊 CONSULTAS ÚTEIS

### Verificar Progresso

```sql
-- Fase 1: Dados brutos
SELECT COUNT(*) as total_raw FROM parks_raw;
SELECT source, COUNT(*) FROM parks_raw GROUP BY source;

-- Fase 2: Dados consolidados
SELECT COUNT(*) as total_master FROM parks_master;
SELECT COUNT(*) as need_review FROM parks_master WHERE needs_manual_review = TRUE;

-- Fase 3: Proprietários identificados
SELECT COUNT(*) as with_owner FROM parks_master WHERE owner_id IS NOT NULL;
SELECT COUNT(*) as with_company FROM parks_master WHERE company_id IS NOT NULL;

-- Taxa de sucesso Fase 3
SELECT
    COUNT(*) as total,
    SUM(CASE WHEN owner_id IS NOT NULL OR company_id IS NOT NULL THEN 1 ELSE 0 END) as found,
    ROUND(100.0 * SUM(CASE WHEN owner_id IS NOT NULL OR company_id IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as success_rate
FROM parks_master;
```

### Ver Parques sem Proprietário

```sql
SELECT
    master_id,
    name,
    city,
    county,
    latitude,
    longitude
FROM parks_master
WHERE owner_id IS NULL
  AND company_id IS NULL
ORDER BY confidence_score DESC
LIMIT 20;
```

### Ver Proprietários Encontrados

```sql
SELECT
    p.name as park_name,
    p.city,
    p.county,
    o.owner_name_1,
    o.mailing_city,
    o.mailing_state,
    o.confidence_score
FROM parks_master p
JOIN owners o ON p.owner_id = o.id
WHERE o.is_valid_mailing_address = TRUE
ORDER BY o.confidence_score DESC
LIMIT 20;
```

---

## ⚠️ TROUBLESHOOTING

### Problema: "Module not found"

**Solução:**

```powershell
# Reinstalar dependências
pip install -r requirements.txt

# Verificar instalação
pip list | Select-String -Pattern "shapely|geopy|rapidfuzz|usaddress"
```

### Problema: "GeoJSON counties not found"

**Solução:**

```powershell
# Download manual do GeoJSON
python -c "from src.owners.county_mapper import download_indiana_counties_geojson; download_indiana_counties_geojson()"

# Ou use o mock para desenvolvimento
# O sistema usa fallback automático para Geopy
```

### Problema: "Google API quota exceeded"

**Solução:**

```powershell
# Verificar quota atual
python -c "from src.ingestion.google_places import GooglePlacesAPI; api = GooglePlacesAPI('test'); print(api.get_statistics())"

# Aumentar limite no .env
notepad .env
# MAX_API_CALLS_PER_DAY=50000
```

### Problema: "Rate limit exceeded" (Beacon/Schneider)

**Solução:**

```python
# Editar src/owners/base_fetcher.py
# Aumentar delay entre requests
rate_limiter = RateLimiter(requests_per_minute=20)  # Era 30
```

### Problema: "Parcel ID not found"

**Isto é esperado!** Nem todos os condados têm dados públicos online.

**Opções:**

1. Implementar fetcher específico para aquele condado
2. Marcar para revisão manual
3. Usar serviços pagos (DataTree, CoreLogic)

---

## 📈 ESTATÍSTICAS ESPERADAS

### Fase 1: Ingestão

| Fonte          | Parques (Indiana) | Tempo         | Custo      |
| -------------- | ----------------- | ------------- | ---------- |
| OSM            | 200-400           | 2-3 min       | $0         |
| Google Places  | 800-1,500         | 10-15 min     | $16-25     |
| **Combinadas** | **1,000-1,800**   | **15-20 min** | **$16-25** |

### Fase 2: Deduplicação

| Métrica                | Valor Típico |
| ---------------------- | ------------ |
| Taxa de deduplicação   | 25-35%       |
| Registros únicos       | 1,000-1,200  |
| Necessitam revisão     | 2-5%         |
| Tempo de processamento | 2-5 min      |

### Fase 3: Proprietários

| Método                          | Taxa de Sucesso | Tempo/Parque | Precisão   |
| ------------------------------- | --------------- | ------------ | ---------- |
| Generic Web Search              | 30-50%          | 5-15 seg     | Baixa      |
| Fetcher Específico (Beacon)     | 85-95%          | 10-30 seg    | Alta       |
| Fetcher Específico (Custom GIS) | 70-90%          | 15-45 seg    | Média-Alta |

**Projeção para Indiana completo (1,200 parques):**

- Generic: ~450 proprietários (38%), ~3h de processamento
- Fetchers específicos: ~1,050 proprietários (88%), ~6-10h de processamento

---

## 🎯 PRÓXIMOS PASSOS

### Implementar Fetchers Específicos

**Condados prioritários** (maior densidade de parques):

1. **Marion County** (Indianapolis) - Sistema Beacon

   - ~150-200 parques estimados
   - Guia: `docs/FETCHER_IMPLEMENTATION_GUIDE.md` seção Beacon

2. **Lake County** (Gary) - Sistema Beacon

   - ~80-120 parques estimados

3. **Allen County** (Fort Wayne) - Sistema Beacon

   - ~60-100 parques estimados

4. **Hamilton County** - GIS customizado
   - ~40-60 parques estimados

**Total de 4 condados = ~55-60% da cobertura de Indiana!**

### Fase 4: Exportação (Planejada)

- [ ] Exportar CSV para mala direta
- [ ] Gerar relatórios Excel com estatísticas
- [ ] Criar mapas interativos (GeoJSON)
- [ ] API REST para consultas programáticas

---

## 📚 DOCUMENTAÇÃO ADICIONAL

- 📘 [`README.md`](README.md) - Visão geral do projeto
- 📗 [`docs/WORKFLOW.md`](docs/WORKFLOW.md) - Fluxo de trabalho detalhado
- 📙 [`docs/DEDUPLICATION_ALGORITHM.md`](docs/DEDUPLICATION_ALGORITHM.md) - Algoritmo de deduplicação
- 📕 [`docs/PHASE3_OWNER_IDENTIFICATION.md`](docs/PHASE3_OWNER_IDENTIFICATION.md) - Fase 3 completa
- 📓 [`docs/FETCHER_IMPLEMENTATION_GUIDE.md`](docs/FETCHER_IMPLEMENTATION_GUIDE.md) - Como criar fetchers
- 📔 [`docs/PHASE3_SUMMARY.md`](docs/PHASE3_SUMMARY.md) - Resumo executivo Fase 3

---

**Projeto:** MHP Intelligence  
**Fases Implementadas:** 0, 1, 2, 3  
**Status Global:** ✅ 75% COMPLETO  
**Última Atualização:** Dezembro 2025

---

## 🎬 COMEÇANDO AGORA? USE ESTE CHECKLIST

### ✅ Checklist de Execução Rápida

```powershell
# 1. Setup (10 min)
pip install -r requirements.txt
copy .env.example .env
notepad .env  # Adicionar DATABASE_URL
python scripts/create_schema.py

# 2. Ingestão (5 min) - Começar com OSM (gratuito)
python scripts/populate_parks_raw.py
# Escolher opção: 1 (OpenStreetMap)

# 3. Deduplicação (2 min)
python scripts/process_to_master.py

# 4. Proprietários - TESTE (1 min)
python scripts/test_phase3.py
# Escolher opção: 4 (Todos os testes)

# 5. Proprietários - PRODUÇÃO (variável)
python scripts/identify_owners.py
# Escolher opção: 3 (Processar 10 parques como teste)

# 6. Verificar resultados
python -c "from src.database import get_db_session; from sqlalchemy import text; with get_db_session() as db: print(db.execute(text('SELECT COUNT(*) FROM parks_master')).scalar())"
```

### 📊 Verificação de Status Rápida

```sql
-- Conectar ao banco
psql -U postgres -d mhp_intelligence

-- Ver resumo
SELECT
    (SELECT COUNT(*) FROM parks_raw) as dados_brutos,
    (SELECT COUNT(*) FROM parks_master) as dados_limpos,
    (SELECT COUNT(*) FROM parks_master WHERE owner_id IS NOT NULL) as com_proprietario;
```

---

## 📝 Logs

Todos os scripts geram logs em `logs/`:

- `create_schema_{timestamp}.log`
- `osm_ingestion_{timestamp}.log`
- `google_places_{timestamp}.log`
- `populate_db_{timestamp}.log`
- `deduplication_{timestamp}.log`
- `process_master_{timestamp}.log`
- `owner_identification_{timestamp}.log`

**Rotação:** Diária  
**Retenção:** 30 dias  
**Formato:** Texto com timestamps e níveis (INFO, WARNING, ERROR)
