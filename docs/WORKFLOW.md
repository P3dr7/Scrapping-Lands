# 🔄 Fluxo de Trabalho - Fases do Projeto

## Visão Geral das Fases

```
FASE 0: Setup
    ↓
FASE 1: Ingestão (parks_raw)
    ↓
FASE 2: Deduplicação (parks_master)
    ↓
FASE 3: Proprietários (owners/companies)
    ↓
FASE 4: Exportação
```

---

## ✅ FASE 0: Configuração Inicial

**Status**: ✅ COMPLETO

### Passos

1. **Instalar dependências**

```powershell
pip install -r requirements.txt
```

2. **Configurar .env**

```powershell
copy .env.example .env
# Editar .env com credenciais do banco
```

3. **Criar schema do banco**

```powershell
python scripts/create_schema.py
```

### Saídas

- ✅ Banco PostgreSQL/PostGIS configurado
- ✅ 4 tabelas criadas: `companies`, `owners`, `parks_raw`, `parks_master`
- ✅ Índices e triggers criados

---

## ✅ FASE 1: Ingestão de Dados Brutos

**Status**: ✅ COMPLETO

### Fontes de Dados

#### OpenStreetMap (Gratuito)

```powershell
python scripts/populate_parks_raw.py
# Escolher opção 1
```

**Características**:

- ✅ Totalmente gratuito
- ✅ Cobertura global
- ✅ Dados abertos
- ⚠️ Pode ter gaps em áreas rurais

**Dados extraídos**:

- Coordenadas geográficas
- Nome do parque
- Endereço (se disponível)
- Tags OSM

#### Google Places API (Pago)

```powershell
python scripts/populate_parks_raw.py
# Escolher opção 2
```

**Características**:

- 💰 Requer API key (custo estimado: $16-25 para Indiana)
- ✅ Cobertura completa
- ✅ Dados enriquecidos (telefone, website, reviews)
- ✅ Sistema de cache (economiza em re-execuções)

**Dados extraídos**:

- Coordenadas precisas
- Nome completo
- Endereço estruturado
- Telefone
- Website
- Avaliações e reviews
- Status operacional

### Saídas

- Registros inseridos em `parks_raw`
- Estatísticas por tipo de parque
- Logs detalhados

**Estimativas para Indiana**:

- OSM: 200-400 parques
- Google Places: 800-1500 parques
- **Total único: 1000-1800 parques**

---

## ✅ FASE 2: Deduplicação e Consolidação

**Status**: ✅ COMPLETO

### Executar Processamento

```powershell
python scripts/process_to_master.py
```

### Algoritmo

#### 1. Normalização de Endereços

- Usa `usaddress` para parsear componentes
- Padroniza abreviações (Street → St, Avenue → Ave)
- Fallback para normalização simples se parsing falhar

**Exemplo**:

```
Input:  "123 Main Street"
Output: "123 main st"

Input:  "123 Main St."
Output: "123 main st"
```

#### 2. Blocking Geográfico

Evita comparação O(n²):

- **Bloco por ZIP code**: Agrupa registros com mesmo CEP
- **Bloco por proximidade**: Raio de 500m para registros sem ZIP
- **Complexidade**: O(n) ao invés de O(n²)

**Economia**:

- Sem blocking: 1000 registros = 1 milhão de comparações
- Com blocking: 1000 registros ≈ 10 mil comparações (100x mais rápido)

#### 3. Detecção de Duplicatas

**Critérios**:

```
É duplicata SE:
    (Similaridade Nome > 85%)
        E
    (Distância < 500m OU Similaridade Endereço > 80%)
```

**Fuzzy Matching**:

- Usa `RapidFuzz` com `token_sort_ratio`
- Ignora ordem das palavras
- Robusto a variações

**Exemplo**:

```python
"Sunset Mobile Home Park" vs "Mobile Home Park Sunset"
→ 100% de similaridade (ignora ordem)
```

#### 4. Consolidação de Dados

**Prioridade de Fontes**:

```
Google Places (3) > OSM (2) > Yelp (1) > Manual (0)
```

**Regras**:
| Campo | Regra |
|-------|-------|
| Nome | Mais completo |
| Coordenadas | **Média** de todas as fontes |
| Telefone | Google Places preferencial |
| Website | Google Places preferencial |
| Avaliações | **Média** de ratings |

**Confidence Score**:

```python
score = (num_fontes/3 × 0.4) + (tem_coords × 0.4) + (tem_contato × 0.2)
```

### Saídas

- Registros consolidados em `parks_master`
- Taxa de deduplicação típica: **30-40%**
- Registros marcados para revisão manual
- Metadata de qualidade e confiança

**Exemplo de Consolidação**:

```
3 registros brutos (OSM + Google + Yelp)
    ↓
1 registro master com:
    - Coordenadas: média das 3 fontes
    - Telefone: do Google Places
    - Nome: versão mais completa
    - Reviews: soma de todas as fontes
```

### Documentação Técnica

Ver algoritmo completo: [`docs/DEDUPLICATION_ALGORITHM.md`](docs/DEDUPLICATION_ALGORITHM.md)

---

## 🔨 FASE 3: Identificação de Proprietários

**Status**: 🔨 EM DESENVOLVIMENTO

### Fontes Planejadas

1. **County Assessor Records** (Registros de propriedade)
2. **Secretary of State** (Registros empresariais)
3. **Web Scraping** (respeitando robots.txt)
4. **APIs públicas** (se disponíveis)

### Processo

1. Para cada parque em `parks_master`:

   - Buscar proprietário em registros públicos
   - Verificar se é pessoa física → `owners`
   - Verificar se é empresa → `companies`
   - Relacionar: `parks_master.owner_id` ou `company_id`

2. Validação de dados:
   - Verificar endereços para mala direta
   - Marcar `mail_eligible = TRUE` se válido
   - Respeitar `do_not_contact` se aplicável

---

## 📤 FASE 4: Exportação

**Status**: 🔜 PLANEJADO

### Exportações Planejadas

1. **CSV para Mala Direta**

   - Nome do proprietário
   - Endereço para correspondência
   - Nome do parque
   - Localização

2. **Excel com Estatísticas**

   - Resumo por condado
   - Tipos de parques
   - Distribuição geográfica

3. **GeoJSON para Mapas**

   - Visualização em ferramentas GIS
   - Clusters geográficos

4. **API REST** (opcional)
   - Consultas programáticas
   - Integração com CRM

---

## 📊 Monitoramento e Logs

Todos os processos geram logs detalhados:

```
logs/
├── create_schema_{time}.log
├── osm_ingestion_{time}.log
├── google_places_{time}.log
├── populate_db_{time}.log
├── deduplication_{time}.log
└── process_master_{time}.log
```

**Retenção**: 30 dias  
**Rotação**: Diária

---

## 🔍 Revisão Manual

### Registros que Precisam Revisão

```sql
SELECT * FROM parks_master
WHERE needs_manual_review = TRUE;
```

**Motivos para Revisão**:

- Sem coordenadas geográficas
- Sem endereço
- Confidence score < 0.5

### Dashboard de Qualidade (Planejado)

```sql
-- Estatísticas de qualidade
SELECT
    COUNT(*) as total,
    AVG(confidence_score) as avg_confidence,
    SUM(CASE WHEN needs_manual_review THEN 1 ELSE 0 END) as needs_review,
    SUM(CASE WHEN latitude IS NOT NULL THEN 1 ELSE 0 END) as with_coords
FROM parks_master;
```

---

## 🚀 Comandos Rápidos

### Setup Completo

```powershell
# 1. Instalar
pip install -r requirements.txt

# 2. Configurar
copy .env.example .env
# Editar .env

# 3. Criar banco
python scripts/create_schema.py
```

### Pipeline Completo

```powershell
# Fase 1: Ingestão
python scripts/populate_parks_raw.py

# Fase 2: Deduplicação
python scripts/process_to_master.py

# Fase 3: (em breve)
# python scripts/identify_owners.py
```

### Verificar Resultados

```powershell
# Conectar ao banco
psql -U postgres -d mhp_intelligence

# Queries úteis
SELECT COUNT(*) FROM parks_raw;
SELECT COUNT(*) FROM parks_master;
SELECT COUNT(*) FROM parks_master WHERE needs_manual_review = TRUE;
```

---

**Última Atualização**: Dezembro 2025  
**Versão**: 1.1 (com deduplicação)
