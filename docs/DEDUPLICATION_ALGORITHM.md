# Algoritmo de Deduplicação e Consolidação

## Visão Geral

Este documento descreve o algoritmo implementado em `src/processing/deduplication.py` para processar dados brutos da tabela `parks_raw` e criar registros únicos e consolidados na tabela `parks_master`.

## Pipeline ETL

```
parks_raw (dados brutos)
    ↓
1. Normalização de Endereços
    ↓
2. Blocking Geográfico
    ↓
3. Detecção de Duplicatas
    ↓
4. Consolidação de Registros
    ↓
parks_master (dados limpos)
```

---

## 1. Normalização de Endereços

### Objetivo

Padronizar endereços para comparação consistente, transformando variações em uma forma canônica.

### Biblioteca Utilizada

- **`usaddress`**: Parser de endereços dos EUA

### Processo

```python
Input:  "123 Main Street, Indianapolis, IN 46204"
Output: "123 main st"

Input:  "123 Main St"
Output: "123 main st"
```

### Componentes Extraídos

- `street_number`: Número da rua
- `street_name`: Nome da rua
- `street_type`: Tipo (St, Ave, Blvd, etc) - normalizado
- `city`: Cidade
- `state`: Estado
- `zip_code`: Código postal

### Normalização de Tipo de Rua

Mapeamento de abreviações comuns:

| Original             | Normalizado |
| -------------------- | ----------- |
| Street, Str, Strt    | st          |
| Avenue, Av, Avn      | ave         |
| Boulevard, Blv, Boul | blvd        |
| Road, Roa            | rd          |
| Drive, Drv, Driv     | dr          |

### Tratamento de Erros

Se `usaddress` falhar ao parsear:

- **Fallback**: Normalização básica (lowercase, substituições comuns)
- **Marca**: `parse_success = False`
- **Preserva**: Endereço original

```python
try:
    parsed = usaddress.tag(address)
    # ... processar
except:
    # Fallback: normalização simples
    normalized = address.lower()
    normalized = re.sub(r'\bstreet\b', 'st', normalized)
    # ...
```

---

## 2. Blocking Geográfico

### Objetivo

Evitar comparação O(n²) de todos os registros. Agrupar registros que têm alta probabilidade de serem duplicatas.

### Complexidade

- **Sem Blocking**: O(n²) - para 1000 registros = 1 milhão de comparações
- **Com Blocking**: O(n) - para 1000 registros ≈ 10,000 comparações (100x mais rápido)

### Estratégia de Blocking

#### Bloco 1: Por ZIP Code

```python
Grupo todos os registros com o mesmo ZIP code
Exemplo: ZIP 46204 → [park1, park2, park5, park12]
```

#### Bloco 2: Por Proximidade Geográfica (500m)

Para registros **sem ZIP code** mas com coordenadas:

```python
Para cada registro não processado:
    1. Encontrar todos os vizinhos em raio de 500m
    2. Criar um bloco com esses vizinhos
    3. Marcar todos como processados
```

### Fórmula de Distância (Haversine)

```python
def calculate_distance_meters(lat1, lon1, lat2, lon2):
    R = 6371000  # Raio da Terra em metros

    Δlat = lat2 - lat1
    Δlon = lon2 - lon1

    a = sin(Δlat/2)² + cos(lat1) × cos(lat2) × sin(Δlon/2)²
    c = 2 × atan2(√a, √(1-a))

    distância = R × c
```

### Resultado

```
Exemplo de Blocos Gerados:
- zip_46204: [15 registros]
- zip_46220: [8 registros]
- geo_cluster_142: [3 registros próximos sem ZIP]
- no_geo_999: [1 registro isolado sem coordenadas]
```

---

## 3. Detecção de Duplicatas

### Critérios de Duplicação

Um par de registros é considerado **duplicata** se:

```
(Similaridade do Nome > 85%)
    E
(Distância Geográfica < 500m OU Similaridade do Endereço > 80%)
```

### Similaridade de Nome

#### Normalização do Nome

Antes de comparar, normaliza-se o nome:

```python
Input:  "The RV Park & Mobile Home Resort"
Steps:
    1. Lowercase: "the rv park & mobile home resort"
    2. Remove stopwords: "park mobile home resort" → ""
    3. Remove pontuação: ""
Output: "" (para comparação)
```

**Stopwords removidas**: rv, park, mobile, home, trailer, campground, resort, the, a, an, and, &

#### Algoritmo de Fuzzy Matching

Usa **RapidFuzz** com `token_sort_ratio`:

```python
from rapidfuzz import fuzz

name1 = "sunset mobile home park"
name2 = "mobile home park sunset"

similarity = fuzz.token_sort_ratio(name1, name2)
# Resultado: 100 (ignora ordem das palavras)
```

**Vantagens do `token_sort_ratio`**:

- Ignora ordem das palavras
- Tokeniza e ordena antes de comparar
- Robusto a variações

### Confirmação Geográfica

#### Caso 1: Ambos têm coordenadas

```python
if distância < 500m:
    confidence = (name_similarity × 0.7) + (distance_score × 0.3)
    is_duplicate = True
else:
    is_duplicate = False
```

#### Caso 2: Sem coordenadas

```python
if addr_similarity > 80%:
    confidence = (name_similarity × 0.6) + (addr_similarity × 0.4)
    is_duplicate = True
```

#### Caso 3: Nome extremamente similar

```python
if name_similarity >= 95%:
    # Muito provável que seja duplicata mesmo sem confirmação geo
    is_duplicate = True
    confidence = name_similarity / 100
```

### Grafo de Duplicatas

```
Dentro de cada bloco, constrói grafo de duplicatas:

Bloco ZIP 46204:
    park1 ─┬─ park2 (85% similar)
           └─ park5 (90% similar)

    park3 ──── park7 (92% similar)

    park4 (isolado)

Resultado:
    Grupo 1: [park1, park2, park5]
    Grupo 2: [park3, park7]
    Grupo 3: [park4]
```

---

## 4. Consolidação de Registros

### Objetivo

Para cada grupo de duplicatas, criar **um único registro master** com os melhores dados de todas as fontes.

### Prioridade de Fontes

```python
SOURCE_PRIORITY = {
    'google_places': 3,  # Maior prioridade
    'osm': 2,
    'yelp': 1,
    'manual': 0
}
```

### Regras de Consolidação

| Campo           | Regra                                     |
| --------------- | ----------------------------------------- |
| **Nome**        | Mais completo (preferir não-abreviado)    |
| **Tipo**        | Mais específico                           |
| **Endereço**    | Mais completo (Google > OSM)              |
| **Coordenadas** | **Média** de todas as fontes válidas      |
| **Telefone**    | Google Places > outros                    |
| **Website**     | Google Places > outros                    |
| **Avaliações**  | **Média** de ratings, **soma** de reviews |

### Exemplo de Consolidação

```python
Grupo de Duplicatas:
    1. OSM:
       - Nome: "Sunset MHP"
       - Coords: (39.123, -86.456)
       - Telefone: null

    2. Google Places:
       - Nome: "Sunset Mobile Home Park"
       - Coords: (39.124, -86.457)
       - Telefone: "(317) 555-1234"
       - Website: "sunsetmhp.com"

    3. Yelp:
       - Nome: "Sunset Mobile Home Park"
       - Rating: 4.2
       - Reviews: 15

Registro Master Consolidado:
    - Nome: "Sunset Mobile Home Park"  (mais completo)
    - Coords: (39.1235, -86.4565)  (média)
    - Telefone: "(317) 555-1234"  (Google)
    - Website: "sunsetmhp.com"  (Google)
    - Rating: 4.2  (Yelp)
    - Reviews: 15  (Yelp)
    - Source IDs: [
        {source: "osm", id: "node_123"},
        {source: "google_places", id: "ChIJ..."},
        {source: "yelp", id: "yelp_456"}
      ]
```

### Cálculo de Confidence Score

```python
confidence_score = (
    (num_sources / 3) × 0.4 +     # Mais fontes = mais confiável
    has_coords × 0.4 +              # Coordenadas = essencial
    has_contact × 0.2               # Contato = útil
)
```

Exemplo:

- 2 fontes, coordenadas, telefone: `(2/3 × 0.4) + (1.0 × 0.4) + (0.5 × 0.2) = 0.77`

### Flags de Qualidade

```json
{
	"num_sources": 2,
	"has_coordinates": true,
	"has_contact_info": true,
	"has_reviews": true
}
```

### Marcação para Revisão Manual

Registros marcados com `needs_manual_review = TRUE` se:

```python
needs_manual_review = (
    not latitude OR
    not address OR
    confidence_score < 0.5
)
```

---

## 5. Inserção em parks_master

### SQL de Inserção

```sql
INSERT INTO parks_master (
    master_id, name, park_type, alternative_names,
    address, city, state, zip_code, county,
    latitude, longitude, geom, location_confidence,
    phone, website, email,
    business_status, avg_rating, total_reviews,
    source_ids, confidence_score, data_quality_flags,
    needs_manual_review
) VALUES (
    uuid, nome, tipo, [nomes_alternativos],
    endereço, cidade, estado, zip, condado,
    lat, lon, ST_SetSRID(ST_MakePoint(lon, lat), 4326),
    confiança_localização,
    telefone, site, email,
    status, rating, reviews,
    source_ids_json, confidence, flags_json,
    needs_review
)
ON CONFLICT (master_id) DO UPDATE SET
    updated_at = CURRENT_TIMESTAMP
```

### Marcação de Processados

```sql
UPDATE parks_raw
SET is_processed = TRUE
WHERE id IN (...)
```

---

## Métricas de Performance

### Complexidade Computacional

| Fase                    | Complexidade | Exemplo (n=1000)   |
| ----------------------- | ------------ | ------------------ |
| Normalização            | O(n)         | 1,000 ops          |
| Blocking                | O(n)         | 1,000 ops          |
| Detecção (com blocking) | O(n × k)     | ~10,000 ops (k≈10) |
| Consolidação            | O(n)         | 1,000 ops          |
| **Total**               | **O(n × k)** | **~13,000 ops**    |

### Métricas Esperadas

Para Indiana (estimado 1000-1500 parques):

| Métrica              | Valor Esperado |
| -------------------- | -------------- |
| Registros brutos     | 1000-1500      |
| Registros master     | 600-900        |
| Taxa de deduplicação | 30-40%         |
| Precisão             | >95%           |
| Recall               | >90%           |
| Tempo de execução    | <2 minutos     |

---

## Limitações e Melhorias Futuras

### Limitações Atuais

1. **Dependência de ZIP code**: Parques na fronteira de ZIP codes podem não ser comparados
2. **Threshold fixo**: 85% de similaridade pode ser muito alto ou baixo
3. **Sem aprendizado**: Não aprende com correções manuais

### Melhorias Planejadas (Fase 3)

1. **Machine Learning**:

   - Treinar modelo de classificação de duplicatas
   - Usar features: distância, similaridade, fonte, etc
   - Aprender thresholds ótimos

2. **Geocoding Reverso**:

   - Validar endereços com serviços de geocoding
   - Corrigir coordenadas imprecisas

3. **Blocking Avançado**:

   - Soundex/Metaphone para nomes
   - Grids geográficos hierárquicos

4. **Interface de Revisão**:
   - Dashboard web para revisar duplicatas
   - Feedback loop para melhorar algoritmo

---

## Uso

### Script de Linha de Comando

```powershell
python scripts/process_to_master.py
```

### Uso Programático

```python
from src.processing.deduplication import process_parks_raw_to_master

master_records = process_parks_raw_to_master()
```

### Logs

Logs detalhados são salvos em:

```
logs/deduplication_{timestamp}.log
logs/process_master_{timestamp}.log
```

---

## Referências

- **RapidFuzz**: https://github.com/maxbachmann/RapidFuzz
- **usaddress**: https://github.com/datamade/usaddress
- **Haversine Formula**: https://en.wikipedia.org/wiki/Haversine_formula
- **Record Linkage**: Fellegi-Sunter Model

---

**Versão**: 1.0  
**Data**: Dezembro 2025  
**Autor**: MHP Intelligence Team
