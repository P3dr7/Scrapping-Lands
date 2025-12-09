# 🔑 Guia Completo: Como Obter API Keys

## 📋 Índice

1. [Google Places API](#google-places-api) - Para dados de parques (Fase 1)
2. [Google Custom Search API](#google-custom-search-api) - Para buscar proprietários (Fase 3)
3. [Configuração do .env](#configuração-do-env)
4. [Verificação e Testes](#verificação-e-testes)
5. [Gerenciamento de Custos](#gerenciamento-de-custos)

---

## 🗺️ Google Places API

### Para que serve?

- **Fase 1**: Buscar parques de trailers (MHP) e RV parks em Indiana
- **Dados obtidos**: Nome, endereço, coordenadas, telefone, website, avaliações
- **Custo estimado**: $16-25 para cobrir Indiana completo

### Passo a Passo

#### 1. Criar Conta Google Cloud

1. Acesse: [console.cloud.google.com](https://console.cloud.google.com)
2. Clique em **"Get started for free"** (se não tiver conta)
3. Faça login com sua conta Google
4. Aceite os termos de serviço

💡 **Dica**: Google oferece **$300 em créditos grátis** por 90 dias para novos usuários!

---

#### 2. Criar um Projeto

1. No console, clique no **seletor de projetos** (canto superior esquerdo)
2. Clique em **"New Project"**
3. Preencha:
   - **Project name**: `MHP-Intelligence` (ou qualquer nome)
   - **Organization**: Deixe em branco (se não tiver)
4. Clique em **"Create"**
5. Aguarde alguns segundos e selecione o projeto criado

---

#### 3. Habilitar APIs Necessárias

1. No menu lateral, vá em: **APIs & Services** → **Library**
2. Busque e habilite as seguintes APIs:

   **a) Places API (NEW)**

   - Pesquise: `Places API (New)`
   - Clique em **"Enable"**
   - ⚠️ Certifique-se de escolher a versão **NEW**, não a antiga!

   **b) Geocoding API** (opcional, mas recomendado)

   - Pesquise: `Geocoding API`
   - Clique em **"Enable"**

---

#### 4. Criar API Key

1. Vá em: **APIs & Services** → **Credentials**
2. Clique em **"+ CREATE CREDENTIALS"** → **"API key"**
3. Uma janela aparecerá com sua chave. **COPIE IMEDIATAMENTE!**
   ```
   Exemplo: AIzaSyB1234567890abcdefghijklmnopqrstuvw
   ```

---

#### 5. Restringir API Key (Segurança Importante!)

⚠️ **NUNCA** use uma API key sem restrições em produção!

1. Na janela que apareceu após criar a key, clique em **"RESTRICT KEY"**

   Ou vá em: **Credentials** → Clique no ícone de lápis da sua key

2. **API restrictions**:
   - Selecione: **"Restrict key"**
   - Marque apenas:
     - ✅ `Places API (New)`
     - ✅ `Geocoding API` (se habilitou)
3. **Application restrictions** (opcional para desenvolvimento):

   - Para desenvolvimento local: Deixe em **"None"**
   - Para produção: Use **"IP addresses"** e adicione seu servidor

4. Clique em **"Save"**

---

#### 6. Configurar Billing (Obrigatório)

⚠️ **Google exige cartão de crédito**, mas você controla os limites!

1. Vá em: **Billing** → **Link a billing account**
2. Clique em **"Create billing account"**
3. Preencha seus dados:
   - Nome
   - Endereço
   - **Cartão de crédito** (não será cobrado se não ultrapassar $200/mês grátis)
4. Clique em **"Submit and enable billing"**

💰 **Proteções contra custos inesperados**:

1. Vá em: **Billing** → **Budgets & alerts**
2. Clique em **"Create budget"**
3. Configure:
   - **Budget amount**: $50 (ou quanto quiser gastar)
   - **Alert thresholds**: 50%, 75%, 90%, 100%
   - **Email**: Seu email para receber alertas
4. Clique em **"Finish"**

---

#### 7. Configurar Quotas (Proteção Adicional)

Para evitar gastos excessivos, limite as requisições:

1. Vá em: **APIs & Services** → **Places API (New)**
2. Clique na aba **"Quotas & System Limits"**
3. Clique em **"All Quotas"**
4. Procure por: **"Requests per day"**
5. Clique no ícone de lápis e defina:

   ```
   Limite diário: 20,000 requests
   ```

   (Isso custa no máximo ~$340, mas você pode colocar menos)

6. Clique em **"Save"**

---

### ✅ Teste Rápido

Teste se sua API key está funcionando:

```powershell
# Windows PowerShell
$apiKey = "SUA_API_KEY_AQUI"
$url = "https://places.googleapis.com/v1/places:searchNearby"
$headers = @{
    "Content-Type" = "application/json"
    "X-Goog-Api-Key" = $apiKey
    "X-Goog-FieldMask" = "places.displayName,places.location"
}
$body = @{
    locationRestriction = @{
        circle = @{
            center = @{
                latitude = 39.7684
                longitude = -86.1581
            }
            radius = 5000.0
        }
    }
    includedTypes = @("rv_park")
} | ConvertTo-Json -Depth 5

Invoke-RestMethod -Uri $url -Method POST -Headers $headers -Body $body
```

**Resultado esperado**: Lista de RV parks próximos a Indianapolis

---

## 🔍 Google Custom Search API

### Para que serve?

- **Fase 3**: Buscar informações de proprietários via Google
- **GenericWebSearchFetcher**: Busca "County Assessor [endereço]"
- **Custo**: $5 por 1000 queries (100 queries grátis/dia)

### Passo a Passo

#### 1. Habilitar API

1. No mesmo projeto do Google Cloud
2. Vá em: **APIs & Services** → **Library**
3. Pesquise: `Custom Search API`
4. Clique em **"Enable"**

---

#### 2. Criar Custom Search Engine

1. Acesse: [programmablesearchengine.google.com](https://programmablesearchengine.google.com/controlpanel/all)
2. Clique em **"Add"** ou **"Create"**
3. Preencha:

   **Basic tab:**

   - **Name**: `County Assessor Search`
   - **What to search**: Selecione **"Search the entire web"**

   **Sites to search:**

   - Adicione alguns sites de County Assessors para começar:
     ```
     beacon.schneidercorp.com
     *.in.gov
     ```
   - Ou deixe em branco para buscar em toda a web

4. Clique em **"Create"**

---

#### 3. Obter Search Engine ID

1. Após criar, você verá seu **Search Engine ID**
   ```
   Exemplo: 0123456789abcdefg:hijklmnop
   ```
2. **COPIE** este ID!

---

#### 4. Configurar para Buscar Toda a Web

1. Na página do seu Custom Search Engine, clique em **"Edit search engine"**
2. Vá na aba **"Setup"**
3. Em **"Sites to search"**, clique em **"Search the entire web"**
4. Toggle: **ON** (ativado)
5. Remova sites específicos (se tiver adicionado)
6. Clique em **"Update"**

---

#### 5. Obter API Key

Use a **mesma API key** criada anteriormente, mas adicione restrição:

1. Vá em: **APIs & Services** → **Credentials**
2. Edite sua API key
3. Em **API restrictions**, adicione:
   - ✅ `Custom Search API`
4. Clique em **"Save"**

---

### ✅ Teste Rápido

```powershell
$apiKey = "SUA_API_KEY_AQUI"
$searchEngineId = "SEU_SEARCH_ENGINE_ID_AQUI"
$query = "Marion County Assessor 123 Main St Indianapolis"
$url = "https://www.googleapis.com/customsearch/v1?key=$apiKey&cx=$searchEngineId&q=$query"

Invoke-RestMethod -Uri $url
```

**Resultado esperado**: Resultados de busca do Google relacionados ao assessor

---

## 📝 Configuração do .env

Depois de obter todas as keys, configure o arquivo `.env`:

```powershell
# Copiar template
copy .env.example .env

# Editar
notepad .env
```

### Template Completo

```env
# ===========================
# DATABASE
# ===========================
DATABASE_URL=postgresql://user:password@localhost:5432/mhp_intelligence

# Para Supabase (exemplo):
# DATABASE_URL=postgresql://postgres.PROJECT_ID:PASSWORD@aws-0-us-east-1.pooler.supabase.com:6543/postgres

# ===========================
# GOOGLE PLACES API (Fase 1)
# ===========================
GOOGLE_PLACES_API_KEY=AIzaSyB1234567890abcdefghijklmnopqrstuvw

# Rate Limiting
GOOGLE_PLACES_RATE_LIMIT=10
MAX_API_CALLS_PER_DAY=20000

# Cache
CACHE_DIR=data/cache
CACHE_EXPIRY_DAYS=7

# ===========================
# GOOGLE CUSTOM SEARCH (Fase 3)
# ===========================
GOOGLE_CUSTOM_SEARCH_API_KEY=AIzaSyB1234567890abcdefghijklmnopqrstuvw
GOOGLE_CUSTOM_SEARCH_ENGINE_ID=0123456789abcdefg:hijklmnop

# Rate Limiting para Custom Search
CUSTOM_SEARCH_RATE_LIMIT=1
CUSTOM_SEARCH_MAX_DAILY=100

# ===========================
# OPENSTREETMAP (Fase 1)
# ===========================
OSM_RATE_LIMIT_SECONDS=1.0
OSM_USER_AGENT=MHP-Intelligence-Bot/1.0 (contact@example.com)

# ===========================
# LOGGING
# ===========================
LOG_LEVEL=INFO
LOG_DIR=logs
LOG_RETENTION_DAYS=30

# ===========================
# GENERAL
# ===========================
STATE=indiana
ENVIRONMENT=development
```

---

## ✅ Verificação e Testes

### 1. Verificar se .env está correto

```powershell
# Ver conteúdo (cuidado com segurança!)
Get-Content .env

# Verificar variáveis específicas
python -c "import os; from dotenv import load_dotenv; load_dotenv(); print('Places API:', os.getenv('GOOGLE_PLACES_API_KEY')[:20] + '...' if os.getenv('GOOGLE_PLACES_API_KEY') else 'NOT SET')"
```

### 2. Testar Google Places API

```powershell
python -c "from src.ingestion.google_places import GooglePlacesAPI; api = GooglePlacesAPI('test'); print('✅ Google Places API configurada corretamente!' if api.api_key else '❌ API key não encontrada')"
```

### 3. Testar Google Custom Search API

```powershell
python -c "from src.owners.fetchers.generic_fetcher import GenericWebSearchFetcher; fetcher = GenericWebSearchFetcher('Marion County'); print('✅ Custom Search API configurada!' if fetcher.api_key else '❌ API key não encontrada')"
```

### 4. Teste Completo End-to-End

```powershell
# Executar script de teste
python scripts/test_phase3.py

# Escolher opção 2 (Generic Fetcher)
# Se retornar resultados, está tudo OK!
```

---

## 💰 Gerenciamento de Custos

### Preços Atuais (Dezembro 2025)

#### Google Places API

| Operação                  | Custo por 1000 | Uso no Projeto          |
| ------------------------- | -------------- | ----------------------- |
| **Nearby Search**         | $32.00         | ~240 calls para Indiana |
| **Place Details (Basic)** | $17.00         | ~1000-1500 calls        |
| **Total Indiana**         | -              | **$16-25**              |

#### Google Custom Search API

| Plano      | Custo           | Limite          |
| ---------- | --------------- | --------------- |
| **Grátis** | $0              | 100 queries/dia |
| **Pago**   | $5/1000 queries | Ilimitado       |

### Estratégias para Economizar

#### 1. Use Cache Agressivamente

O sistema já implementa cache para Google Places:

```python
# Cache de 7 dias (padrão)
# Reduz custo em 90%+ ao re-executar
```

Para aumentar duração do cache:

```env
# .env
CACHE_EXPIRY_DAYS=30  # Cache por 30 dias
```

#### 2. Comece com OSM (Gratuito)

```powershell
# Fase 1: Só OpenStreetMap (sem custo)
python scripts/populate_parks_raw.py
# Escolha opção: 1

# Depois, se precisar, complemente com Google Places
# Escolha opção: 2
```

#### 3. Use Google Places Apenas para Gaps

```python
# Processar só parques sem dados completos
# (implementação futura)
```

#### 4. Limite Execuções

```env
# .env - Limitar requests diários
MAX_API_CALLS_PER_DAY=5000  # Máximo ~$80/dia
GOOGLE_PLACES_RATE_LIMIT=5  # Mais lento, mas seguro
```

#### 5. Custom Search: Use Limite Grátis

```python
# GenericWebSearchFetcher usa max 100 queries/dia
# Se precisar mais, implemente fetchers específicos (gratuitos!)
```

#### 6. Monitore Gastos

**Dashboard de Custos:**

1. Acesse: [console.cloud.google.com/billing](https://console.cloud.google.com/billing)
2. Clique em **"Cost table"**
3. Filtre por: **"Places API"** e **"Custom Search API"**
4. Veja gastos diários/mensais

**Alertas Automáticos:**

Configure no início (veja seção "Configurar Billing" acima)

---

## 🔒 Segurança das API Keys

### ⚠️ NUNCA faça isso:

❌ Commitar `.env` no Git  
❌ Compartilhar keys publicamente  
❌ Usar keys sem restrições  
❌ Deixar keys em código-fonte  
❌ Postar keys em issues/forums

### ✅ SEMPRE faça isso:

✅ Use `.env` (já está no `.gitignore`)  
✅ Restrinja APIs na Google Cloud Console  
✅ Configure limites de quota  
✅ Use alertas de billing  
✅ Rotacione keys periodicamente (a cada 3-6 meses)

### 🔄 Rotacionar API Keys

Se sua key foi exposta:

1. Vá em: **APIs & Services** → **Credentials**
2. Crie uma **nova API key**
3. Configure restrições na nova key
4. Atualize `.env` com a nova key
5. Teste se tudo funciona
6. **DELETE** a key antiga (clique no ícone de lixeira)

---

## 🆘 Troubleshooting

### Erro: "API key not valid"

**Possíveis causas:**

- Key copiada incorretamente (espaços extras)
- API não está habilitada no projeto
- Restrições muito severas

**Solução:**

1. Verifique se copiou a key completa
2. Vá em **APIs & Services** → **Library** e habilite a API
3. Edite restrições da key (remova temporariamente para testar)

---

### Erro: "This API project is not authorized to use this API"

**Causa:** API não está habilitada no projeto

**Solução:**

```
1. APIs & Services → Library
2. Busque a API (ex: "Places API")
3. Clique em "Enable"
```

---

### Erro: "Quota exceeded"

**Causa:** Ultrapassou limite diário

**Soluções:**

1. Aguarde até meia-noite (Pacific Time) para reset
2. Aumente quota em: **APIs & Services** → **Quotas**
3. Use cache para evitar chamadas duplicadas

---

### Erro: "Billing must be enabled"

**Causa:** Projeto não tem billing configurado

**Solução:**

1. Vá em **Billing** → **Link a billing account**
2. Adicione cartão de crédito
3. Configure limites de budget para segurança

---

### Custom Search retorna poucos resultados

**Causa:** Search Engine configurado para sites específicos

**Solução:**

1. Acesse: [programmablesearchengine.google.com](https://programmablesearchengine.google.com/controlpanel/all)
2. Edite seu Search Engine
3. Ative: **"Search the entire web"**
4. Remova sites específicos da lista

---

## 📚 Recursos Adicionais

### Documentação Oficial

- **Google Places API**: [developers.google.com/maps/documentation/places/web-service](https://developers.google.com/maps/documentation/places/web-service)
- **Custom Search API**: [developers.google.com/custom-search](https://developers.google.com/custom-search)
- **Pricing**: [cloud.google.com/maps-platform/pricing](https://cloud.google.com/maps-platform/pricing)

### Calculadora de Custos

Estime seus gastos: [cloud.google.com/products/calculator](https://cloud.google.com/products/calculator)

Selecione:

- Places API
- Custom Search API
- Insira número estimado de requests

---

## ✅ Checklist Final

Antes de executar o projeto, confirme:

- [ ] Conta Google Cloud criada
- [ ] Projeto criado
- [ ] Places API (New) habilitada
- [ ] Custom Search API habilitada
- [ ] API Key criada e copiada
- [ ] API Key restrita (segurança)
- [ ] Billing configurado com limites
- [ ] Budget alerts configurados
- [ ] Custom Search Engine criado
- [ ] Search Engine ID copiado
- [ ] `.env` configurado com todas as keys
- [ ] Testes de conexão executados ✅

---

**Projeto:** MHP Intelligence  
**Documento:** Guia de API Keys  
**Última Atualização:** Dezembro 2025  
**Autor:** Sistema de Documentação Automatizada

---

## 🎯 Próximo Passo

Agora que tem as API keys configuradas:

```powershell
# Execute o pipeline!
python scripts/populate_parks_raw.py
```

Boa sorte! 🚀
