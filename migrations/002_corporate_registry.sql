-- ============================================================================
-- MIGRAÇÃO: Adicionar colunas para Fase 4 - Corporate Registry Enrichment
-- ============================================================================
-- Este script adiciona colunas necessárias para armazenar dados do 
-- Indiana Secretary of State (SOS) e suportar o enriquecimento corporativo.
-- 
-- Execute com: psql -d sua_database -f migrations/002_corporate_registry.sql
-- ============================================================================

-- ============================================================================
-- PARTE 1: Alterações na tabela COMPANIES
-- ============================================================================

-- Adicionar colunas para Registered Agent
ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS registered_agent_name VARCHAR(500);

ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS registered_agent_address JSONB;
-- Estrutura esperada:
-- {
--   "name": "AGENT NAME",
--   "address_line1": "123 MAIN ST",
--   "address_line2": "STE 100",
--   "city": "INDIANAPOLIS",
--   "state": "IN",
--   "zip_code": "46204"
-- }

-- Adicionar coluna para Principals/Officers
ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS principals JSONB;
-- Estrutura esperada:
-- [
--   {"name": "JOHN DOE", "title": "President", "address": "..."},
--   {"name": "JANE SMITH", "title": "Secretary", "address": "..."}
-- ]

-- Adicionar colunas para status do SOS
ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS sos_status VARCHAR(50);
-- Valores: 'Active', 'Inactive', 'Dissolved', 'Revoked', etc.

ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS sos_formation_date VARCHAR(20);

ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS sos_expiration_date VARCHAR(20);

-- Dados raw do SOS para referência
ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS sos_raw_data JSONB;

-- Data da última verificação no SOS
ALTER TABLE companies 
ADD COLUMN IF NOT EXISTS sos_last_checked_at TIMESTAMP WITH TIME ZONE;


-- ============================================================================
-- PARTE 2: Alterações na tabela OWNERS
-- ============================================================================

-- Status da busca no SOS
ALTER TABLE owners 
ADD COLUMN IF NOT EXISTS sos_lookup_status VARCHAR(20) DEFAULT 'pending';
-- Valores: 'pending', 'success', 'not_found', 'failed', 'skipped'

-- Adicionar constraint para valores válidos
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint 
        WHERE conname = 'valid_sos_lookup_status'
    ) THEN
        ALTER TABLE owners
        ADD CONSTRAINT valid_sos_lookup_status 
        CHECK (sos_lookup_status IN ('pending', 'success', 'not_found', 'failed', 'skipped'));
    END IF;
END $$;

-- Adicionar FK para company_id se não existir
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM information_schema.columns 
        WHERE table_name = 'owners' 
        AND column_name = 'company_id'
    ) THEN
        ALTER TABLE owners ADD COLUMN company_id INTEGER REFERENCES companies(id);
    END IF;
END $$;


-- ============================================================================
-- PARTE 3: Índices para performance
-- ============================================================================

-- Índice para busca de owners pendentes
CREATE INDEX IF NOT EXISTS idx_owners_sos_pending 
ON owners(sos_lookup_status) 
WHERE sos_lookup_status = 'pending' OR sos_lookup_status IS NULL;

-- Índice para busca de companies por state_registration
CREATE INDEX IF NOT EXISTS idx_companies_state_reg 
ON companies(state_registration, registration_state);

-- Índice para busca por registered_agent_name
CREATE INDEX IF NOT EXISTS idx_companies_agent_name 
ON companies(registered_agent_name);

-- Índice para companies com principals (busca em JSONB)
CREATE INDEX IF NOT EXISTS idx_companies_principals 
ON companies USING GIN(principals);


-- ============================================================================
-- PARTE 4: Views úteis para análise
-- ============================================================================

-- View de owners com dados de company enriquecidos
CREATE OR REPLACE VIEW v_owners_enriched AS
SELECT 
    o.id AS owner_id,
    o.full_name AS owner_name,
    o.owner_type,
    o.sos_lookup_status,
    c.id AS company_id,
    c.legal_name AS company_name,
    c.company_type,
    c.sos_status AS company_status,
    c.registered_agent_name,
    c.registered_agent_address->>'city' AS agent_city,
    c.registered_agent_address->>'state' AS agent_state,
    c.registered_agent_address->>'zip_code' AS agent_zip,
    jsonb_array_length(COALESCE(c.principals, '[]'::jsonb)) AS num_principals,
    c.sos_formation_date,
    c.created_at AS company_created_at,
    c.last_verified_at AS company_last_verified
FROM owners o
LEFT JOIN companies c ON o.company_id = c.id;


-- View de principals (officers) extraídos das companies
CREATE OR REPLACE VIEW v_company_principals AS
SELECT 
    c.id AS company_id,
    c.legal_name AS company_name,
    c.company_type,
    p->>'name' AS principal_name,
    p->>'title' AS principal_title,
    p->>'address' AS principal_address
FROM companies c
CROSS JOIN LATERAL jsonb_array_elements(COALESCE(c.principals, '[]'::jsonb)) AS p
WHERE c.principals IS NOT NULL AND jsonb_array_length(c.principals) > 0;


-- View de estatísticas de enriquecimento
CREATE OR REPLACE VIEW v_enrichment_stats AS
SELECT 
    'Total Owners' AS metric,
    COUNT(*)::TEXT AS value
FROM owners
UNION ALL
SELECT 
    'Owners Pending SOS',
    COUNT(*)::TEXT
FROM owners 
WHERE sos_lookup_status = 'pending' OR sos_lookup_status IS NULL
UNION ALL
SELECT 
    'Owners SOS Success',
    COUNT(*)::TEXT
FROM owners 
WHERE sos_lookup_status = 'success'
UNION ALL
SELECT 
    'Owners SOS Not Found',
    COUNT(*)::TEXT
FROM owners 
WHERE sos_lookup_status = 'not_found'
UNION ALL
SELECT 
    'Owners Skipped (Individuals)',
    COUNT(*)::TEXT
FROM owners 
WHERE sos_lookup_status = 'skipped'
UNION ALL
SELECT 
    'Total Companies',
    COUNT(*)::TEXT
FROM companies
UNION ALL
SELECT 
    'Companies with Registered Agent',
    COUNT(*)::TEXT
FROM companies 
WHERE registered_agent_name IS NOT NULL
UNION ALL
SELECT 
    'Companies with Principals',
    COUNT(*)::TEXT
FROM companies 
WHERE principals IS NOT NULL AND jsonb_array_length(principals) > 0
UNION ALL
SELECT 
    'Active Companies',
    COUNT(*)::TEXT
FROM companies 
WHERE sos_status = 'Active';


-- ============================================================================
-- PARTE 5: Função para extrair primeiro principal como "pessoa real"
-- ============================================================================

CREATE OR REPLACE FUNCTION get_real_person(company_id_param INTEGER)
RETURNS TABLE(
    person_name VARCHAR,
    person_title VARCHAR,
    source VARCHAR
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        -- Primeiro tenta o registered agent (pessoa física)
        CASE 
            WHEN c.registered_agent_name IS NOT NULL 
                 AND c.registered_agent_name NOT ILIKE '%INC%'
                 AND c.registered_agent_name NOT ILIKE '%LLC%'
                 AND c.registered_agent_name NOT ILIKE '%CORP%'
                 AND c.registered_agent_name NOT ILIKE '%SERVICES%'
            THEN c.registered_agent_name::VARCHAR
            -- Senão, pega o primeiro principal
            ELSE (c.principals->0->>'name')::VARCHAR
        END AS person_name,
        COALESCE(
            (c.principals->0->>'title')::VARCHAR,
            'Registered Agent'::VARCHAR
        ) AS person_title,
        CASE 
            WHEN c.registered_agent_name IS NOT NULL 
                 AND c.registered_agent_name NOT ILIKE '%INC%'
                 AND c.registered_agent_name NOT ILIKE '%LLC%'
            THEN 'registered_agent'::VARCHAR
            ELSE 'principal'::VARCHAR
        END AS source
    FROM companies c
    WHERE c.id = company_id_param;
END;
$$ LANGUAGE plpgsql;


-- ============================================================================
-- MENSAGEM DE CONCLUSÃO
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '============================================================';
    RAISE NOTICE 'Migração 002_corporate_registry concluída com sucesso!';
    RAISE NOTICE '';
    RAISE NOTICE 'Tabelas alteradas:';
    RAISE NOTICE '  - companies: +8 colunas (registered_agent, principals, sos_*)';
    RAISE NOTICE '  - owners: +1 coluna (sos_lookup_status)';
    RAISE NOTICE '';
    RAISE NOTICE 'Views criadas:';
    RAISE NOTICE '  - v_owners_enriched';
    RAISE NOTICE '  - v_company_principals';
    RAISE NOTICE '  - v_enrichment_stats';
    RAISE NOTICE '';
    RAISE NOTICE 'Funções criadas:';
    RAISE NOTICE '  - get_real_person(company_id)';
    RAISE NOTICE '============================================================';
END $$;
