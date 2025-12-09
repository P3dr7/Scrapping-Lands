-- Migração simples para Phase 4
-- Apenas adiciona colunas necessárias

-- Colunas para companies
ALTER TABLE companies ADD COLUMN IF NOT EXISTS registered_agent_name VARCHAR(500);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS registered_agent_address JSONB;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS principals JSONB;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS sos_status VARCHAR(50);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS sos_formation_date VARCHAR(20);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS sos_expiration_date VARCHAR(20);
ALTER TABLE companies ADD COLUMN IF NOT EXISTS sos_raw_data JSONB;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS sos_last_checked_at TIMESTAMP WITH TIME ZONE;

-- Coluna para owners
ALTER TABLE owners ADD COLUMN IF NOT EXISTS sos_lookup_status VARCHAR(20) DEFAULT 'pending';
ALTER TABLE owners ADD COLUMN IF NOT EXISTS company_id INTEGER;

-- Índice para performance
CREATE INDEX IF NOT EXISTS idx_owners_sos_pending ON owners(sos_lookup_status) WHERE sos_lookup_status = 'pending';
CREATE INDEX IF NOT EXISTS idx_companies_sos_status ON companies(sos_status);
