-- ============================================================================
-- MIGRAÇÃO 003: Tabela de Contatos (Fase 5)
-- ============================================================================
-- Armazena emails e telefones coletados de múltiplas fontes.
-- ============================================================================

-- Tabela principal de contatos
CREATE TABLE IF NOT EXISTS contacts (
    id SERIAL PRIMARY KEY,
    
    -- Referências (um ou outro, ou nenhum se for contato avulso)
    park_id INTEGER REFERENCES parks_master(id) ON DELETE CASCADE,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    owner_id INTEGER REFERENCES owners(id) ON DELETE CASCADE,
    
    -- Dados de contato
    contact_type VARCHAR(50) NOT NULL,
    -- Tipos: 'park_office', 'registered_agent', 'principal', 'corporate', 'personal', 'general'
    
    email VARCHAR(255),
    email_verified BOOLEAN DEFAULT FALSE,
    email_verification_date TIMESTAMP WITH TIME ZONE,
    
    phone VARCHAR(50),
    phone_type VARCHAR(20),  -- 'mobile', 'landline', 'office', 'fax', 'unknown'
    phone_verified BOOLEAN DEFAULT FALSE,
    
    -- Pessoa associada (se aplicável)
    person_name VARCHAR(255),
    person_title VARCHAR(100),
    
    -- Metadados de origem
    source VARCHAR(100) NOT NULL,
    -- Fontes: 'website_scrape', 'google_places', 'hunter_io', 'apollo', 'manual', 'inbiz'
    source_url TEXT,
    source_reference TEXT,
    
    -- Qualidade
    confidence_level NUMERIC(3,2) DEFAULT 0.5,
    -- 0.0 = baixa confiança, 1.0 = alta confiança
    
    -- Controle de qualidade
    is_valid BOOLEAN DEFAULT TRUE,
    bounce_count INTEGER DEFAULT 0,
    last_bounce_date TIMESTAMP WITH TIME ZONE,
    
    -- Preferências de contato
    do_not_contact BOOLEAN DEFAULT FALSE,
    preferred_contact BOOLEAN DEFAULT FALSE,
    
    -- Timestamps
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    last_used_at TIMESTAMP WITH TIME ZONE,
    
    -- Constraints
    CONSTRAINT valid_contact_type CHECK (
        contact_type IN ('park_office', 'registered_agent', 'principal', 'corporate', 'personal', 'general')
    ),
    CONSTRAINT has_contact_info CHECK (
        email IS NOT NULL OR phone IS NOT NULL
    )
);

-- Índices para performance
CREATE INDEX IF NOT EXISTS idx_contacts_park_id ON contacts(park_id);
CREATE INDEX IF NOT EXISTS idx_contacts_company_id ON contacts(company_id);
CREATE INDEX IF NOT EXISTS idx_contacts_owner_id ON contacts(owner_id);
CREATE INDEX IF NOT EXISTS idx_contacts_email ON contacts(email) WHERE email IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_contacts_source ON contacts(source);
CREATE INDEX IF NOT EXISTS idx_contacts_type ON contacts(contact_type);
CREATE INDEX IF NOT EXISTS idx_contacts_valid ON contacts(is_valid) WHERE is_valid = TRUE;

-- Índice único para evitar duplicatas
CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_email_park 
ON contacts(park_id, LOWER(email)) 
WHERE email IS NOT NULL AND park_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_email_company 
ON contacts(company_id, LOWER(email)) 
WHERE email IS NOT NULL AND company_id IS NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_unique_phone_park 
ON contacts(park_id, phone) 
WHERE phone IS NOT NULL AND park_id IS NOT NULL;

-- Tabela de log de tentativas de scraping
CREATE TABLE IF NOT EXISTS contact_scrape_log (
    id SERIAL PRIMARY KEY,
    park_id INTEGER REFERENCES parks_master(id) ON DELETE CASCADE,
    company_id INTEGER REFERENCES companies(id) ON DELETE CASCADE,
    
    url TEXT NOT NULL,
    scrape_status VARCHAR(20) NOT NULL,
    -- Status: 'success', 'failed', 'blocked', 'timeout', 'no_contacts'
    
    emails_found INTEGER DEFAULT 0,
    phones_found INTEGER DEFAULT 0,
    
    error_message TEXT,
    response_code INTEGER,
    response_time_ms INTEGER,
    
    robots_allowed BOOLEAN,
    
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scrape_log_park ON contact_scrape_log(park_id);
CREATE INDEX IF NOT EXISTS idx_scrape_log_status ON contact_scrape_log(scrape_status);

-- View para contatos válidos e não bloqueados
CREATE OR REPLACE VIEW v_active_contacts AS
SELECT 
    c.*,
    pm.name AS park_name,
    co.legal_name AS company_name
FROM contacts c
LEFT JOIN parks_master pm ON c.park_id = pm.id
LEFT JOIN companies co ON c.company_id = co.id
WHERE c.is_valid = TRUE 
  AND c.do_not_contact = FALSE
  AND (c.bounce_count IS NULL OR c.bounce_count < 3);

-- View para estatísticas de contatos
CREATE OR REPLACE VIEW v_contact_stats AS
SELECT 
    source,
    contact_type,
    COUNT(*) AS total_contacts,
    COUNT(email) AS with_email,
    COUNT(phone) AS with_phone,
    COUNT(*) FILTER (WHERE email_verified = TRUE) AS verified_emails,
    AVG(confidence_level) AS avg_confidence
FROM contacts
WHERE is_valid = TRUE
GROUP BY source, contact_type
ORDER BY source, contact_type;
