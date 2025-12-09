-- Schema SQL para o projeto MHP Intelligence
-- PostgreSQL 14+ com extensão PostGIS
-- ORDEM: companies → owners → parks_raw → parks_master

-- Ativar extensão PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================================================
-- Tabela 1: companies (PRIMEIRO - sem dependências)
-- Empresas proprietárias de parques (incluindo REITs, grupos de investimento)
-- ============================================================================
CREATE TABLE IF NOT EXISTS companies (
    -- Identificação
    id SERIAL PRIMARY KEY,
    
    -- Informações da empresa
    legal_name VARCHAR(500) NOT NULL,
    dba_name VARCHAR(500),  -- "Doing Business As" / nome fantasia
    trade_names TEXT[],  -- Outros nomes comerciais
    
    -- Tipo de empresa
    company_type VARCHAR(100),  -- 'LLC', 'Corporation', 'REIT', 'Partnership', etc
    entity_type VARCHAR(100),  -- 'private', 'public', 'nonprofit', etc
    
    -- Identificadores legais
    ein VARCHAR(20),  -- Employer Identification Number
    state_registration VARCHAR(100),
    registration_state VARCHAR(2),
    
    -- Endereço corporativo
    corporate_address TEXT,
    corporate_city VARCHAR(200),
    corporate_state VARCHAR(2),
    corporate_zip VARCHAR(10),
    corporate_country VARCHAR(2) DEFAULT 'US',
    
    -- Endereço para correspondência
    mailing_address TEXT,
    mailing_city VARCHAR(200),
    mailing_state VARCHAR(2),
    mailing_zip VARCHAR(10),
    
    -- Contato
    main_phone VARCHAR(50),
    website TEXT,
    email VARCHAR(255),
    
    -- Informações de negócio
    parent_company_id INTEGER REFERENCES companies(id),  -- Para subsidiárias
    total_parks_owned INTEGER DEFAULT 0,
    is_reit BOOLEAN DEFAULT FALSE,
    stock_ticker VARCHAR(10),  -- Se for empresa pública
    
    -- Source tracking
    source VARCHAR(100),
    source_reference TEXT,
    
    -- Auditoria
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_verified_at TIMESTAMP WITH TIME ZONE,
    is_verified BOOLEAN DEFAULT FALSE,
    
    -- Mala direta
    mail_eligible BOOLEAN DEFAULT TRUE,
    do_not_contact BOOLEAN DEFAULT FALSE,
    
    -- Constraints
    CONSTRAINT unique_company_ein UNIQUE (ein)
);

-- Índices para companies
CREATE INDEX IF NOT EXISTS idx_companies_legal_name ON companies(legal_name);
CREATE INDEX IF NOT EXISTS idx_companies_parent ON companies(parent_company_id);
CREATE INDEX IF NOT EXISTS idx_companies_type ON companies(company_type);
CREATE INDEX IF NOT EXISTS idx_companies_mail_eligible ON companies(mail_eligible, do_not_contact);


-- ============================================================================
-- Tabela 2: owners (SEGUNDO - depende de companies)
-- Proprietários individuais dos parques (dados do County Assessor)
-- ============================================================================
CREATE TABLE IF NOT EXISTS owners (
    -- Identificação
    id SERIAL PRIMARY KEY,
    
    -- Informações pessoais (do County Assessor)
    full_name VARCHAR(255) NOT NULL,
    owner_name_2 VARCHAR(255),  -- Co-proprietário (ex: spouse)
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    middle_name VARCHAR(100),
    
    -- Endereço para mala direta (CRITICAL - do County Assessor)
    mailing_address JSONB,  -- Estruturado: {line1, line2, city, state, zip, country}
    
    -- Informações fiscais (County Assessor)
    parcel_ids TEXT[],  -- Múltiplas parcelas podem ter mesmo proprietário
    property_class_codes TEXT[],  -- Códigos de classificação das propriedades
    assessed_values NUMERIC[],  -- Valores avaliados
    tax_years INTEGER[],  -- Anos fiscais correspondentes
    
    -- Dados de qualidade
    confidence_score NUMERIC(3,2) DEFAULT 0.0,  -- 0.0 a 1.0
    needs_manual_review BOOLEAN DEFAULT FALSE,
    manual_review_notes TEXT,
    
    -- Contato
    phone VARCHAR(50),
    email VARCHAR(255),
    
    -- Informações adicionais
    is_individual BOOLEAN DEFAULT TRUE,
    is_commercial_property BOOLEAN DEFAULT FALSE,  -- Se propriedade é comercial
    is_verified BOOLEAN DEFAULT FALSE,  -- Se dados foram verificados manualmente
    
    -- Relacionamento com empresas
    associated_company_id INTEGER REFERENCES companies(id),
    
    -- Source tracking (County Assessor records)
    source VARCHAR(100),  -- 'Marion County Beacon', 'Google Search', etc
    source_url TEXT,  -- URL onde foi encontrado
    county_name VARCHAR(100),  -- Condado onde foi encontrado
    
    -- Metadata (tudo que vier do fetcher)
    metadata JSONB,  -- Dados brutos do County Assessor
    
    -- Auditoria
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_verified_at TIMESTAMP WITH TIME ZONE,
    fetched_at TIMESTAMP WITH TIME ZONE,  -- Quando foi buscado do County Assessor
    
    -- Mala direta
    mail_eligible BOOLEAN DEFAULT TRUE,  -- Se endereço é válido para mailing
    mail_sent_count INTEGER DEFAULT 0,
    last_mail_sent_at TIMESTAMP WITH TIME ZONE,
    do_not_contact BOOLEAN DEFAULT FALSE,
    bounce_count INTEGER DEFAULT 0,  -- Quantas vezes a correspondência voltou
    
    -- Constraints
    CONSTRAINT unique_owner_name_address UNIQUE (full_name, mailing_address)
);

-- Índices para owners
CREATE INDEX IF NOT EXISTS idx_owners_name ON owners(full_name);
CREATE INDEX IF NOT EXISTS idx_owners_company ON owners(associated_company_id);
CREATE INDEX IF NOT EXISTS idx_owners_mail_eligible ON owners(mail_eligible, do_not_contact);
CREATE INDEX IF NOT EXISTS idx_owners_county ON owners(county_name);
CREATE INDEX IF NOT EXISTS idx_owners_confidence ON owners(confidence_score);
CREATE INDEX IF NOT EXISTS idx_owners_needs_review ON owners(needs_manual_review);
CREATE INDEX IF NOT EXISTS idx_owners_mailing_address_gin ON owners USING gin(mailing_address jsonb_path_ops);



-- ============================================================================
-- Tabela 3: parks_raw (TERCEIRO - sem dependências de outras tabelas)
-- Armazena todos os dados brutos coletados de múltiplas fontes
-- ============================================================================
CREATE TABLE IF NOT EXISTS parks_raw (
    -- Identificação
    id SERIAL PRIMARY KEY,
    external_id VARCHAR(255),  -- ID da fonte externa (place_id, OSM node_id, etc)
    source VARCHAR(50) NOT NULL,  -- 'google_places', 'osm', 'yelp', 'manual', etc
    
    -- Informações básicas
    name VARCHAR(500),
    park_type VARCHAR(100),  -- 'mobile_home_park', 'rv_park', 'campground', etc
    
    -- Localização
    address TEXT,
    city VARCHAR(200),
    state VARCHAR(2) DEFAULT 'IN',
    zip_code VARCHAR(10),
    county VARCHAR(100),
    
    -- Coordenadas geográficas (WGS 84 - SRID 4326)
    latitude DECIMAL(10, 7),
    longitude DECIMAL(11, 7),
    geom GEOGRAPHY(POINT, 4326),  -- PostGIS geography type para cálculos precisos
    
    -- Dados de contato
    phone VARCHAR(50),
    website TEXT,
    email VARCHAR(255),
    
    -- Informações operacionais
    business_status VARCHAR(50),  -- 'OPERATIONAL', 'CLOSED_TEMPORARILY', 'CLOSED_PERMANENTLY'
    rating DECIMAL(2, 1),
    total_reviews INTEGER,
    
    -- Metadados brutos (JSON para flexibilidade)
    raw_data JSONB,  -- Armazena resposta completa da API
    tags JSONB,  -- Tags específicas (OSM tags, categorias, etc)
    
    -- Auditoria e controle
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    fetched_at TIMESTAMP WITH TIME ZONE,  -- Quando foi coletado da fonte
    is_processed BOOLEAN DEFAULT FALSE,  -- Se já foi processado para parks_master
    
    -- Constraints
    CONSTRAINT unique_external_source UNIQUE (external_id, source)
);

-- Índices para parks_raw
CREATE INDEX IF NOT EXISTS idx_parks_raw_geom ON parks_raw USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_parks_raw_source ON parks_raw(source);
CREATE INDEX IF NOT EXISTS idx_parks_raw_state_city ON parks_raw(state, city);
CREATE INDEX IF NOT EXISTS idx_parks_raw_processed ON parks_raw(is_processed);
CREATE INDEX IF NOT EXISTS idx_parks_raw_created_at ON parks_raw(created_at);


-- ============================================================================
-- Tabela 4: parks_master (QUARTO - depende de owners e companies)
-- Tabela consolidada com dados deduplicados e enriquecidos
-- ============================================================================
CREATE TABLE IF NOT EXISTS parks_master (
    -- Identificação
    id SERIAL PRIMARY KEY,
    master_id UUID DEFAULT gen_random_uuid() UNIQUE,  -- ID único universal
    
    -- Informações básicas (consolidadas)
    name VARCHAR(500) NOT NULL,
    park_type VARCHAR(100),
    alternative_names TEXT[],  -- Array de nomes alternativos encontrados
    
    -- Localização consolidada
    address TEXT,
    city VARCHAR(200),
    state VARCHAR(2) DEFAULT 'IN',
    zip_code VARCHAR(10),
    county VARCHAR(100),
    
    -- Coordenadas (melhor estimativa)
    latitude DECIMAL(10, 7) NOT NULL,
    longitude DECIMAL(11, 7) NOT NULL,
    geom GEOGRAPHY(POINT, 4326) NOT NULL,
    location_confidence DECIMAL(3, 2),  -- 0.00 a 1.00
    
    -- Dados de contato consolidados
    phone VARCHAR(50),
    website TEXT,
    email VARCHAR(255),
    
    -- Informações operacionais
    business_status VARCHAR(50) DEFAULT 'OPERATIONAL',
    avg_rating DECIMAL(2, 1),
    total_reviews INTEGER DEFAULT 0,
    
    -- Características do parque
    total_lots INTEGER,  -- Número de lotes/espaços
    accepts_rv BOOLEAN,
    accepts_mobile_homes BOOLEAN,
    has_amenities JSONB,  -- {pool, laundry, playground, etc}
    
    -- Proprietário (relacionamento com tabela owners)
    owner_id INTEGER REFERENCES owners(id),
    company_id INTEGER REFERENCES companies(id),
    
    -- Metadados
    source_ids JSONB,  -- Array de {source, external_id} que formaram este master
    confidence_score DECIMAL(3, 2),  -- Score de confiança nos dados (0-1)
    data_quality_flags JSONB,  -- Flags de qualidade dos dados
    
    -- Auditoria
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    last_verified_at TIMESTAMP WITH TIME ZONE,
    needs_manual_review BOOLEAN DEFAULT FALSE,
    
    -- Constraints
    CONSTRAINT valid_location_confidence CHECK (location_confidence >= 0 AND location_confidence <= 1),
    CONSTRAINT valid_confidence_score CHECK (confidence_score >= 0 AND confidence_score <= 1)
);

-- Índices para parks_master
CREATE INDEX IF NOT EXISTS idx_parks_master_geom ON parks_master USING GIST(geom);
CREATE INDEX IF NOT EXISTS idx_parks_master_state_city ON parks_master(state, city);
CREATE INDEX IF NOT EXISTS idx_parks_master_owner ON parks_master(owner_id);
CREATE INDEX IF NOT EXISTS idx_parks_master_company ON parks_master(company_id);
CREATE INDEX IF NOT EXISTS idx_parks_master_manual_review ON parks_master(needs_manual_review);


-- ============================================================================
-- Triggers para atualização automática de updated_at
-- ============================================================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_companies_updated_at BEFORE UPDATE ON companies
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_owners_updated_at BEFORE UPDATE ON owners
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_parks_raw_updated_at BEFORE UPDATE ON parks_raw
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_parks_master_updated_at BEFORE UPDATE ON parks_master
    FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();


-- ============================================================================
-- Comentários nas tabelas
-- ============================================================================
COMMENT ON TABLE companies IS 'Empresas proprietárias de parques';
COMMENT ON TABLE owners IS 'Proprietários individuais para mala direta';
COMMENT ON TABLE parks_raw IS 'Dados brutos de todas as fontes (OSM, Google Places, etc)';
COMMENT ON TABLE parks_master IS 'Dados consolidados e deduplicados de parques';

COMMENT ON COLUMN parks_raw.geom IS 'Geometria PostGIS no formato GEOGRAPHY (WGS 84)';
COMMENT ON COLUMN parks_master.geom IS 'Localização consolidada e verificada';
COMMENT ON COLUMN parks_master.source_ids IS 'JSON com origem dos dados: [{source: "osm", id: "123"}, ...]';