-- Common Data Warehouse 
CREATE TABLE IF NOT EXISTS datasource (
    id TEXT PRIMARY KEY, -- e.g., 'HMIS', 'PHEM'
    title TEXT,
    url TEXT,
    description TEXT,
    lastupdated TIMESTAMPTZ
);

INSERT INTO datasource (
    id,
    title,
    url,
    description,
    lastupdated
) VALUES (
    'hmis',
    'HMIS DHIS2',
    'https://dhis.moh.gov.et',
    'National Health Management Information System operated by the MOH',
    NOW()
), (
    'phem',
    'PHEM DHIS2',
    'https://dhis.moh.gov.et',
    'Public Health Emergency Management system for disease surveillance and emergency response, operated by EPHI',
    NOW()
);

CREATE TABLE IF NOT EXISTS orgunits (
    id VARCHAR(50),
    code TEXT,
    name TEXT,
    shortname TEXT,
    parentid VARCHAR(50),
    oupath TEXT,
    level INTEGER,
    openingdate DATE,
    closeddate DATE,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP,
    leaf BOOLEAN,
    downloadedat DATE,
    -- Hierarchy Columns (Level 1-8)
    oulevel1id VARCHAR(50), oulevel1name TEXT,
    oulevel2id VARCHAR(50), oulevel2name TEXT,
    oulevel3id VARCHAR(50), oulevel3name TEXT,
    oulevel4id VARCHAR(50), oulevel4name TEXT,
    oulevel5id VARCHAR(50), oulevel5name TEXT,
    oulevel6id VARCHAR(50), oulevel6name TEXT,
    oulevel7id VARCHAR(50), oulevel7name TEXT,
    oulevel8id VARCHAR(50), oulevel8name TEXT,
    datasource_id TEXT REFERENCES datasource(id),
    PRIMARY KEY (id, datasource_id)
);

CREATE TABLE IF NOT EXISTS dataelements (
    id VARCHAR(50),
    code TEXT,
    name TEXT,
    shortname TEXT,
    displayname TEXT,
    formname TEXT,
    description TEXT,
    valuetype TEXT,
    domaintype TEXT,
    aggregationtype TEXT,
    zeroissignificant BOOLEAN,
    categorycombo_id TEXT,   
    categorycombo_name TEXT,
    optionset_id TEXT,
    optionset_name TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    href TEXT,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    datasource_id TEXT REFERENCES datasource(id),
    PRIMARY KEY (id, datasource_id)
);

CREATE TABLE IF NOT EXISTS datasets (
    id VARCHAR(50),
    code TEXT,
    name TEXT,
    shortname TEXT,
    displayname TEXT,
    description TEXT,
    periodtype VARCHAR(50),    
    categorycombo_id VARCHAR(11),
    categorycombo_name TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    href TEXT,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    datasource_id TEXT REFERENCES datasource(id),
    PRIMARY KEY (id, datasource_id)
);

-- HMIS data warehouse

CREATE TABLE IF NOT EXISTS hmis_datavalues (
    orgunit VARCHAR(50),
    dataelement VARCHAR(50),
    period VARCHAR(50),
    categoryoptioncombo VARCHAR(50),
    attributeoptioncombo VARCHAR(50),
    value_string TEXT,
    value_double DOUBLE PRECISION,
    comment TEXT,
    storedby TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    followup BOOLEAN,
    deleted BOOLEAN,
    _ingestedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (orgunit, dataelement, period, categoryoptioncombo, attributeoptioncombo)
);

CREATE INDEX idx_hmis_de_ou_period ON hmis_datavalues (dataelement, orgunit, period);

CREATE TABLE IF NOT EXISTS hmis_dataset_completeness (
    dataset_id VARCHAR(50),
    orgunit_id VARCHAR(50),
    period VARCHAR(50),
    attributeoptioncombo VARCHAR(50),
    storedby TEXT,
    date_submitted TIMESTAMP,
    lastupdated TIMESTAMP,
    completed BOOLEAN DEFAULT TRUE,
    _ingestedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Primary Key to ensure uniqueness per facility/report/month
    PRIMARY KEY (dataset_id, orgunit_id, period, attributeoptioncombo)
);

CREATE TABLE IF NOT EXISTS hmis_reporting_rates (
    dataset_id VARCHAR(50),
    orgunit_id VARCHAR(50),
    period VARCHAR(50),
    expected_reports DOUBLE PRECISION,
    actual_reports DOUBLE PRECISION,
    actual_reports_on_time DOUBLE PRECISION,
    _ingestedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, orgunit_id, period)
);

CREATE TABLE IF NOT EXISTS hmis_periods (
    id VARCHAR(50) PRIMARY KEY, -- e.g., '2021W52'
    start_date DATE,
    end_date DATE,
    period_name TEXT,           -- e.g., 'Week 52 2021-12-27 - 2022-01-02'
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- PHEM data warehouse

CREATE TABLE IF NOT EXISTS phem_datavalues (
    orgunit VARCHAR(50),
    dataelement VARCHAR(50),
    period VARCHAR(50),
    categoryoptioncombo VARCHAR(50),
    attributeoptioncombo VARCHAR(50),
    value_string TEXT,
    value_double DOUBLE PRECISION,
    comment TEXT,
    storedby TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    followup BOOLEAN,
    deleted BOOLEAN,
    _ingestedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (orgunit, dataelement, period, categoryoptioncombo, attributeoptioncombo)
);

CREATE INDEX idx_phem_de_ou_period ON phem_datavalues (dataelement, orgunit, period);

CREATE TABLE IF NOT EXISTS phem_dataset_completeness (
    dataset_id VARCHAR(50),
    orgunit_id VARCHAR(50),
    period VARCHAR(50),
    attributeoptioncombo VARCHAR(50),
    storedby TEXT,
    date_submitted TIMESTAMP,
    lastupdated TIMESTAMP,
    completed BOOLEAN DEFAULT TRUE,
    _ingestedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Primary Key to ensure uniqueness per facility/report/month
    PRIMARY KEY (dataset_id, orgunit_id, period, attributeoptioncombo)
);

CREATE TABLE IF NOT EXISTS phem_reporting_rates (
    dataset_id VARCHAR(50),
    orgunit_id VARCHAR(50),
    period VARCHAR(50),
    expected_reports DOUBLE PRECISION,
    actual_reports DOUBLE PRECISION,
    actual_reports_on_time DOUBLE PRECISION,
    _ingestedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, orgunit_id, period)
);

CREATE TABLE IF NOT EXISTS phem_periods (
    id VARCHAR(50) PRIMARY KEY, -- e.g., '2021W52'
    start_date DATE,
    end_date DATE,
    period_name TEXT,           -- e.g., 'Week 52 2021-12-27 - 2022-01-02'
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- HMIS Staging Tables

CREATE SCHEMA IF NOT EXISTS stg_hmis;

CREATE TABLE IF NOT EXISTS stg_hmis.orgunits (
    id VARCHAR(50) PRIMARY KEY,
    code TEXT,
    name TEXT,
    shortname TEXT,
    leaf BOOLEAN,
    parentid VARCHAR(50),
    path TEXT,
    level INTEGER,
    openingdate DATE,
    closeddate DATE,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    datasource_id TEXT REFERENCES datasource(id)
);

CREATE TABLE IF NOT EXISTS stg_hmis.categorycombos (
    id VARCHAR(50),
    combo_type VARCHAR(20), -- 'attribute' or 'category'
    name TEXT,
    code TEXT,
    shortname TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, combo_type) -- Compound Key allows ID to exist for both types (attributes and categories)
);

CREATE TABLE IF NOT EXISTS stg_hmis.categories (
    id VARCHAR(50),
    combo_type VARCHAR(20),
    name TEXT,
    code TEXT,
    shortname TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, combo_type)
);

CREATE TABLE IF NOT EXISTS stg_hmis.categoryoptions (
    id VARCHAR(50),
    combo_type VARCHAR(20),
    name TEXT,
    code TEXT,
    shortname TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, combo_type)
);

-- Entity table for Category Option Combos
CREATE TABLE IF NOT EXISTS stg_hmis.categoryoptioncombos (
    id VARCHAR(50) PRIMARY KEY,
    name TEXT,
    code TEXT,
    categorycombo_id VARCHAR(50),
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Relationships
CREATE TABLE IF NOT EXISTS stg_hmis.categorycombo_categories (
    categorycombo_id VARCHAR(50),
    category_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (categorycombo_id, category_id)
);

CREATE TABLE IF NOT EXISTS stg_hmis.category_categoryoptions (
    category_id VARCHAR(50),
    categoryoption_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category_id, categoryoption_id)
);

-- Bridge table for members (categoryOptionCombo_id -> categoryOption_id)
CREATE TABLE IF NOT EXISTS stg_hmis.categoryoptioncombo_options (
    categoryoptioncombo_id VARCHAR(50),
    categoryoption_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (categoryoptioncombo_id, categoryoption_id)
);

-- For build_category_dimension
CREATE TABLE IF NOT EXISTS stg_hmis.category_name_map (
    original_name TEXT PRIMARY KEY,
    safe_name TEXT,
    _builtat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- For build_attribute_dimension
CREATE TABLE IF NOT EXISTS stg_hmis.attribute_name_map (
    original_name TEXT PRIMARY KEY,
    safe_name TEXT,
    _builtat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS stg_hmis.dataset_orgunits (
    dataset_id VARCHAR(50),
    orgunit_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, orgunit_id)
);


CREATE TABLE IF NOT EXISTS stg_hmis.dataset_dataelements (
    dataset_id VARCHAR(50),
    dataelement_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, dataelement_id)
);


-- PHEM Staging Tables

CREATE SCHEMA IF NOT EXISTS stg_phem;

CREATE TABLE IF NOT EXISTS stg_phem.orgunits (
    id VARCHAR(50) PRIMARY KEY,
    code TEXT,
    name TEXT,
    shortname TEXT,
    leaf BOOLEAN,
    parentid VARCHAR(50),
    path TEXT,
    level INTEGER,
    openingdate DATE,
    closeddate DATE,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    datasource_id TEXT REFERENCES datasource(id)
);

CREATE TABLE IF NOT EXISTS stg_phem.categorycombos (
    id VARCHAR(50),
    combo_type VARCHAR(20), -- 'attribute' or 'category'
    name TEXT,
    code TEXT,
    shortname TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, combo_type) -- Compound Key allows ID to exist for both types (attributes and categories)
);

CREATE TABLE IF NOT EXISTS stg_phem.categories (
    id VARCHAR(50),
    combo_type VARCHAR(20),
    name TEXT,
    code TEXT,
    shortname TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, combo_type)
);

CREATE TABLE IF NOT EXISTS stg_phem.categoryoptions (
    id VARCHAR(50),
    combo_type VARCHAR(20),
    name TEXT,
    code TEXT,
    shortname TEXT,
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (id, combo_type)
);

-- Entity table for Category Option Combos
CREATE TABLE IF NOT EXISTS stg_phem.categoryoptioncombos (
    id VARCHAR(50) PRIMARY KEY,
    name TEXT,
    code TEXT,
    categorycombo_id VARCHAR(50),
    created TIMESTAMP,
    lastupdated TIMESTAMP,
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Relationships
CREATE TABLE IF NOT EXISTS stg_phem.categorycombo_categories (
    categorycombo_id VARCHAR(50),
    category_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (categorycombo_id, category_id)
);

CREATE TABLE IF NOT EXISTS stg_phem.category_categoryoptions (
    category_id VARCHAR(50),
    categoryoption_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (category_id, categoryoption_id)
);

-- Bridge table for members (categoryOptionCombo_id -> categoryOption_id)
CREATE TABLE IF NOT EXISTS stg_phem.categoryoptioncombo_options (
    categoryoptioncombo_id VARCHAR(50),
    categoryoption_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (categoryoptioncombo_id, categoryoption_id)
);

-- For build_category_dimension
CREATE TABLE IF NOT EXISTS stg_phem.category_name_map (
    original_name TEXT PRIMARY KEY,
    safe_name TEXT,
    _builtat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- For build_attribute_dimension
CREATE TABLE IF NOT EXISTS stg_phem.attribute_name_map (
    original_name TEXT PRIMARY KEY,
    safe_name TEXT,
    _builtat TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


CREATE TABLE IF NOT EXISTS stg_phem.dataset_orgunits (
    dataset_id VARCHAR(50),
    orgunit_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, orgunit_id)
);


CREATE TABLE IF NOT EXISTS stg_phem.dataset_dataelements (
    dataset_id VARCHAR(50),
    dataelement_id VARCHAR(50),
    _fetchedat TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (dataset_id, dataelement_id)
);