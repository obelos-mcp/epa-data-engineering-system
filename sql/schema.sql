-- =====================================================================
-- Environmental Site Data Management & QA/QC Automation System
-- PostgreSQL Database Schema
-- 
-- This schema supports EPA environmental data management with:
-- - Staging layer for raw CSV data ingestion
-- - Core layer for normalized, validated production data
-- - Audit and monitoring capabilities
-- - Support for 2.5M+ records with optimized performance
-- =====================================================================

-- Create schemas
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS core;

-- Enable PostGIS extension for spatial data (if needed)
-- CREATE EXTENSION IF NOT EXISTS postgis;

-- =====================================================================
-- STAGING SCHEMA - Mirror CSV structure exactly for ETL processing
-- =====================================================================

-- Staging table for ECHO_EXPORTER.csv (facility master data)
CREATE TABLE staging.echo_facilities (
    registry_id NUMERIC,
    fac_name TEXT,
    fac_street TEXT,
    fac_city TEXT,
    fac_state TEXT,
    fac_zip TEXT,
    fac_county TEXT,
    fac_fips_code NUMERIC,
    fac_epa_region NUMERIC,
    fac_indian_cntry_flg TEXT,
    fac_federal_flg TEXT,
    fac_us_mex_border_flg TEXT,
    fac_chesapeake_bay_flg TEXT,
    fac_naa_flag TEXT,
    fac_lat NUMERIC,
    fac_long NUMERIC,
    fac_map_icon TEXT,
    fac_collection_method TEXT,
    fac_reference_point TEXT,
    fac_accuracy_meters NUMERIC,
    fac_derived_tribes TEXT,
    fac_derived_huc TEXT,
    fac_derived_wbd TEXT,
    fac_derived_stcty_fips TEXT,
    fac_derived_zip TEXT,
    fac_derived_cd113 TEXT,
    fac_derived_cb2010 TEXT,
    fac_percent_minority NUMERIC,
    fac_pop_den NUMERIC,
    fac_major_flag TEXT,
    fac_active_flag TEXT,
    fac_myrtk_universe TEXT,
    fac_inspection_count INTEGER,
    fac_date_last_inspection DATE,
    fac_days_last_inspection INTEGER,
    fac_informal_count INTEGER,
    fac_date_last_informal_action DATE,
    fac_formal_action_count INTEGER,
    fac_date_last_formal_action DATE,
    fac_total_penalties NUMERIC,
    fac_penalty_count INTEGER,
    fac_date_last_penalty DATE,
    fac_last_penalty_amt NUMERIC,
    fac_qtrs_with_nc NUMERIC,
    fac_programs_with_snc TEXT,
    fac_compliance_status TEXT,
    fac_snc_flg TEXT,
    fac_3yr_compliance_history TEXT,
    air_flag TEXT,
    npdes_flag TEXT,
    sdwis_flag TEXT,
    rcra_flag TEXT,
    tri_flag TEXT,
    ghg_flag TEXT,
    air_ids TEXT,
    caa_permit_types TEXT,
    caa_naics TEXT,
    caa_sics TEXT,
    caa_evaluation_count INTEGER,
    caa_days_last_evaluation INTEGER,
    caa_informal_count INTEGER,
    caa_formal_action_count INTEGER,
    caa_date_last_formal_action DATE,
    caa_penalties NUMERIC,
    caa_last_penalty_date DATE,
    caa_last_penalty_amt NUMERIC,
    caa_qtrs_with_nc NUMERIC,
    caa_compliance_status TEXT,
    caa_hpv_flag TEXT,
    caa_3yr_compl_qtrs_history TEXT,
    npdes_ids TEXT,
    cwa_permit_types TEXT,
    cwa_compliance_tracking TEXT,
    cwa_naics TEXT,
    cwa_sics TEXT,
    cwa_inspection_count INTEGER,
    cwa_days_last_inspection INTEGER,
    cwa_informal_count INTEGER,
    cwa_formal_action_count INTEGER,
    cwa_date_last_formal_action DATE,
    cwa_penalties NUMERIC,
    cwa_last_penalty_date DATE,
    cwa_last_penalty_amt NUMERIC,
    cwa_qtrs_with_nc NUMERIC,
    cwa_compliance_status TEXT,
    cwa_snc_flag TEXT,
    cwa_13qtrs_compl_history TEXT,
    cwa_13qtrs_efflnt_exceedances TEXT,
    cwa_3_yr_qncr_codes TEXT,
    rcra_ids TEXT,
    rcra_permit_types TEXT,
    rcra_naics TEXT,
    rcra_inspection_count INTEGER,
    rcra_days_last_evaluation INTEGER,
    rcra_informal_count INTEGER,
    rcra_formal_action_count INTEGER,
    rcra_date_last_formal_action DATE,
    rcra_penalties NUMERIC,
    rcra_last_penalty_date DATE,
    rcra_last_penalty_amt NUMERIC,
    rcra_qtrs_with_nc NUMERIC,
    rcra_compliance_status TEXT,
    rcra_snc_flag TEXT,
    rcra_3yr_compl_qtrs_history TEXT,
    sdwa_ids TEXT,
    sdwa_system_types TEXT,
    sdwa_informal_count INTEGER,
    sdwa_formal_action_count INTEGER,
    sdwa_compliance_status TEXT,
    sdwa_snc_flag TEXT,
    tri_ids TEXT,
    tri_releases_transfers NUMERIC,
    tri_on_site_releases NUMERIC,
    tri_off_site_transfers NUMERIC,
    tri_reporter_in_past TEXT,
    fec_case_ids TEXT,
    fec_number_of_cases INTEGER,
    fec_last_case_date DATE,
    fec_total_penalties NUMERIC,
    ghg_ids TEXT,
    ghg_co2_releases NUMERIC,
    dfr_url TEXT,
    fac_sic_codes TEXT,
    fac_naics_codes TEXT,
    fac_date_last_inspection_epa DATE,
    fac_date_last_inspection_state DATE,
    fac_date_last_formal_act_epa DATE,
    fac_date_last_formal_act_st DATE,
    fac_date_last_informal_act_epa DATE,
    fac_date_last_informal_act_st DATE,
    fac_federal_agency TEXT,
    tri_reporter TEXT,
    fac_imp_water_flg TEXT,
    -- ETL metadata
    etl_batch_id BIGINT,
    etl_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_source_file TEXT,
    etl_row_number BIGINT
);

-- Staging table for ICIS_FACILITIES.csv (NPDES facility details)
CREATE TABLE staging.icis_facilities (
    icis_facility_interest_id BIGINT,
    npdes_id TEXT,
    facility_uin NUMERIC,
    facility_type_code TEXT,
    facility_name TEXT,
    location_address TEXT,
    supplemental_address_text TEXT,
    city TEXT,
    county_code TEXT,
    state_code TEXT,
    zip TEXT,
    geocode_latitude NUMERIC,
    geocode_longitude NUMERIC,
    impaired_waters TEXT,
    -- ETL metadata
    etl_batch_id BIGINT,
    etl_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_source_file TEXT,
    etl_row_number BIGINT
);

-- Staging table for ICIS_PERMITS.csv (water discharge permits)
CREATE TABLE staging.icis_permits (
    activity_id BIGINT,
    external_permit_nmbr TEXT,
    version_nmbr INTEGER,
    facility_type_indicator TEXT,
    permit_type_code TEXT,
    major_minor_status_flag TEXT,
    permit_status_code TEXT,
    total_design_flow_nmbr NUMERIC,
    actual_average_flow_nmbr NUMERIC,
    state_water_body TEXT,
    state_water_body_name TEXT,
    permit_name TEXT,
    agency_type_code TEXT,
    original_issue_date DATE,
    issue_date DATE,
    issuing_agency TEXT,
    effective_date DATE,
    expiration_date DATE,
    retirement_date DATE,
    termination_date DATE,
    permit_comp_status_flag TEXT,
    dmr_non_receipt_flag TEXT,
    rnc_tracking_flag TEXT,
    master_external_permit_nmbr TEXT,
    tmdl_interface_flag TEXT,
    edmr_authorization_flag TEXT,
    pretreatment_indicator_code TEXT,
    rad_wbd_huc12s TEXT,
    -- ETL metadata
    etl_batch_id BIGINT,
    etl_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_source_file TEXT,
    etl_row_number BIGINT
);

-- Staging table for NPDES_QNCR_HISTORY.csv (water quality measurements)
CREATE TABLE staging.npdes_measurements (
    npdes_id TEXT,
    yearqtr TEXT,
    hlrnc TEXT,
    nume90q INTEGER,
    numcvdt INTEGER,
    numsvcd INTEGER,
    numpsch INTEGER,
    numd8090q INTEGER,
    -- ETL metadata
    etl_batch_id BIGINT,
    etl_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_source_file TEXT,
    etl_row_number BIGINT
);

-- Staging table for POLL_RPT_COMBINED_EMISSIONS.csv (air emissions data)
CREATE TABLE staging.air_emissions (
    reporting_year INTEGER,
    registry_id NUMERIC,
    pgm_sys_acrnm TEXT,
    pgm_sys_id TEXT,
    pollutant_name TEXT,
    annual_emission NUMERIC,
    unit_of_measure TEXT,
    nei_type TEXT,
    nei_hap_voc_flag TEXT,
    -- ETL metadata
    etl_batch_id BIGINT,
    etl_load_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    etl_source_file TEXT,
    etl_row_number BIGINT
);

-- =====================================================================
-- CORE SCHEMA - Normalized production tables with proper relationships
-- =====================================================================

-- Master facilities table combining ECHO and ICIS data
CREATE TABLE core.facilities (
    facility_id BIGSERIAL PRIMARY KEY,
    registry_id NUMERIC UNIQUE, -- From ECHO_EXPORTER
    facility_uin NUMERIC, -- From ICIS_FACILITIES
    facility_name TEXT NOT NULL,
    facility_type_code TEXT,
    -- Address information
    street_address TEXT,
    supplemental_address TEXT,
    city TEXT,
    county TEXT,
    state_code TEXT NOT NULL,
    zip_code TEXT,
    fips_code TEXT,
    epa_region INTEGER CHECK (epa_region BETWEEN 1 AND 10),
    -- Geographic coordinates
    latitude NUMERIC CHECK (latitude BETWEEN -90 AND 90),
    longitude NUMERIC CHECK (longitude BETWEEN -180 AND 180),
    coordinate_accuracy_meters NUMERIC,
    coordinate_collection_method TEXT,
    -- Facility characteristics
    is_federal_facility BOOLEAN DEFAULT FALSE,
    is_major_facility BOOLEAN DEFAULT FALSE,
    is_active BOOLEAN DEFAULT TRUE,
    percent_minority NUMERIC CHECK (percent_minority BETWEEN 0 AND 100),
    population_density NUMERIC CHECK (population_density >= 0),
    -- Program participation flags
    has_air_permits BOOLEAN DEFAULT FALSE,
    has_water_permits BOOLEAN DEFAULT FALSE,
    has_waste_permits BOOLEAN DEFAULT FALSE,
    has_drinking_water_permits BOOLEAN DEFAULT FALSE,
    has_tri_reporting BOOLEAN DEFAULT FALSE,
    has_ghg_reporting BOOLEAN DEFAULT FALSE,
    -- Industry codes
    naics_codes TEXT[], -- Array of NAICS codes
    sic_codes TEXT[], -- Array of SIC codes
    -- Audit fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'etl_system',
    updated_by TEXT DEFAULT 'etl_system'
);

-- Water discharge permits from ICIS system
CREATE TABLE core.permits (
    permit_id BIGSERIAL PRIMARY KEY,
    facility_id BIGINT NOT NULL REFERENCES core.facilities(facility_id),
    npdes_id TEXT UNIQUE, -- Key for linking water quality data
    external_permit_number TEXT NOT NULL,
    permit_version INTEGER DEFAULT 1,
    permit_type_code TEXT NOT NULL,
    permit_status_code TEXT NOT NULL,
    major_minor_status TEXT CHECK (major_minor_status IN ('Major', 'Minor', 'N')),
    -- Flow limits and design
    total_design_flow_mgd NUMERIC CHECK (total_design_flow_mgd >= 0),
    actual_average_flow_mgd NUMERIC CHECK (actual_average_flow_mgd >= 0),
    -- Water body information
    receiving_water_body_code TEXT,
    receiving_water_body_name TEXT,
    watershed_huc12_codes TEXT[],
    -- Permit dates
    original_issue_date DATE,
    current_issue_date DATE,
    effective_date DATE,
    expiration_date DATE CHECK (expiration_date > effective_date),
    retirement_date DATE,
    termination_date DATE,
    -- Agency information
    issuing_agency TEXT,
    agency_type_code TEXT,
    -- Compliance tracking flags
    dmr_non_receipt_flag BOOLEAN DEFAULT FALSE,
    rnc_tracking_flag BOOLEAN DEFAULT FALSE,
    pretreatment_indicator BOOLEAN DEFAULT FALSE,
    -- Audit fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'etl_system',
    updated_by TEXT DEFAULT 'etl_system'
);

-- Water quality measurements and compliance history
CREATE TABLE core.water_measurements (
    measurement_id BIGSERIAL PRIMARY KEY,
    permit_id BIGINT NOT NULL REFERENCES core.permits(permit_id),
    npdes_id TEXT NOT NULL, -- For direct linking to raw data
    measurement_year INTEGER NOT NULL CHECK (measurement_year BETWEEN 1970 AND EXTRACT(YEAR FROM CURRENT_DATE)),
    measurement_quarter INTEGER NOT NULL CHECK (measurement_quarter BETWEEN 1 AND 4),
    -- Compliance metrics from QNCR data
    hlrnc_status TEXT, -- Highest Level of Regulatory Non-Compliance
    effluent_violations_90day INTEGER DEFAULT 0 CHECK (effluent_violations_90day >= 0),
    compliance_violations INTEGER DEFAULT 0 CHECK (compliance_violations >= 0),
    single_event_violations INTEGER DEFAULT 0 CHECK (single_event_violations >= 0),
    schedule_violations INTEGER DEFAULT 0 CHECK (schedule_violations >= 0),
    dmr_violations_80_90day INTEGER DEFAULT 0 CHECK (dmr_violations_80_90day >= 0),
    -- Calculated compliance status
    is_in_compliance BOOLEAN DEFAULT TRUE,
    has_significant_noncompliance BOOLEAN DEFAULT FALSE,
    -- Audit fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'etl_system',
    updated_by TEXT DEFAULT 'etl_system',
    -- Ensure unique measurements per permit/period
    UNIQUE (permit_id, measurement_year, measurement_quarter)
);

-- Air emissions data from facilities
CREATE TABLE core.air_emissions (
    emission_id BIGSERIAL PRIMARY KEY,
    facility_id BIGINT NOT NULL REFERENCES core.facilities(facility_id),
    registry_id NUMERIC NOT NULL, -- For linking back to ECHO data
    reporting_year INTEGER NOT NULL CHECK (reporting_year BETWEEN 1990 AND EXTRACT(YEAR FROM CURRENT_DATE)),
    program_system_acronym TEXT NOT NULL,
    program_system_id TEXT,
    pollutant_name TEXT NOT NULL,
    annual_emission_amount NUMERIC CHECK (annual_emission_amount >= 0),
    emission_unit_of_measure TEXT NOT NULL,
    nei_emission_type TEXT, -- National Emissions Inventory type
    is_hap_pollutant BOOLEAN DEFAULT FALSE, -- Hazardous Air Pollutant
    is_voc_pollutant BOOLEAN DEFAULT FALSE, -- Volatile Organic Compound
    -- Audit fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'etl_system',
    updated_by TEXT DEFAULT 'etl_system',
    -- Ensure unique emissions per facility/year/pollutant/program
    UNIQUE (facility_id, reporting_year, pollutant_name, program_system_acronym, program_system_id)
);

-- Comprehensive compliance history across all programs
CREATE TABLE core.compliance_history (
    compliance_id BIGSERIAL PRIMARY KEY,
    facility_id BIGINT NOT NULL REFERENCES core.facilities(facility_id),
    program_type TEXT NOT NULL CHECK (program_type IN ('AIR', 'WATER', 'WASTE', 'DRINKING_WATER', 'TRI')),
    compliance_year INTEGER NOT NULL CHECK (compliance_year BETWEEN 1970 AND EXTRACT(YEAR FROM CURRENT_DATE)),
    compliance_quarter INTEGER CHECK (compliance_quarter BETWEEN 1 AND 4),
    -- Inspection data
    inspection_count INTEGER DEFAULT 0 CHECK (inspection_count >= 0),
    days_since_last_inspection INTEGER CHECK (days_since_last_inspection >= 0),
    last_inspection_date DATE,
    -- Enforcement actions
    informal_action_count INTEGER DEFAULT 0 CHECK (informal_action_count >= 0),
    formal_action_count INTEGER DEFAULT 0 CHECK (formal_action_count >= 0),
    last_formal_action_date DATE,
    -- Penalties
    total_penalties NUMERIC DEFAULT 0 CHECK (total_penalties >= 0),
    penalty_count INTEGER DEFAULT 0 CHECK (penalty_count >= 0),
    last_penalty_date DATE,
    last_penalty_amount NUMERIC CHECK (last_penalty_amount >= 0),
    -- Compliance status
    quarters_with_noncompliance INTEGER DEFAULT 0 CHECK (quarters_with_noncompliance >= 0),
    has_significant_noncompliance BOOLEAN DEFAULT FALSE,
    overall_compliance_status TEXT,
    three_year_compliance_history TEXT, -- Encoded compliance pattern
    -- Audit fields
    created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'etl_system',
    updated_by TEXT DEFAULT 'etl_system',
    -- Ensure unique compliance records
    UNIQUE (facility_id, program_type, compliance_year, COALESCE(compliance_quarter, 0))
);

-- =====================================================================
-- AUDIT AND MONITORING TABLES
-- =====================================================================

-- ETL pipeline execution tracking
CREATE TABLE core.etl_run_log (
    run_id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT UNIQUE NOT NULL,
    process_name TEXT NOT NULL,
    source_file_name TEXT,
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_time TIMESTAMP,
    status TEXT NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED', 'CANCELLED')),
    records_processed BIGINT DEFAULT 0,
    records_inserted BIGINT DEFAULT 0,
    records_updated BIGINT DEFAULT 0,
    records_failed BIGINT DEFAULT 0,
    error_message TEXT,
    run_parameters JSONB, -- Store run configuration as JSON
    created_by TEXT DEFAULT 'etl_system'
);

-- Data quality findings and issues
CREATE TABLE core.qa_findings (
    finding_id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT REFERENCES core.etl_run_log(batch_id),
    table_name TEXT NOT NULL,
    column_name TEXT,
    finding_type TEXT NOT NULL CHECK (finding_type IN (
        'MISSING_VALUE', 'INVALID_FORMAT', 'OUT_OF_RANGE', 'DUPLICATE_KEY', 
        'REFERENTIAL_INTEGRITY', 'BUSINESS_RULE', 'DATA_ANOMALY'
    )),
    severity TEXT NOT NULL CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL')),
    finding_description TEXT NOT NULL,
    affected_records BIGINT DEFAULT 1,
    sample_values TEXT[], -- Array of example problematic values
    resolution_status TEXT DEFAULT 'OPEN' CHECK (resolution_status IN ('OPEN', 'RESOLVED', 'ACCEPTED')),
    resolution_notes TEXT,
    found_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    resolved_date TIMESTAMP,
    resolved_by TEXT
);

-- Data lineage and transformation tracking
CREATE TABLE core.data_lineage (
    lineage_id BIGSERIAL PRIMARY KEY,
    batch_id BIGINT REFERENCES core.etl_run_log(batch_id),
    source_table TEXT NOT NULL,
    source_column TEXT,
    target_table TEXT NOT NULL,
    target_column TEXT,
    transformation_type TEXT NOT NULL CHECK (transformation_type IN (
        'DIRECT_COPY', 'CALCULATED', 'LOOKUP', 'AGGREGATED', 'DERIVED', 'SPLIT', 'MERGED'
    )),
    transformation_logic TEXT, -- Description or SQL of transformation
    source_record_count BIGINT,
    target_record_count BIGINT,
    transformation_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT DEFAULT 'etl_system'
);

-- =====================================================================
-- COMMENTS FOR DOCUMENTATION
-- =====================================================================

-- Table comments
COMMENT ON SCHEMA staging IS 'Staging schema for raw CSV data ingestion and initial processing';
COMMENT ON SCHEMA core IS 'Core production schema with normalized, validated environmental data';

COMMENT ON TABLE staging.echo_facilities IS 'Raw ECHO facility data with all 133+ columns from EPA ECHO system';
COMMENT ON TABLE staging.icis_facilities IS 'Raw ICIS facility data for NPDES water discharge facilities';
COMMENT ON TABLE staging.icis_permits IS 'Raw ICIS permit data for water discharge permits and authorizations';
COMMENT ON TABLE staging.npdes_measurements IS 'Raw NPDES water quality measurements and compliance data by quarter';
COMMENT ON TABLE staging.air_emissions IS 'Raw air emissions data from various EPA reporting programs';

COMMENT ON TABLE core.facilities IS 'Master facility registry combining ECHO and ICIS data with standardized attributes';
COMMENT ON TABLE core.permits IS 'Water discharge permits with regulatory details and compliance tracking';
COMMENT ON TABLE core.water_measurements IS 'Quarterly water quality measurements and compliance metrics';
COMMENT ON TABLE core.air_emissions IS 'Annual air emissions by facility, pollutant, and reporting program';
COMMENT ON TABLE core.compliance_history IS 'Historical compliance status across all environmental programs';

COMMENT ON TABLE core.etl_run_log IS 'ETL pipeline execution tracking for monitoring and troubleshooting';
COMMENT ON TABLE core.qa_findings IS 'Data quality issues identified during ETL processing';
COMMENT ON TABLE core.data_lineage IS 'Data transformation tracking for audit and compliance purposes';

-- Key column comments
COMMENT ON COLUMN core.facilities.registry_id IS 'EPA Registry ID - primary key for linking to ECHO and emissions data';
COMMENT ON COLUMN core.facilities.facility_uin IS 'Unique Identification Number from ICIS system';
COMMENT ON COLUMN core.permits.npdes_id IS 'NPDES permit identifier for linking water quality measurements';
COMMENT ON COLUMN core.water_measurements.hlrnc_status IS 'Highest Level of Regulatory Non-Compliance status code';
COMMENT ON COLUMN core.air_emissions.is_hap_pollutant IS 'TRUE if pollutant is classified as Hazardous Air Pollutant';
COMMENT ON COLUMN core.compliance_history.three_year_compliance_history IS 'Encoded 3-year compliance pattern (e.g., quarters in/out of compliance)';

-- =====================================================================
-- FUNCTIONS AND TRIGGERS FOR AUDIT TRAIL
-- =====================================================================

-- Function to update the updated_date timestamp
CREATE OR REPLACE FUNCTION core.update_modified_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_date = CURRENT_TIMESTAMP;
    NEW.updated_by = COALESCE(current_setting('application_name', true), 'unknown');
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Add update triggers to core tables
CREATE TRIGGER update_facilities_modtime 
    BEFORE UPDATE ON core.facilities 
    FOR EACH ROW EXECUTE FUNCTION core.update_modified_column();

CREATE TRIGGER update_permits_modtime 
    BEFORE UPDATE ON core.permits 
    FOR EACH ROW EXECUTE FUNCTION core.update_modified_column();

CREATE TRIGGER update_water_measurements_modtime 
    BEFORE UPDATE ON core.water_measurements 
    FOR EACH ROW EXECUTE FUNCTION core.update_modified_column();

CREATE TRIGGER update_air_emissions_modtime 
    BEFORE UPDATE ON core.air_emissions 
    FOR EACH ROW EXECUTE FUNCTION core.update_modified_column();

CREATE TRIGGER update_compliance_history_modtime 
    BEFORE UPDATE ON core.compliance_history 
    FOR EACH ROW EXECUTE FUNCTION core.update_modified_column();

-- =====================================================================
-- INITIAL DATA SETUP
-- =====================================================================

-- Create sequence for ETL batch IDs
CREATE SEQUENCE IF NOT EXISTS core.etl_batch_id_seq START 1;

-- Insert initial ETL run for schema creation
INSERT INTO core.etl_run_log (batch_id, process_name, status, records_processed, created_by)
VALUES (nextval('core.etl_batch_id_seq'), 'SCHEMA_CREATION', 'SUCCESS', 0, 'schema_setup');
