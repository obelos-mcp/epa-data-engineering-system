-- =====================================================================
-- Environmental Site Data Management & QA/QC Automation System
-- Performance Optimization Indexes
-- 
-- This file contains all indexes for optimizing query performance
-- across staging and core schemas for environmental data management.
-- =====================================================================

-- =====================================================================
-- STAGING SCHEMA INDEXES - For ETL processing efficiency
-- =====================================================================

-- ECHO Facilities staging indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_echo_registry_id 
    ON staging.echo_facilities(registry_id) WHERE registry_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_echo_npdes_ids 
    ON staging.echo_facilities USING gin(string_to_array(npdes_ids, ',')) 
    WHERE npdes_ids IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_echo_state 
    ON staging.echo_facilities(fac_state) WHERE fac_state IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_echo_coordinates 
    ON staging.echo_facilities(fac_lat, fac_long) 
    WHERE fac_lat IS NOT NULL AND fac_long IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_echo_etl_batch 
    ON staging.echo_facilities(etl_batch_id, etl_load_date);

-- ICIS Facilities staging indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_icis_fac_npdes_id 
    ON staging.icis_facilities(npdes_id) WHERE npdes_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_icis_fac_uin 
    ON staging.icis_facilities(facility_uin) WHERE facility_uin IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_icis_fac_state 
    ON staging.icis_facilities(state_code) WHERE state_code IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_icis_fac_etl_batch 
    ON staging.icis_facilities(etl_batch_id, etl_load_date);

-- ICIS Permits staging indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_icis_permit_external 
    ON staging.icis_permits(external_permit_nmbr) WHERE external_permit_nmbr IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_icis_permit_dates 
    ON staging.icis_permits(effective_date, expiration_date) 
    WHERE effective_date IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_icis_permit_status 
    ON staging.icis_permits(permit_status_code) WHERE permit_status_code IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_icis_permit_etl_batch 
    ON staging.icis_permits(etl_batch_id, etl_load_date);

-- NPDES Measurements staging indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_npdes_meas_npdes_id 
    ON staging.npdes_measurements(npdes_id) WHERE npdes_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_npdes_meas_yearqtr 
    ON staging.npdes_measurements(yearqtr) WHERE yearqtr IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_npdes_meas_etl_batch 
    ON staging.npdes_measurements(etl_batch_id, etl_load_date);

-- Air Emissions staging indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_air_registry_id 
    ON staging.air_emissions(registry_id) WHERE registry_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_air_year 
    ON staging.air_emissions(reporting_year) WHERE reporting_year IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_air_pollutant 
    ON staging.air_emissions(pollutant_name) WHERE pollutant_name IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_staging_air_etl_batch 
    ON staging.air_emissions(etl_batch_id, etl_load_date);

-- =====================================================================
-- CORE SCHEMA INDEXES - For production query performance
-- =====================================================================

-- Facilities table indexes
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_registry_id 
    ON core.facilities(registry_id) WHERE registry_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_uin 
    ON core.facilities(facility_uin) WHERE facility_uin IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_name 
    ON core.facilities USING gin(to_tsvector('english', facility_name));

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_location 
    ON core.facilities(state_code, city, county);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_coordinates 
    ON core.facilities(latitude, longitude) 
    WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

-- Spatial index for geographic queries (if PostGIS is enabled)
-- CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_geom 
--     ON core.facilities USING GIST(ST_Point(longitude, latitude)) 
--     WHERE latitude IS NOT NULL AND longitude IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_programs 
    ON core.facilities(has_air_permits, has_water_permits, has_waste_permits, has_tri_reporting);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_active 
    ON core.facilities(is_active, is_major_facility);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_naics 
    ON core.facilities USING gin(naics_codes) WHERE naics_codes IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_updated 
    ON core.facilities(updated_date);

-- Permits table indexes
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_npdes_id 
    ON core.permits(npdes_id) WHERE npdes_id IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_facility_id 
    ON core.permits(facility_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_external_number 
    ON core.permits(external_permit_number);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_status 
    ON core.permits(permit_status_code, major_minor_status);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_dates 
    ON core.permits(effective_date, expiration_date) 
    WHERE effective_date IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_active 
    ON core.permits(effective_date, expiration_date, permit_status_code) 
    WHERE permit_status_code IN ('ADC', 'EFF') -- Active permits
    AND effective_date <= CURRENT_DATE 
    AND (expiration_date IS NULL OR expiration_date >= CURRENT_DATE);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_water_body 
    ON core.permits(receiving_water_body_code) WHERE receiving_water_body_code IS NOT NULL;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_updated 
    ON core.permits(updated_date);

-- Water Measurements table indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_permit_id 
    ON core.water_measurements(permit_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_npdes_id 
    ON core.water_measurements(npdes_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_time_period 
    ON core.water_measurements(measurement_year, measurement_quarter);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_recent 
    ON core.water_measurements(measurement_year DESC, measurement_quarter DESC) 
    WHERE measurement_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 5; -- Last 5 years

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_compliance 
    ON core.water_measurements(is_in_compliance, has_significant_noncompliance);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_violations 
    ON core.water_measurements(effluent_violations_90day, compliance_violations, schedule_violations) 
    WHERE effluent_violations_90day > 0 OR compliance_violations > 0 OR schedule_violations > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_updated 
    ON core.water_measurements(updated_date);

-- Composite index for common dashboard queries (permit compliance over time)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_dashboard 
    ON core.water_measurements(permit_id, measurement_year, is_in_compliance);

-- Air Emissions table indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_facility_id 
    ON core.air_emissions(facility_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_registry_id 
    ON core.air_emissions(registry_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_year 
    ON core.air_emissions(reporting_year);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_recent 
    ON core.air_emissions(reporting_year DESC) 
    WHERE reporting_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 10; -- Last 10 years

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_pollutant 
    ON core.air_emissions(pollutant_name);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_hap 
    ON core.air_emissions(is_hap_pollutant) WHERE is_hap_pollutant = TRUE;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_amount 
    ON core.air_emissions(annual_emission_amount) 
    WHERE annual_emission_amount > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_program 
    ON core.air_emissions(program_system_acronym, program_system_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_updated 
    ON core.air_emissions(updated_date);

-- Composite index for emissions trending analysis
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_emissions_trending 
    ON core.air_emissions(facility_id, pollutant_name, reporting_year);

-- Compliance History table indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_facility_id 
    ON core.compliance_history(facility_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_program 
    ON core.compliance_history(program_type);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_time_period 
    ON core.compliance_history(compliance_year, compliance_quarter);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_recent 
    ON core.compliance_history(compliance_year DESC, compliance_quarter DESC) 
    WHERE compliance_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 3; -- Last 3 years

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_noncompliance 
    ON core.compliance_history(has_significant_noncompliance) 
    WHERE has_significant_noncompliance = TRUE;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_penalties 
    ON core.compliance_history(total_penalties) 
    WHERE total_penalties > 0;

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_inspections 
    ON core.compliance_history(last_inspection_date, inspection_count);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_updated 
    ON core.compliance_history(updated_date);

-- Composite index for facility compliance dashboard
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_compliance_dashboard 
    ON core.compliance_history(facility_id, program_type, compliance_year, has_significant_noncompliance);

-- =====================================================================
-- AUDIT AND MONITORING INDEXES
-- =====================================================================

-- ETL Run Log indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_etl_batch_id 
    ON core.etl_run_log(batch_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_etl_process_time 
    ON core.etl_run_log(process_name, start_time);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_etl_status 
    ON core.etl_run_log(status, start_time);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_etl_recent 
    ON core.etl_run_log(start_time DESC) 
    WHERE start_time >= CURRENT_DATE - INTERVAL '30 days';

-- QA Findings indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_qa_batch_id 
    ON core.qa_findings(batch_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_qa_table_column 
    ON core.qa_findings(table_name, column_name);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_qa_type_severity 
    ON core.qa_findings(finding_type, severity);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_qa_unresolved 
    ON core.qa_findings(resolution_status, found_date) 
    WHERE resolution_status = 'OPEN';

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_qa_recent 
    ON core.qa_findings(found_date DESC) 
    WHERE found_date >= CURRENT_DATE - INTERVAL '90 days';

-- Data Lineage indexes
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_lineage_batch_id 
    ON core.data_lineage(batch_id);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_lineage_source_target 
    ON core.data_lineage(source_table, target_table);

CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_lineage_transformation 
    ON core.data_lineage(transformation_type, transformation_date);

-- =====================================================================
-- PARTIAL INDEXES FOR COMMON QUERY PATTERNS
-- =====================================================================

-- Active facilities with recent data
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_active_recent 
    ON core.facilities(facility_id) 
    WHERE is_active = TRUE 
    AND updated_date >= CURRENT_DATE - INTERVAL '1 year';

-- Major facilities with violations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_major_facilities_violations 
    ON core.facilities(facility_id, state_code) 
    WHERE is_major_facility = TRUE;

-- Recent water measurements with violations
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_recent_violations 
    ON core.water_measurements(permit_id, measurement_year, measurement_quarter) 
    WHERE measurement_year >= EXTRACT(YEAR FROM CURRENT_DATE) - 2 
    AND (effluent_violations_90day > 0 OR compliance_violations > 0);

-- High-emission facilities (top 10% by emissions)
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_air_high_emissions 
    ON core.air_emissions(facility_id, pollutant_name, annual_emission_amount) 
    WHERE annual_emission_amount > 1000; -- Adjust threshold based on data analysis

-- =====================================================================
-- EXPRESSION INDEXES FOR CALCULATED FIELDS
-- =====================================================================

-- Index for full facility address search
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_facilities_full_address 
    ON core.facilities USING gin(to_tsvector('english', 
        COALESCE(facility_name, '') || ' ' || 
        COALESCE(street_address, '') || ' ' || 
        COALESCE(city, '') || ' ' || 
        COALESCE(state_code, '')));

-- Index for permit age calculation
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_permits_age 
    ON core.permits((CURRENT_DATE - effective_date)) 
    WHERE effective_date IS NOT NULL;

-- Index for quarterly date extraction from NPDES measurements
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_core_water_meas_date_parts 
    ON core.water_measurements(measurement_year, measurement_quarter, 
        (measurement_year::text || 'Q' || measurement_quarter::text));

-- =====================================================================
-- STATISTICS AND MAINTENANCE
-- =====================================================================

-- Update table statistics for query planner
ANALYZE staging.echo_facilities;
ANALYZE staging.icis_facilities;
ANALYZE staging.icis_permits;
ANALYZE staging.npdes_measurements;
ANALYZE staging.air_emissions;

ANALYZE core.facilities;
ANALYZE core.permits;
ANALYZE core.water_measurements;
ANALYZE core.air_emissions;
ANALYZE core.compliance_history;

-- =====================================================================
-- INDEX MAINTENANCE FUNCTIONS
-- =====================================================================

-- Function to rebuild indexes concurrently
CREATE OR REPLACE FUNCTION core.rebuild_indexes_concurrently(schema_name TEXT DEFAULT 'core')
RETURNS TEXT AS $$
DECLARE
    index_record RECORD;
    result_text TEXT := '';
BEGIN
    FOR index_record IN 
        SELECT schemaname, tablename, indexname 
        FROM pg_indexes 
        WHERE schemaname = schema_name
        AND indexname LIKE 'idx_%'
    LOOP
        BEGIN
            EXECUTE 'REINDEX INDEX CONCURRENTLY ' || quote_ident(index_record.schemaname) || '.' || quote_ident(index_record.indexname);
            result_text := result_text || 'Rebuilt: ' || index_record.indexname || E'\n';
        EXCEPTION WHEN OTHERS THEN
            result_text := result_text || 'Failed to rebuild: ' || index_record.indexname || ' - ' || SQLERRM || E'\n';
        END;
    END LOOP;
    
    RETURN result_text;
END;
$$ LANGUAGE plpgsql;

-- Function to get index usage statistics
CREATE OR REPLACE FUNCTION core.get_index_usage_stats(schema_name TEXT DEFAULT 'core')
RETURNS TABLE(
    table_name TEXT,
    index_name TEXT,
    index_scans BIGINT,
    tuples_read BIGINT,
    tuples_fetched BIGINT
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        t.relname::TEXT as table_name,
        i.relname::TEXT as index_name,
        s.idx_scan as index_scans,
        s.idx_tup_read as tuples_read,
        s.idx_tup_fetch as tuples_fetched
    FROM pg_stat_user_indexes s
    JOIN pg_class i ON i.oid = s.indexrelid
    JOIN pg_class t ON t.oid = s.relid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = schema_name
    ORDER BY s.idx_scan DESC;
END;
$$ LANGUAGE plpgsql;

-- Comments for documentation
COMMENT ON FUNCTION core.rebuild_indexes_concurrently IS 'Rebuilds all indexes in specified schema concurrently to minimize locking';
COMMENT ON FUNCTION core.get_index_usage_stats IS 'Returns index usage statistics to help identify unused or underutilized indexes';
