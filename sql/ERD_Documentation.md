# Environmental Site Data Management System - Entity Relationship Diagram

## Overview
This document describes the database schema relationships for the EPA Environmental Site Data Management & QA/QC Automation System. The system uses a two-layer architecture with staging and production schemas to handle 2.5M+ environmental records efficiently.

## Schema Architecture

### Two-Layer Design
```
┌─────────────────┐    ETL Process    ┌─────────────────┐
│  STAGING SCHEMA │ ─────────────────→ │   CORE SCHEMA   │
│                 │                   │                 │
│ • Raw CSV data  │                   │ • Normalized    │
│ • Exact mirror  │                   │ • Validated     │
│ • Temporary     │                   │ • Production    │
└─────────────────┘                   └─────────────────┘
```

## Core Schema Entity Relationships

### Primary Entities and Relationships

```
                    CORE.FACILITIES (Master Entity)
                           │
                           │ (1:M)
                           ▼
                    CORE.PERMITS
                           │
                           │ (1:M)
                           ▼
                 CORE.WATER_MEASUREMENTS
                           
    CORE.FACILITIES ────────────────────────────────┐
           │                                        │
           │ (1:M)                                  │ (1:M)
           ▼                                        ▼
    CORE.AIR_EMISSIONS                    CORE.COMPLIANCE_HISTORY
```

### Detailed Relationship Mapping

#### 1. CORE.FACILITIES (Central Hub)
**Purpose**: Master facility registry combining ECHO and ICIS data
**Primary Key**: `facility_id` (BIGSERIAL)
**Unique Keys**: 
- `registry_id` (links to ECHO_EXPORTER data)
- Links to child tables via `facility_id`

**Relationships**:
- **1:Many** → `core.permits` (via facility_id)
- **1:Many** → `core.air_emissions` (via facility_id) 
- **1:Many** → `core.compliance_history` (via facility_id)

**Key Attributes**:
- Geographic coordinates (latitude/longitude with validation)
- Program participation flags (air, water, waste permits)
- Industry classification (NAICS/SIC codes as arrays)
- Facility characteristics (federal, major, active status)

#### 2. CORE.PERMITS (Water Discharge Permits)
**Purpose**: NPDES water discharge permits and authorizations
**Primary Key**: `permit_id` (BIGSERIAL)
**Foreign Key**: `facility_id` → `core.facilities`
**Unique Keys**: `npdes_id` (links to water measurements)

**Relationships**:
- **Many:1** ← `core.facilities` (via facility_id)
- **1:Many** → `core.water_measurements` (via permit_id)

**Key Attributes**:
- Permit status and dates (effective, expiration, termination)
- Flow limits (design and actual average flow)
- Receiving water body information
- Compliance tracking flags

#### 3. CORE.WATER_MEASUREMENTS (Quarterly Water Quality Data)
**Purpose**: NPDES quarterly compliance and violation tracking
**Primary Key**: `measurement_id` (BIGSERIAL)
**Foreign Key**: `permit_id` → `core.permits`
**Unique Constraint**: `(permit_id, measurement_year, measurement_quarter)`

**Relationships**:
- **Many:1** ← `core.permits` (via permit_id)

**Key Attributes**:
- Quarterly compliance metrics (violations, exceedances)
- HLRNC status (Highest Level of Regulatory Non-Compliance)
- Calculated compliance flags

#### 4. CORE.AIR_EMISSIONS (Annual Emissions Data)
**Purpose**: Air pollutant emissions by facility and year
**Primary Key**: `emission_id` (BIGSERIAL)
**Foreign Key**: `facility_id` → `core.facilities`
**Unique Constraint**: `(facility_id, reporting_year, pollutant_name, program_system_acronym, program_system_id)`

**Relationships**:
- **Many:1** ← `core.facilities` (via facility_id)

**Key Attributes**:
- Annual emission amounts with units
- Pollutant classification (HAP, VOC flags)
- Reporting program details (TRI, NEI, etc.)

#### 5. CORE.COMPLIANCE_HISTORY (Cross-Program Compliance)
**Purpose**: Historical compliance status across all environmental programs
**Primary Key**: `compliance_id` (BIGSERIAL)
**Foreign Key**: `facility_id` → `core.facilities`
**Unique Constraint**: `(facility_id, program_type, compliance_year, compliance_quarter)`

**Relationships**:
- **Many:1** ← `core.facilities` (via facility_id)

**Key Attributes**:
- Program-specific compliance (AIR, WATER, WASTE, etc.)
- Inspection and enforcement history
- Penalty information
- Three-year compliance patterns

## Staging Schema Structure

### Staging Tables (Mirror CSV Structure)
```
staging.echo_facilities      ←── ECHO_EXPORTER.csv (133 columns)
staging.icis_facilities      ←── ICIS_FACILITIES.csv (14 columns)
staging.icis_permits         ←── ICIS_PERMITS.csv (28 columns)
staging.npdes_measurements   ←── NPDES_QNCR_HISTORY.csv (8 columns)
staging.air_emissions       ←── POLL_RPT_COMBINED_EMISSIONS.csv (9 columns)
```

Each staging table includes ETL metadata:
- `etl_batch_id` - Links to ETL run tracking
- `etl_load_date` - Timestamp of data ingestion
- `etl_source_file` - Original CSV filename
- `etl_row_number` - Row position in source file

## Audit and Monitoring Schema

### ETL Tracking and Data Quality
```
core.etl_run_log ────┐
                     │
                     ├─── core.qa_findings
                     │
                     └─── core.data_lineage
```

#### CORE.ETL_RUN_LOG
- Tracks ETL pipeline executions
- Records processing statistics
- Links to QA findings and lineage

#### CORE.QA_FINDINGS  
- Data quality issues by severity
- Business rule violations
- Resolution tracking

#### CORE.DATA_LINEAGE
- Source-to-target transformations
- Audit trail for compliance
- Transformation logic documentation

## Key Linking Strategies

### Primary Linking Keys
1. **REGISTRY_ID**: Links ECHO facilities to air emissions data
2. **NPDES_ID**: Links ICIS facilities/permits to water quality measurements  
3. **FACILITY_UIN**: Alternative facility identifier from ICIS system
4. **FACILITY_ID**: Internal surrogate key for all core relationships

### Data Integration Flow
```
ECHO_EXPORTER ──┐
                ├─── CORE.FACILITIES ──┬─── CORE.AIR_EMISSIONS
ICIS_FACILITIES ─┘                     │
                                       ├─── CORE.COMPLIANCE_HISTORY
ICIS_PERMITS ─────── CORE.PERMITS ─────┤
                           │           │
NPDES_QNCR_HISTORY ────────┴─── CORE.WATER_MEASUREMENTS
```

## Performance Considerations

### Indexing Strategy
- **Primary/Foreign Keys**: Automatic indexes for referential integrity
- **Time-based Queries**: Indexes on date columns with partial indexes for recent data
- **Geographic Queries**: Spatial indexes on coordinates
- **Text Search**: GIN indexes for facility names and full-text search
- **Compliance Queries**: Composite indexes for dashboard queries

### Partitioning Recommendations
For tables with time-series data (>1M records):
- `core.water_measurements`: Partition by measurement_year
- `core.air_emissions`: Partition by reporting_year
- `core.compliance_history`: Partition by compliance_year

### Query Optimization
- Materialized views for common dashboard queries
- Partial indexes for active/recent data
- Expression indexes for calculated fields
- Statistics updates for query planner

## Data Quality and Constraints

### Referential Integrity
- All foreign key relationships enforced
- Cascade rules for data consistency
- Unique constraints on natural keys

### Business Rules
- Geographic coordinates within valid ranges (-90 to 90 lat, -180 to 180 long)
- Dates not in future for historical data
- Numeric constraints for measurements (>= 0)
- Enumerated values for status codes

### Audit Trail
- Automatic timestamps on all core tables
- User tracking for data modifications
- Trigger-based audit logging
- ETL batch tracking for data lineage

## Scalability Features

### Handle 2.5M+ Records
- Efficient indexing strategy
- Partitioning for large tables
- Bulk loading optimizations
- Concurrent index creation

### Support for Growth
- Extensible schema design
- Additional program types via compliance_history
- Flexible JSON fields for run parameters
- Modular ETL architecture

This ERD design supports efficient querying, maintains data integrity, and provides comprehensive audit capabilities for EPA environmental data management at scale.
