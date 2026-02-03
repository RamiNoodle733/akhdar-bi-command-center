-- ============================================
-- Akhdar BI Command Center
-- 01_create_schemas.sql - Database Schemas
-- ============================================

-- Raw data layer (source system data as-is)
CREATE SCHEMA IF NOT EXISTS raw;

-- Staging layer (cleaned, typed data)
CREATE SCHEMA IF NOT EXISTS staging;

-- Dimensional model (star schema)
CREATE SCHEMA IF NOT EXISTS warehouse;

-- KPI marts (business metrics)
CREATE SCHEMA IF NOT EXISTS marts;

-- Power BI export views (no PII)
CREATE SCHEMA IF NOT EXISTS powerbi_export;

-- Grant permissions
GRANT ALL PRIVILEGES ON SCHEMA raw TO akhdar;
GRANT ALL PRIVILEGES ON SCHEMA staging TO akhdar;
GRANT ALL PRIVILEGES ON SCHEMA warehouse TO akhdar;
GRANT ALL PRIVILEGES ON SCHEMA marts TO akhdar;
GRANT ALL PRIVILEGES ON SCHEMA powerbi_export TO akhdar;
