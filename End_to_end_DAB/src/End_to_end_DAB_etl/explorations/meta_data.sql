CREATE OR REPLACE TABLE uat.mukesh.metadata_ingestion (
    entity_name STRING,
    source_path STRING,
    file_format STRING,
    target_table STRING,
    active_flag STRING
);
INSERT INTO uat.mukesh.metadata_ingestion VALUES
('agent',    '/Volumes/uat/mukesh/agent',    'csv', 'bronze_agent',    'Y'),
('customer', '/Volumes/uat/mukesh/customer', 'csv', 'bronze_customer', 'Y'),
('vehicle',  '/Volumes/uat/mukesh/vehicle',  'csv', 'bronze_vehicle',  'Y'),
('policy',   '/Volumes/uat/mukesh/policy',   'csv', 'bronze_policy',   'Y'),
('claim',    '/Volumes/uat/mukesh/claim',    'csv', 'bronze_claim',    'Y');
