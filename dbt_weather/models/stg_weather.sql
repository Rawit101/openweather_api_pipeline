{{ config(materialized='table') }}

SELECT 
    JSONExtractString(raw_json, 'name') AS city_name,
    JSONExtractFloat(raw_json, 'main', 'temp') AS temperature,
    JSONExtractFloat(raw_json, 'main', 'humidity') AS humidity,
    JSONExtractString(raw_json, 'weather', 1, 'main') AS weather_condition,
    toDate(JSONExtractString(raw_json, 'ingestion_date')) AS ingestion_date
FROM s3(
    'http://minio:9000/weather-raw-data/raw/*/*.json', 
    'admin', 
    'password123', 
    'JSONAsString', 
    'raw_json String'
)