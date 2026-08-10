{{ config(materialized='table') }}

SELECT 
    ingestion_date,
    city_name,
    round(avg(temperature), 2) AS avg_temperature,
    round(avg(humidity), 2) AS avg_humidity,
    any(weather_condition) AS primary_weather
FROM {{ ref('stg_weather') }}
GROUP BY ingestion_date, city_name