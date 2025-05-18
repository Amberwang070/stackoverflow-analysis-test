-- Q2: generate the result that required by the question
{{
    config(
        materialized='table',
        schema='marts'
    )
}}

WITH monthly_bands AS (
    SELECT 
        STRFTIME(DATE_TRUNC('month', question_date), '%Y-%m') as month,
        response_time_band,
        COUNT(*) as question_count
    FROM {{ ref('int_stackoverflow__answer_timing') }}
    GROUP BY 
        DATE_TRUNC('month', question_date),
        response_time_band
)

-- Pivot the data using CASE statements to create columns for each time band
-- Using MAX() because each band will only have one value per month
SELECT 
    month,
    MAX(CASE WHEN response_time_band = 'Very Fast (less than 15 mins)' THEN question_count END) as "Very Fast (<15 mins)",
    MAX(CASE WHEN response_time_band = 'Fast (15 to 60 mins)' THEN question_count END) as "Fast (15-60 mins)",
    MAX(CASE WHEN response_time_band = 'Same Day (1 to 8 hours)' THEN question_count END) as "Same Day (1-8 hours)",
    MAX(CASE WHEN response_time_band = 'Next Day (less than 24 hours)' THEN question_count END) as "Next Day (<24 hours)",
    MAX(CASE WHEN response_time_band = 'Within 5 Days' THEN question_count END) as "Within 5 Days",
    MAX(CASE WHEN response_time_band = 'More than 5 Days' THEN question_count END) as "More than 5 Days"
FROM monthly_bands
GROUP BY month
ORDER BY month DESC