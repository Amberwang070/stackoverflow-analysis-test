-- models/analysis/time_band_stats.sql
-- Purpose: Analyze answer time distributions to inform time band decisions
-- This analysis helps determine appropriate time bands for question response times
{{
    config(
        materialized='table',
        schema='analysis'
    )
}}

-- Calculate raw answer times for questions from last 3 years
WITH raw_answer_times AS (
    SELECT 
        -- Calculate time difference between question and answer
        DATEDIFF('minute', q.created_at, a.created_at) as minutes_to_answer,
        q.created_at as question_date
    FROM {{ ref('stg_stackoverflow__posts') }} q
    LEFT JOIN {{ ref('stg_stackoverflow__posts') }} a
        ON q.accepted_answer_id = a.post_id
    WHERE q.post_type_id = 1
        AND a.created_at IS NOT NULL
        -- Get last three years data
        AND q.created_at >= '2020-01-01' 
),

-- Calculate key statistics for answer times
stats AS (
    SELECT 
        COUNT(*) as total_answers,
        MIN(minutes_to_answer) as min_minutes,
        MAX(minutes_to_answer) as max_minutes,
        ROUND(AVG(minutes_to_answer), 2) as avg_minutes,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY minutes_to_answer) as p25,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY minutes_to_answer) as median,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY minutes_to_answer) as p75,
        MIN(question_date) as period_start,
        MAX(question_date) as period_end
    FROM raw_answer_times
)

-- Format results in a readable report format
-- Convert minutes to appropriate time units (minutes/hours/days) based on size
SELECT 
    'Date Range' as metric, 
    'From ' || period_start || ' to ' || period_end as value
FROM stats
UNION ALL
SELECT 
    'Total Answers', 
    total_answers::VARCHAR 
FROM stats
UNION ALL
SELECT 
    'Minimum Time', 
    CASE 
        WHEN min_minutes < 60 THEN min_minutes::VARCHAR || ' minutes'
        WHEN min_minutes < 1440 THEN ROUND(min_minutes/60.0, 1)::VARCHAR || ' hours'
        ELSE ROUND(min_minutes/1440.0, 1)::VARCHAR || ' days'
    END
FROM stats
UNION ALL
SELECT 
    'Maximum Time', 
    CASE 
        WHEN max_minutes < 60 THEN max_minutes::VARCHAR || ' minutes'
        WHEN max_minutes < 1440 THEN ROUND(max_minutes/60.0, 1)::VARCHAR || ' hours'
        ELSE ROUND(max_minutes/1440.0, 1)::VARCHAR || ' days'
    END
FROM stats
UNION ALL
SELECT 
    'Average Time', 
    CASE 
        WHEN avg_minutes < 60 THEN ROUND(avg_minutes, 1)::VARCHAR || ' minutes'
        WHEN avg_minutes < 1440 THEN ROUND(avg_minutes/60.0, 1)::VARCHAR || ' hours'
        ELSE ROUND(avg_minutes/1440.0, 1)::VARCHAR || ' days'
    END
FROM stats
UNION ALL
SELECT 
    '25th Percentile', 
    CASE 
        WHEN p25 < 60 THEN ROUND(p25, 1)::VARCHAR || ' minutes'
        WHEN p25 < 1440 THEN ROUND(p25/60.0, 1)::VARCHAR || ' hours'
        ELSE ROUND(p25/1440.0, 1)::VARCHAR || ' days'
    END
FROM stats
UNION ALL
SELECT 
    'Median Time', 
    CASE 
        WHEN median < 60 THEN ROUND(median, 1)::VARCHAR || ' minutes'
        WHEN median < 1440 THEN ROUND(median/60.0, 1)::VARCHAR || ' hours'
        ELSE ROUND(median/1440.0, 1)::VARCHAR || ' days'
    END
FROM stats
UNION ALL
SELECT 
    '75th Percentile', 
    CASE 
        WHEN p75 < 60 THEN ROUND(p75, 1)::VARCHAR || ' minutes'
        WHEN p75 < 1440 THEN ROUND(p75/60.0, 1)::VARCHAR || ' hours'
        ELSE ROUND(p75/1440.0, 1)::VARCHAR || ' days'
    END
FROM stats