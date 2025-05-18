-- iintermediate for getting the data in the require format
{{
    config(
        materialized='table',
        schema='intermediate'
    )
}}

WITH question_authors AS (
    SELECT 
        post_id,
        user_id as author_id
    FROM {{ ref('stg_stackoverflow__posts') }}
    WHERE post_type_id = 1  -- Questions only
)

SELECT
    c.comment_id,
    c.post_id,
    c.comment_text,
    c.user_id,
    c.created_at as comment_date,
    qa.author_id as question_author_id
FROM {{ ref('stg_stackoverflow__comments') }} c
INNER JOIN question_authors qa 
    ON c.post_id = qa.post_id
WHERE c.comment_text LIKE '@%'  -- Comments with @mentions
  AND c.user_id = qa.author_id  -- Comment made by question author