{{
    config(
        materialized='table'
    )
}}

SELECT
    'v1' AS version_id,
    TRUE AS is_active,
    CURRENT_TIMESTAMP() AS created_at