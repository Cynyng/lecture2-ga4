-- ============================================================
-- Lecture 2: Intermediate / Advanced BigQuery for GA4
-- Student: Lucynda Young
-- Course: Marketing Analytics
-- Dataset: GA4 Sample Ecommerce
-- ============================================================
-- ------------------------------------------------------------
-- Query 1: Total Users vs New Users using CTE
-- Purpose: Identify how many users visited and how many were new
-- ------------------------------------------------------------

WITH UserInfo AS (
SELECT
  user_pseudo_id,
  MAX(IF(event_name IN ('first_visit', 'first_open'), 1, 0)) AS is_new_user
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201101' AND '20201130'
GROUP BY user_pseudo_id
)

SELECT
COUNT(*) AS total_users,
SUM(is_new_user) AS new_users
FROM UserInfo;

-- Query 2A: Extract Page Location from event_params

SELECT
TIMESTAMP_MICROS(event_timestamp) AS event_time,
(
SELECT value.string_value
FROM UNNEST(event_params)
WHERE key = 'page_location' LIMIT 1
) AS page_location
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE event_name = 'page_view'
AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201202'
LIMIT 50;

SELECT
event_date,
item.item_name,
COUNT(*) AS item_rows
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*` e,
UNNEST(e.items) AS item
WHERE e.event_name = 'purchase'
AND _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
GROUP BY event_date, item.item_name
ORDER BY item_rows DESC
LIMIT 20;

SELECT
event_date,
STRING_AGG(DISTINCT event_name, ', ' ORDER BY event_name) AS events_seen
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201203'
GROUP BY event_date
ORDER BY event_date;

SELECT
event_name,
APPROX_COUNT_DISTINCT(user_pseudo_id) AS approx_users
FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
WHERE _TABLE_SUFFIX BETWEEN '20201201' AND '20201231'
GROUP BY event_name
ORDER BY approx_users DESC
LIMIT 15;