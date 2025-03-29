{{
  config(
    materialized='view'
  )
}}

SELECT 
  DATE_TRUNC(date, MONTH) AS month,
  team,
  AVG(CASE WHEN own_goal THEN 1 ELSE 0 END) AS n_own_goals_avg,
  SUM(CASE WHEN own_goal THEN 1 ELSE 0 END) AS n_own_goals
FROM {{ source('raw', 'goalscorers') }}
GROUP BY 1, 2