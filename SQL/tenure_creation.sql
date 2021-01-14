CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_audiencecohort_classification` AS (
SELECT
    *,
    CASE
        WHEN DATE_DIFF(CURRENT_DATE(), signup_date, WEEK) = 1 THEN 'FIRST_WEEK'
        WHEN DATE_DIFF(CURRENT_DATE(), signup_date, WEEK) = 2 THEN 'SECOND_WEEK'
        WHEN DATE_DIFF(CURRENT_DATE(), signup_date, WEEK) IN (3,4) THEN 'THIRD_FOURTH_WEEK'
        WHEN DATE_DIFF(CURRENT_DATE(), signup_date, WEEK) >=4 and DATE_DIFF(CURRENT_DATE(), signup_date, WEEK) < 8 THEN 'SECOND_MONTH'
        WHEN DATE_DIFF(CURRENT_DATE(), signup_date, WEEK) >= 8 THEN 'THREE_PLUS_MONTHS'
    END as tenure_classification
FROM
    `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_audiencecohort_table_signup_dates`)