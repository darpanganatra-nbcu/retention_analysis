DECLARE report_start_date DATE DEFAULT "2020-12-01";
DECLARE report_end_date DATE DEFAULT "2020-12-31";

WITH lapsing_users AS (
    SELECT
        DISTINCT report_date,
        tracking_id,
        most_recent_start
    FROM
        `nbcu-sdp-sandbox-prod.QS_Test.Daily_Last_View_Status`
    WHERE
        report_Date >= '2020-07-29'
        AND (
            ABS(DATE_DIFF(report_Date, most_recent_start, day)) >= 15
            AND ABS(DATE_DIFF(report_Date, most_recent_start, day)) <= 29
        )
        AND report_date BETWEEN report_start_date
        and report_end_date
),
viewing AS (
    SELECT
        DISTINCT adobe_tracking_id
    FROM
        `res-nbcupea-dev-ds-sandbox-001.silverTables.SILVER_VIDEO` AS video
        LEFT JOIN lapsing_users ON video.adobe_tracking_id = lapsing_users.tracking_id
    WHERE
        -- you viewed between 15-30 days after your most recent start
        video.adobe_date <= DATE_ADD(lapsing_users.most_recent_start, INTERVAL 29 day) -- video viewed before 30 days of inactivity
        AND video.adobe_date >= DATE_ADD(lapsing_users.most_recent_start, INTERVAL 15 day) -- video viewed after 15 days of inactivity
        AND num_views_started = 1
        AND lapsing_users.most_recent_start BETWEEN report_start_date
        and report_end_date
),
lapse_data AS (
    SELECT
        *
    FROM
        lapsing_users
        LEFT JOIN viewing ON lapsing_users.tracking_id = viewing.adobe_tracking_id
)
SELECT
    audiencecohort,
    tenure_classification,
    COUNT(DISTINCT tracking_id) AS save_rate_denom,
    COUNT(DISTINCT adobe_tracking_id) AS saves,
    COUNT(DISTINCT adobe_tracking_id) / COUNT(DISTINCT tracking_id) AS save_rate
FROM
    (
        SELECT
            A.aid,
            A.audiencecohort,
            A.tenure_classification ,
            B.tracking_id,
            B.adobe_tracking_id
        FROM
            `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_audiencecohort_classification` A
            INNER JOIN lapse_data B ON A.aid = B.tracking_id
        where
        -- Have to add signup date restriction
            A.signup_date >= "2020-06-21"
    )
GROUP BY
    audiencecohort,
    tenure_classification
