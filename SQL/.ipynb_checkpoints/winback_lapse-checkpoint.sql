DECLARE report_start_date DATE DEFAULT "2020-10-01";
DECLARE report_end_date DATE DEFAULT "2020-10-30";

WITH lapsing_users AS (
    SELECT
        DISTINCT report_date,
        tracking_id,
        most_recent_start
    FROM
        `nbcu-sdp-sandbox-prod.QS_Test.Daily_Last_View_Status`
    WHERE
        report_Date >= '2020-07-29'
        AND ABS(DATE_DIFF(report_Date, most_recent_start, day)) >= 30
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
        video.adobe_date >= DATE_ADD(lapsing_users.most_recent_start, INTERVAL 30 day) -- video viewed after 30 days of inactivity
        AND num_views_started = 1
        AND video.adobe_date BETWEEN report_start_date
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
    COUNT(DISTINCT adobe_tracking_id) AS winbacks,
    COUNT(DISTINCT tracking_id) AS winback_rate_denom,
    COUNT(DISTINCT adobe_tracking_id) / COUNT(DISTINCT tracking_id) AS winback_rate
FROM
    (
        SELECT
            A.aid,
            A.audiencecohort,
            B.tracking_id,
            B.adobe_tracking_id
        FROM
            `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` A
            INNER JOIN lapse_data B ON A.aid = B.tracking_id
        where
            A.signup_date >= "2020-06-21"
    )
GROUP BY
    audiencecohort
