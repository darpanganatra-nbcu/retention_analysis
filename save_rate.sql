WITH
  lapsing_users AS ( -- 0.19556928398576803 8/2, 0.2639318358093471 7/19
  SELECT
    DISTINCT report_date,
    tracking_id,
    most_recent_start
  FROM
    `nbcu-sdp-sandbox-prod.QS_Test.Daily_Last_View_Status`
  WHERE
    report_Date >= '2020-07-29'
    AND ( ABS(DATE_DIFF(report_Date, most_recent_start, day )) >= 15
      AND ABS(DATE_DIFF(report_Date, most_recent_start, day )) <= 30 ) ),
  viewing AS (
  SELECT
    DISTINCT adobe_tracking_id
  FROM
    `res-nbcupea-dev-ds-sandbox-001.silverTables.SILVER_VIDEO` AS video
  LEFT JOIN
    lapsing_users
  ON
    video.adobe_tracking_id = lapsing_users.tracking_id
  WHERE
    -- you viewed between 15-30 days after your most recent start
    video.adobe_date <= DATE_ADD(lapsing_users.most_recent_start, INTERVAL 29 day) -- video viewed before 30 days of inactivity
    AND video.adobe_date >= DATE_ADD(lapsing_users.most_recent_start, INTERVAL 14 day) -- video viewed after 15 days of inactivity
    AND num_views_started = 1 ),

lapse_data AS (
  SELECT
    *
  FROM
    lapsing_users
  LEFT JOIN
    viewing
  ON
    lapsing_users.tracking_id = viewing.adobe_tracking_id )

SELECT
  adopter_group,
  audiencecohort,
  COUNT(DISTINCT tracking_id ) AS save_rate_denom,
  COUNT(DISTINCT adobe_tracking_id) AS saves,
  COUNT(DISTINCT adobe_tracking_id) / COUNT(DISTINCT tracking_id) AS save_rate
FROM
  (SELECT A.aid, A.adopter_group , A.audiencecohort, B.tracking_id , B.adobe_tracking_id  FROM `nbcu-sdp-sandbox-prod.hr_sandbox.all_audiencecohort_table_signup_dates` A INNER JOIN lapse_data B ON A.aid = B.tracking_id where A.signup_date >= "2020-06-21")

  GROUP BY audiencecohort , adopter_group 