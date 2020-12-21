/**************************************************************************************************
Calculate each campaign's targetable group and holdout group avg. viewing stats per tenure/adopter group, and per all viewing and promoted program viewing.

Stats returned per cohort are: return rate, avg starts, avg distinct titles, avg hours watched, avg completion rate.

For EARLY adopters, analyze their viewing from campaign start date + 7 day attribution window (4/29 to 5/6).
For LATE adopters v2, analyze their viewing from AFTER signup date + 7 day attribution window

  >> code points to sandbox for: the all audience cohort user table created at end of "Make Targetable Groups"
   (`all_audiencecohort_table_signup_dates` table  with adobe tracking ids (aids), signup date, and audiencecohort labels)
 
  >> code returns table with all campaign cohort summary stats: conversion rates, avg starts, avg titles, avg hours watched, completion rate            
***************************************************************************************************/

DECLARE campaign_start_period date; -- start of campaign for the start date of the viewing analysis for 'early' adopters 
DECLARE analysis_window int64; -- num. days in attribution window
DECLARE program_list ARRAY<STRING>; -- analyze viewing activity for a select list of promoted programs

SET campaign_start_period = '2020-07-29';
SET analysis_window = 7; 
/**
if given a list of promoted programs, can create a downstream lens to measure viewing activity just for promoted titles
**/
SET program_list = ['Brave New World', 'Intelligence', 'The Capture']; 


/*************************************************************************************************************
 Grab the total viewing data of early adopters in each audience cohorts from campaign start day to +7 days after they signed up.
 :: total viewing for all programs (not just promoted programs )
***************************************************************************************************************/
WITH early_adopter_total_viewing AS (
SELECT
aid,
audiencecohort,
distinct_titles,
content_starts,
content_completed,
total_hours
FROM (SELECT  
      aid,
      audiencecohort, 
      count(DISTINCT new_program) AS distinct_titles,
      sum(num_views_started) AS content_starts,
      sum(num_views_completed) AS content_completed,
      sum(num_seconds_played_no_ads)/3600 as total_hours
      
      FROM (
              (SELECT * ,  case when lower(consumption_type) like '%shortform%' then 'shortform' else program end as new_program 
              FROM
             `res-nbcupea-dev-ds-sandbox-001.silverTables.SILVER_VIDEO`
               WHERE 
              num_views_started = 1  
             )  AS video
             INNER JOIN 
             (
             SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` 
               WHERE 
             adopter_group like '%pre%' and signup_date >= '2020-06-21' 
             ) AS usertable 
             ON 
             video.adobe_tracking_id = usertable.aid 
             AND 
             video.adobe_timestamp >= datetime(campaign_start_period) -- 4/29
             AND 
             video.adobe_timestamp < datetime_add(datetime(campaign_start_period), INTERVAL analysis_window day) -- analyze 4/29 to 5/5
             ) 
            GROUP BY 
            aid, audiencecohort) ),
            
/*************************************************************************************************************
 Grab the PROMOTED program video viewing data of early adopters in each audience cohorts from campaign start day to +7 days after they signed up.
 :: just promoted programs
***************************************************************************************************************/
early_adopter_promoted_viewing AS (
SELECT
aid,
audiencecohort,
distinct_titles,
content_starts,
content_completed,
total_hours
FROM (SELECT  
      aid,
      audiencecohort, 
      count(DISTINCT new_program) AS distinct_titles,
      sum(num_views_started) AS content_starts,
      sum(num_views_completed) AS content_completed,
      sum(num_seconds_played_no_ads)/3600 as total_hours
      
      FROM (
              (SELECT * , case when lower(consumption_type) like '%shortform%' then 'shortform' else program end as new_program 
              FROM
             `res-nbcupea-dev-ds-sandbox-001.silverTables.SILVER_VIDEO`
               WHERE 
              num_views_started = 1  
              AND
              program in UNNEST(program_list)
             )  AS video
             INNER JOIN 
             (
             SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` 
               WHERE 
             adopter_group like '%pre%' and signup_date >= '2020-06-21' -- where signup_date <  CAST('2020-04-29' AS DATE) 
             ) AS usertable 
             ON 
             video.adobe_tracking_id = usertable.aid 
             AND 
             video.adobe_timestamp >= datetime(campaign_start_period) -- 4/29
             AND 
             video.adobe_timestamp < datetime_add(datetime(campaign_start_period), INTERVAL analysis_window day) -- analyze 4/29 to 5/5
             ) 
            GROUP BY 
            aid, audiencecohort) ) ,

/*************************************************************************************************************
 Grab the total video viewing data of late adopters 
 * using a NEW attribution window: from day AFTER sign up + 7 days
 :: total viewing for all programs (not just promoted programs )
***************************************************************************************************************/
late_adopter_total_viewing_after_second_session AS ( -- do not include peacock activity on the day of their signup 
SELECT
aid,
audiencecohort,
distinct_titles,
content_starts,
content_completed,
total_hours
FROM (SELECT  
      aid,
      audiencecohort, 
      count(DISTINCT new_program) AS distinct_titles,
      sum(num_views_started) AS content_starts,
      sum(num_views_completed) AS content_completed,
      sum(num_seconds_played_no_ads)/3600 as total_hours
      
      FROM (
              (SELECT * , case when lower(consumption_type) like '%shortform%' then 'shortform' else program end as new_program 
              FROM
             `res-nbcupea-dev-ds-sandbox-001.silverTables.SILVER_VIDEO`
              WHERE 
              num_views_started = 1
             )  AS video
             INNER JOIN 
             (
             SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` 
              WHERE 
             adopter_group like '%during%' and signup_date >= '2020-06-21'  -- signup_date >= CAST('2020-04-29' AS DATE) -- users who signed up on or after 4/29
             ) AS usertable 
             ON 
             video.adobe_tracking_id = usertable.aid 
             AND 
             date(video.adobe_timestamp) > usertable.signup_date -- begin analysis from day AFTER they signed up
             AND 
             date(video.adobe_timestamp) <= date(datetime_add(datetime(usertable.signup_date), INTERVAL analysis_window day)) 
             ) 
            GROUP BY 
            aid, audiencecohort) ),

/*************************************************************************************************************
 Grab the promoted program video viewing data of late adopters v2
 * using a NEW attribution window: from day AFTER sign up + 7 days
 :: just promoted programs 
***************************************************************************************************************/
late_adopter_promoted_program_viewing_after_second_session AS ( -- do not include peacock activity on the day of their signup 
SELECT
aid,
audiencecohort,
distinct_titles,
content_starts,
content_completed,
total_hours
FROM (SELECT  
      aid,
      audiencecohort, 
      count(DISTINCT new_program) AS distinct_titles,
      sum(num_views_started) AS content_starts,
      sum(num_views_completed) AS content_completed,
      sum(num_seconds_played_no_ads)/3600 as total_hours
      
      FROM (
              (SELECT * , case when lower(consumption_type) like '%shortform%' then 'shortform' else program end as new_program 
              FROM
             `res-nbcupea-dev-ds-sandbox-001.silverTables.SILVER_VIDEO`
              WHERE 
              num_views_started = 1
              and program in UNNEST(program_list)
             )  AS video
             INNER JOIN 
             (
             SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` 
              WHERE 
             adopter_group like '%during%' and signup_date >= '2020-06-21'  -- signup_date >= CAST('2020-04-29' AS DATE) -- users who signed up on or after 4/29 to 5/12 
             ) AS usertable 
             ON 
             video.adobe_tracking_id = usertable.aid 
             AND 
             date(video.adobe_timestamp) > usertable.signup_date -- begin analysis from day AFTER they signed up
             AND 
             date(video.adobe_timestamp) <= date(datetime_add(datetime(usertable.signup_date), INTERVAL analysis_window day)) 
             ) 
            GROUP BY 
            aid, audiencecohort)
)

/*************************************************************************************************************
Get overall stats for the early and late adopter of each campaign's audience cohort group
:: early users (4/15 to 4/28 signups)
:: late (4/29 to 5/14 signups with attribution window DAY OF signup +7days)
:: late v2 (4/29 to 5/14 signups with new attribution window DAY AFTER a user signedup +7days)
:: all viewing - summary stats for all viewing on peacock
:: promoted program viewing - summary stats for promoted program viewing
***************************************************************************************************************/
-- Get overall stats for the early and late adopter of each campaign's audience cohort group
SELECT 
cohort.audiencecohort,
    'early' as adopter_group,-- early adopter group has 1+ week tenure
    'all viewing' as lens,
cohort_size,
downstream.conversions_watched_video,
downstream.conversions_watched_video/cohort_size as conversion_rate,
downstream.repertoire,
downstream.avg_starts,
downstream.usage,
downstream.avg_completion_rate,
downstream.all_starts,
downstream.all_completes
FROM 
(SELECT audiencecohort, 
  count(DISTINCT aid) AS cohort_size -- early adopters before 4/29
  FROM 
             (
             SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` 
              WHERE 
             adopter_group like '%pre%' and signup_date >= '2020-06-21'  -- signup_date <  CAST('2020-04-29' AS DATE) 
             ) 
  GROUP BY audiencecohort) 
  AS cohort
  LEFT JOIN
  (SELECT 
    audiencecohort, 
    count(DISTINCT aid) AS conversions_watched_video, -- counts num. users per cohort who watched anything in 7 days after 4/29
    sum(distinct_titles) / count(DISTINCT aid) AS repertoire,
    sum(content_starts) / count(DISTINCT aid) AS avg_starts,
    sum(total_hours) / count(DISTINCT aid) AS usage,
    sum(content_completed)/sum(content_starts) AS avg_completion_rate,
    sum(content_starts) as all_starts,-- to use for completion rate sig test - diff of proportions
    sum(content_completed) as all_completes -- to use for completoin rate sig test - diff of proportions
  FROM early_adopter_total_viewing 
  GROUP BY audiencecohort) 
  AS downstream
ON cohort.audiencecohort = downstream.audiencecohort

UNION DISTINCT

-- Get overall cohort avg user viewing stats for late adopters after their second session
SELECT 
cohort.audiencecohort,
    "late after second session" as adopter_group,
    "all viewing" as lens,
cohort_size,
downstream.conversions_watched_video,
downstream.conversions_watched_video/cohort_size as conversion_rate,
downstream.repertoire,
downstream.avg_starts,
downstream.usage,
downstream.avg_completion_rate,
downstream.all_starts,
downstream.all_completes
FROM 
(SELECT audiencecohort, 
  count(DISTINCT aid) AS cohort_size -- later adopters signed up on >= 4/29
  FROM 
             (
             SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` 
              WHERE 
             adopter_group like '%during%' and signup_date >= '2020-06-21'  -- signup_date >=  CAST('2020-04-29' AS DATE) --  4/29 and onwards sigups
             ) 
  GROUP BY audiencecohort) 
  AS cohort
  LEFT JOIN
  (SELECT 
    audiencecohort, 
    count(DISTINCT aid) AS conversions_watched_video, 
    sum(distinct_titles) / count(DISTINCT aid) AS repertoire,
    sum(content_starts) / count(DISTINCT aid) AS avg_starts,
    sum(total_hours) / count(DISTINCT aid) AS usage,
    sum(content_completed)/sum(content_starts) AS avg_completion_rate,
    sum(content_starts) as all_starts,-- include to use for completion rate sig test - diff of proportions
    sum(content_completed) as all_completes -- include to use for completoin rate sig test - diff of proportions
  FROM late_adopter_total_viewing_after_second_session 
  GROUP BY audiencecohort) 
  AS downstream
ON cohort.audiencecohort = downstream.audiencecohort

UNION DISTINCT

SELECT 
cohort.audiencecohort,
    'early' as adopter_group,-- early adopter group has 1+ week tenure
    'promoted program viewing' as lens,
cohort_size,
downstream.conversions_watched_video,
downstream.conversions_watched_video/cohort_size as conversion_rate,
downstream.repertoire,
downstream.avg_starts,
downstream.usage,
downstream.avg_completion_rate,
downstream.all_starts,
downstream.all_completes
FROM 
(SELECT audiencecohort, 
  count(DISTINCT aid) AS cohort_size -- early adopters before 4/29
  FROM 
             (
             SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` 
              WHERE 
             adopter_group like '%pre%' and signup_date >= '2020-06-21'  -- signup_date <  CAST('2020-04-29' AS DATE) 
             ) 
  GROUP BY audiencecohort) 
  AS cohort
  LEFT JOIN
  (SELECT 
    audiencecohort, 
    count(DISTINCT aid) AS conversions_watched_video, -- counts num. users per cohort who watched anything in 7 days after 4/29
    sum(distinct_titles) / count(DISTINCT aid) AS repertoire,
    sum(content_starts) / count(DISTINCT aid) AS avg_starts,
    sum(total_hours) / count(DISTINCT aid) AS usage,
    sum(content_completed)/sum(content_starts) AS avg_completion_rate,
    sum(content_starts) as all_starts,-- to use for completion rate sig test - diff of proportions
    sum(content_completed) as all_completes -- to use for completoin rate sig test - diff of proportions
  FROM early_adopter_PROMOTED_viewing 
  GROUP BY audiencecohort) 
  AS downstream
ON cohort.audiencecohort = downstream.audiencecohort

UNION DISTINCT

-- Get overall cohort avg user viewing stats for late adopters after their second session
SELECT 
cohort.audiencecohort,
    "late after second session" as adopter_group,
    "promoted program viewing" as lens,
cohort_size,
downstream.conversions_watched_video,
downstream.conversions_watched_video/cohort_size as conversion_rate,
downstream.repertoire,
downstream.avg_starts,
downstream.usage,
downstream.avg_completion_rate,
downstream.all_starts,
downstream.all_completes
FROM 
(SELECT audiencecohort, 
  count(DISTINCT aid) AS cohort_size -- later adopters signed up on >= 4/29
  FROM 
             (
             SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` 
              WHERE 
             adopter_group like '%during%' and signup_date >= '2020-06-21'  -- signup_date >=  CAST('2020-04-29' AS DATE) --  4/29 and onwards sigups
             ) 
  GROUP BY audiencecohort) 
  AS cohort
  LEFT JOIN
  (SELECT 
    audiencecohort, 
    count(DISTINCT aid) AS conversions_watched_video, 
    sum(distinct_titles) / count(DISTINCT aid) AS repertoire,
    sum(content_starts) / count(DISTINCT aid) AS avg_starts,
    sum(total_hours) / count(DISTINCT aid) AS usage,
    sum(content_completed)/sum(content_starts) AS avg_completion_rate,
    sum(content_starts) as all_starts,-- include to use for completion rate sig test - diff of proportions
    sum(content_completed) as all_completes -- include to use for completoin rate sig test - diff of proportions
  FROM late_adopter_promoted_program_viewing_after_second_session 
  GROUP BY audiencecohort) 
  AS downstream
ON cohort.audiencecohort = downstream.audiencecohort
