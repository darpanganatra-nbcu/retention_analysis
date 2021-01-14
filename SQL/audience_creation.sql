DECLARE tenure_threshold_date date DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
DECLARE campaign_start_date DATE DEFAULT '2020-07-29';

CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_holdout_groups` AS (
SELECT identity_value,
      "holdoutgroup_control" AS audiencecohort
      FROM  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_media_holdout_backfill`
      WHERE
      identity_type = 'other_5'
UNION DISTINCT
SELECT identity_value,
      "holdoutgroup_paidsoc" AS audiencecohort
      FROM  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.paid_social_holdout_backfill`
      WHERE
      identity_type = 'other_5'
UNION DISTINCT
SELECT identity_value,
      "holdoutgroup_paiddis" AS audiencecohort
      FROM  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.paid_display_holdout_backfill`
      WHERE
      identity_type = 'other_5'
UNION DISTINCT
SELECT identity_value,
      "holdoutgroup_braze" AS audiencecohort
      FROM  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.paid_braze_holdout_backfill`
      WHERE
      identity_type = 'other_5'
UNION DISTINCT
SELECT
    IdentityValues_Other_Id_5 as identity_value,
    CASE AudienceName
        WHEN "Master-Media-Hold-Out-Groups---Control" THEN "holdoutgroup_control"
        WHEN "Master-Media-Hold-Out-Groups---Paid-Social-Control-Group" THEN "holdoutgroup_paidsoc"
        WHEN "Master-Media-Hold-Out-Groups---Paid-Display-Control-Group" THEN "holdoutgroup_paiddis"
        WHEN "Master-Media-Hold-Out-Groups---Braze-Control-Group" THEN "holdoutgroup_braze"
    END AS AudienceCohort
FROM
    `nbcu-sdp-prod-003.sdp_persistent_views.mParticleEmailMarketingAudienceListView`
WHERE
    AudienceName IN (
        "Master-Media-Hold-Out-Groups---Control",
        "Master-Media-Hold-Out-Groups---Paid-Social-Control-Group",
        "Master-Media-Hold-Out-Groups---Paid-Display-Control-Group",
        "Master-Media-Hold-Out-Groups---Braze-Control-Group"
    )
    );


CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.holdoutgroup_masterlist_userinfo_signup_dates` AS (
with master_holdout_table as (
SELECT
    *
FROM `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_holdout_groups`
),

master_holdout_userinfo as (
SELECT  a.ProfileID,
        a.ExternalProfilerID as mParticleID,
        b.AudienceCohort,
        c.AdobeTrackingID as aid,
    FROM `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping` a
    INNER JOIN --
      (SELECT  map.ProfileID,
               map.ExternalProfilerID as mparticleid,
               list.AudienceCohort
        FROM `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping` map
        INNER JOIN  -- only grab users in this audience holdout list, note: identity_value is the mptrackingid
         master_holdout_table list
        ON map.ExternalProfilerID = list.identity_value) b
    ON a.ExternalProfilerID = b.mparticleid
    LEFT JOIN --  attach the adobetrackingid to identify audience members in adobe viewing table
        (SELECT   profileid AS profileid_b,
                  ExternalProfilerID AS AdobeTrackingID
            FROM
                   `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping`
            WHERE
                      PartnerOrSystemId= 'trackingid') c
            ON a.ProfileID = c.profileid_b
)


SELECT *
FROM (
  SELECT a.*,
  b.signup_date
  FROM (
    SELECT * FROM master_holdout_userinfo
  ) a
LEFT JOIN
(SELECT * FROM (
        (SELECT DISTINCT HouseholdID,
                  cast(min(datetime(targetedoptindate, 'America/New_York')) as date) as signup_date
                  FROM `nbcu-sdp-prod-003.sdp_persistent_views.AccountView` group by householdid
                  ) ) )  b
ON a.ProfileID = b.HouseholdID
WHERE
signup_date < tenure_threshold_date -- must meet tenure requirement
));

/********************************************************************************************************
  Make the targetable cohort for All Paid Media
*******************************************************************************************************/
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_control` AS
SELECT HouseholdID,
       'targetablegroup_control' AS audiencecohort
              FROM (
                  SELECT DISTINCT HouseholdId
                  FROM `nbcu-sdp-prod-003.sdp_persistent_views.AccountView`
                  WHERE
                  CAST(datetime(targetedoptindate, 'America/New_York') as date) < tenure_threshold_date
                ) all_householdids
FULL OUTER JOIN
         (SELECT DISTINCT PROFILEID
                  FROM
                  `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.holdoutgroup_masterlist_userinfo_signup_dates`
                  WHERE
                  audiencecohort = 'holdoutgroup_control'-- not in all paid media holdout
                  OR
                  audiencecohort = 'holdoutgroup_paidsoc' -- not in paid social holdout
                  OR
                  audiencecohort = 'holdoutgroup_paiddis'  -- not in paid display holdout
                  OR
                  audiencecohort = 'holdoutgroup_brazeco'  -- not in email holdout
                  )  holdouts
  ON all_householdids.householdid  = holdouts.profileid
  WHERE
  holdouts.profileid IS NULL; -- keeps ids not matched to a holdout id.


/********************************************************************************************************
  Make the targetable cohort for Paid Social
*******************************************************************************************************/
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_paidsoc` AS
SELECT HouseholdID,
       'targetablegroup_paidsoc' as audiencecohort
        FROM (
        SELECT DISTINCT HouseholdId
            FROM `nbcu-sdp-prod-003.sdp_persistent_views.AccountView`
            WHERE
            CAST(datetime(targetedoptindate, 'America/New_York') as date) < tenure_threshold_date
        ) all_householdids
FULL OUTER JOIN (SELECT
                  DISTINCT PROFILEID
                  FROM
                  `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.holdoutgroup_masterlist_userinfo_signup_dates`
                  WHERE
                  audiencecohort = 'holdoutgroup_control'-- not in all paid media holdout
                  OR
                  audiencecohort = 'holdoutgroup_paidsoc')  holdouts --  not in paid social holdout group
  ON all_householdids.householdid  = holdouts.profileid
  WHERE
  holdouts.profileid IS NULL; -- keeps ids not matched to a holdout id

/********************************************************************************************************
  Make the targetable cohort for Paid Display
*******************************************************************************************************/
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_paiddis` AS
SELECT HouseholdID,
       'targetablegroup_paiddis' as audiencecohort
       FROM (
            SELECT DISTINCT HouseholdId
            FROM `nbcu-sdp-prod-003.sdp_persistent_views.AccountView`
            WHERE
            CAST(datetime(targetedoptindate, 'America/New_York') as date) < tenure_threshold_date
) all_householdids
FULL OUTER JOIN ( SELECT
                  DISTINCT PROFILEID
                  FROM
                  `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.holdoutgroup_masterlist_userinfo_signup_dates`
                  WHERE
                  audiencecohort = 'holdoutgroup_control'-- not in all paid media holdout
                  OR
                  audiencecohort = 'holdoutgroup_paiddis')  holdouts -- not in paid display holdout group
  ON all_householdids.householdid  = holdouts.profileid
  WHERE
  holdouts.profileid IS NULL; -- keeps ids not matched to holdout records

/********************************************************************************************************
 Union all campaign's targetable audiences lists
********************************************************************************************************/
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_masterlist` AS
select Householdid AS profileid,
                  AudienceCohort
FROM (
SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_control`
UNION ALL
SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_paidsoc`
UNION ALL
SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_paiddis`
);

/********************************************************************************************************
 Append user ids with their adobe tracking ids ("aid"'s) for table with all targetable user info
********************************************************************************************************/
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_masterlist_userinfo` AS
SELECT  b.ProfileID,
        b.ExternalProfilerID AS aid,
        a.AudienceCohort,
        FROM
        (
       SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_masterlist`
       )  a
   LEFT JOIN
        ( SELECT   profileid AS profileid,
                  ExternalProfilerID
            FROM
                   `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping`
            WHERE
                      PartnerOrSystemId= 'trackingid'
           ) b
ON
a.ProfileID = b.profileid
WHERE
b.profileid is not null; -- (1066 people are without adobetrackingsids who got peacock access early before launch.)


/*************************************************************************************************************
Union the targetable and holdout master tables for an  ALL audience cohort table:
    >> columns in targetable master table: aid, profileid, audiencecohort
    >> columns in holdout master table be: aid, profileid, audiencecohort
**************************************************************************************************************/
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_audiencecohort_table` AS
SELECT aid, profileid, audiencecohort FROM `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.holdoutgroup_masterlist_userinfo_signup_dates`
WHERE audiencecohort <> 'holdoutgroup_brazeco' -- braze holdout was used to make a targetable. braze not to be included in analysis.
UNION ALL
SELECT aid, profileid, audiencecohort FROM `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.targetablegroup_masterlist_userinfo`;

/*************************************************************************************************************
  Append all users in the audience cohorts with their account sign up dates for early vs late adopters distinction.
***************************************************************************************************************/

CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_audiencecohort_table_signup_dates` AS
SELECT *

FROM (
  SELECT a.*,
  b.signup_date
  FROM (
    SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_audiencecohort_table`
  ) a
LEFT JOIN
(SELECT * FROM (
        (SELECT DISTINCT HouseholdID,
                  cast(min(datetime(targetedoptindate, 'America/New_York')) as date) as signup_date
                  FROM `nbcu-sdp-prod-003.sdp_persistent_views.AccountView` group by householdid
                  ) )  ) b
ON a.ProfileID = b.HouseholdID );