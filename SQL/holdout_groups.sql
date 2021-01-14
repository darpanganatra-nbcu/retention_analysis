DECLARE tenure_threshold_date date DEFAULT DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY);
DECLARE campaign_start_date DATE DEFAULT '2020-07-29';

/********************************************************************************************************
  Make the targetable cohort for All Paid Media
*******************************************************************************************************/
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_control` AS
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
                  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.holdoutgroup_masterlist_userinfo_signup_dates`
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
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_paidsoc` AS
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
                  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.holdoutgroup_masterlist_userinfo_signup_dates`
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
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_paiddis` AS
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
                  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.holdoutgroup_masterlist_userinfo_signup_dates`
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
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_masterlist` AS
select Householdid AS profileid,
                  AudienceCohort
FROM (
SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_control`
UNION ALL
SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_paidsoc`
UNION ALL
SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_paiddis`
);

/********************************************************************************************************
 Append user ids with their adobe tracking ids ("aid"'s) for table with all targetable user info
 - note:  all but ~1100 of the targetable profile ids have adobetrackingids. make sure to only grab nonnull.
********************************************************************************************************/
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_masterlist_userinfo` AS
SELECT  b.ProfileID,
        b.ExternalProfilerID AS aid,
        a.AudienceCohort,
        FROM
        (
       SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_masterlist`
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
CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table` AS
SELECT aid, profileid, audiencecohort FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.holdoutgroup_masterlist_userinfo_signup_dates`
WHERE audiencecohort <> 'holdoutgroup_brazeco' -- braze holdout was used to make a targetable. braze not to be included in analysis.
UNION ALL
SELECT aid, profileid, audiencecohort FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.targetablegroup_masterlist_userinfo`;

/*************************************************************************************************************
  Append all users in the audience cohorts with their account sign up dates for early vs late adopters distinction.
***************************************************************************************************************/

CREATE OR REPLACE TABLE `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates` AS
SELECT *,
  CASE
    WHEN signup_date < campaign_start_date THEN "pre"
    WHEN signup_date >= campaign_start_date THEN "during"
   ELSE "not_in_cohort"
  END AS adopter_group -- create an adopter_group to toggle for early vs late signups depending on account tenure."late"=signup after campaign start date
FROM (
  SELECT a.*,
  b.signup_date
  FROM (
    SELECT * FROM `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table`
  ) a
LEFT JOIN
(SELECT * FROM (
        (SELECT DISTINCT HouseholdID,
                  cast(min(datetime(targetedoptindate, 'America/New_York')) as date) as signup_date
                  FROM `nbcu-sdp-prod-003.sdp_persistent_views.AccountView` group by householdid
                  ) )  ) b
ON a.ProfileID = b.HouseholdID );
