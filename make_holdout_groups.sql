/************************************************************************************************
  The Manual Step 1: get the mparticle audiences into GCP
       a) Export the holdout audience csv's from mparticle
       b) Upload each holdout audience csv file to google drive
       c) Import each holdout audience into sandbox as new datasets via the "Drive" upload process, toggle read as text.

 >> code points to sandbox for: the mparticle audience files manually imported into GCP
  (in here, these files are -20796_MasterMediaHoldOutGroups_Control_044445May152020
                            -20797_MasterMediaHoldOutGroups_BrazeCo_044449May152020
                            -20799_MasterMediaHoldOutGroups_PaidSoc_044453May152020
                            -20800_MasterMediaHoldOutGroups_PaidDis_044456May152020

    6/4/2020 refresh:
                            - 20797_MasterMediaHoldOutGroups_BrazeCo_194235Jun042020
                            - 20796_MasterMediaHoldOutGroups_Control_063045Jun042020
                            - 20799_MasterMediaHoldOutGroups_PaidSoc_063050Jun042020
                            - 20800_MasterMediaHoldOutGroups_PaidDis_063053Jun042020 )

 >> code does not return anything. code creates a sandbox table (holdoutgroup_masterlist_userinfo_signup_dates)
 that gets used in "Make Targetable Groups".
 -this sandbox table has all mpids, profileids, adobetrackingids, signup date for users in all holdout groups with cohort name eg:holdoutgroup_paiddis
**************************************************************************************************/

DECLARE holdoutcampaign_1  string;
DECLARE holdoutcampaign_2  string;
DECLARE holdoutcampaign_3  string;
DECLARE holdoutcampaign_4  string;

DECLARE tenure_threshold_date date; -- profiles must be signed up before this day for 7 days of viewing data.

SET tenure_threshold_date = '2020-06-02'; -- 6/1 last day for signup. guarantees at least 7 days of viewing data for all users in analysis.

SET holdoutcampaign_1 = 'holdoutgroup_control';
SET holdoutcampaign_2 = 'holdoutgroup_brazeco';
SET holdoutcampaign_3 = 'holdoutgroup_paidsoc';
SET holdoutcampaign_4 = 'holdoutgroup_paiddis';

/**************************************************************************************************
Grab the mp tracking ids from all of the campaign holdout mparticle files.
The mp tracking id is the identity_value when the identity_type is 'other_5'.
**************************************************************************************************/
CREATE OR REPLACE TABLE nbcu-sdp-sandbox-prod.hr_sandbox.holdoutgroup_masterlist AS
SELECT identity_value,
      holdoutcampaign_1 AS audiencecohort
      FROM  `nbcu-sdp-sandbox-prod.hr_sandbox.20796_MasterMediaHoldOutGroups_Control_063045Jun042020`
      WHERE
      identity_type = 'other_5'
UNION ALL
SELECT identity_value,
      holdoutcampaign_2 AS audiencecohort
      FROM  `nbcu-sdp-sandbox-prod.hr_sandbox.20797_MasterMediaHoldOutGroups_BrazeCo_194235Jun042020`
      WHERE
      identity_type = 'other_5'
UNION ALL
SELECT identity_value,
      holdoutcampaign_3 AS audiencecohort
      FROM  `nbcu-sdp-sandbox-prod.hr_sandbox.20799_MasterMediaHoldOutGroups_PaidSoc_063050Jun042020`
      WHERE
      identity_type = 'other_5'
UNION ALL
SELECT identity_value,
      holdoutcampaign_4 AS audiencecohort
      FROM  `nbcu-sdp-sandbox-prod.hr_sandbox.20800_MasterMediaHoldOutGroups_PaidDis_063053Jun042020`
      WHERE
      identity_type = 'other_5';

/**************************************************************************************************
Append SDP user id information to each mp tracking id in the holdout masterlist.
Add profileid, mpid, adobetrackingid, and their cohortname for each user in holdout group list.
**************************************************************************************************/
CREATE OR REPLACE TABLE `nbcu-sdp-sandbox-prod.hr_sandbox.holdoutgroup_masterlist_userinfo` AS
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
         `nbcu-sdp-sandbox-prod.hr_sandbox.holdoutgroup_masterlist` list
        ON map.ExternalProfilerID = list.identity_value) b
    ON a.ExternalProfilerID = b.mparticleid
    LEFT JOIN --  attach the adobetrackingid to identify audience members in adobe viewing table
        (SELECT   profileid AS profileid_b,
                  ExternalProfilerID AS AdobeTrackingID
            FROM
                   `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping`
            WHERE
                      PartnerOrSystemId= 'trackingid') c
            ON a.ProfileID = c.profileid_b;

/**************************************************************************************************
Append each profile's signup date to the table to account for profile tenure in analysis (late v early adopters)
**************************************************************************************************/
CREATE OR REPLACE TABLE `nbcu-sdp-sandbox-prod.hr_sandbox.holdoutgroup_masterlist_userinfo_signup_dates` AS
SELECT *
FROM (
  SELECT a.*,
  b.signup_date
  FROM (
    SELECT * FROM `nbcu-sdp-sandbox-prod.hr_sandbox.holdoutgroup_masterlist_userinfo`
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
);

-- verify implementation of account tenure threshold. check last signup date to get 7 days of viewing for all profiles.
select audiencecohort, max(signup_date), count(*) from `nbcu-sdp-sandbox-prod.hr_sandbox.holdoutgroup_masterlist_userinfo_signup_dates` group by audiencecohort;
