DECLARE holdoutcampaign_1  string;
DECLARE holdoutcampaign_2  string;
DECLARE holdoutcampaign_3  string;

DECLARE tenure_threshold_date date; -- profiles must be signed up before this day for 7 days of viewing data.

SET tenure_threshold_date = '2020-09-16'; -- 6/1 last day for signup. guarantees at least 7 days of viewing data for all users in analysis.

SET holdoutcampaign_1 = 'holdoutgroup_control';
SET holdoutcampaign_2 = 'holdoutgroup_paidsoc';
SET holdoutcampaign_3 = 'holdoutgroup_paiddis';

/**************************************************************************************************
Grab the mp tracking ids from all of the campaign holdout mparticle files.
The mp tracking id is the identity_value when the identity_type is 'other_5'.
**************************************************************************************************/
CREATE OR REPLACE TABLE `nbcu-sdp-sandbox-prod.hr_sandbox.holdoutgroup_masterlist` AS
SELECT identity_value,
      holdoutcampaign_1 AS audiencecohort
      FROM  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_media_holdout`
      WHERE
      identity_type = 'other_5'
UNION ALL
SELECT identity_value,
      holdoutcampaign_2 AS audiencecohort
      FROM  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.paid_social_holdout`
      WHERE
      identity_type = 'other_5'
UNION ALL
SELECT identity_value,
      holdoutcampaign_3 AS audiencecohort
      FROM  `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.paid_display_holdout`
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
