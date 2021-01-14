declare campaign_start_period date default "2020-12-01";
declare analysis_window int64 default DATE_DIFF(DATE_SUB( DATE_TRUNC( DATE_ADD(campaign_start_period, INTERVAL 1 MONTH), MONTH ), INTERVAL 1 DAY ), campaign_start_period, DAY) + 1;
with overall_stats as (
    SELECT
        aid,
        audiencecohort,
        tenure_classification,
        distinct_titles,
        content_starts,
        content_completed,
        total_hours
    FROM
        (
            SELECT
                aid,
                audiencecohort,
                tenure_classification,
                count(DISTINCT new_program) AS distinct_titles,
                sum(num_views_started) AS content_starts,
                sum(num_views_completed) AS content_completed,
                sum(num_seconds_played_no_ads) / 3600 as total_hours
            FROM
                (
                    (
                        SELECT
                            *,
                            case
                                when lower(consumption_type) like '%shortform%' then 'shortform'
                                else program
                            end as new_program
                        FROM
                            `res-nbcupea-dev-ds-sandbox-001.silverTables.SILVER_VIDEO`
                        WHERE
                            num_views_started = 1
                    ) AS video
                    INNER JOIN (
                        SELECT
                            *
                        FROM
                            `res-nbcupea-dev-ds-sandbox-001.weekly_retention_data.all_audiencecohort_classification`
                    ) AS usertable ON video.adobe_tracking_id = usertable.aid
                    AND video.adobe_timestamp >= datetime(campaign_start_period)
                    AND video.adobe_timestamp < datetime_add(
                        datetime(campaign_start_period),
                        INTERVAL analysis_window day
                    )
                )
            GROUP BY
                aid,
                audiencecohort,
                tenure_classification
        )
),
general_stats as (
    SELECT
        audiencecohort,
        tenure_classification,
        count(DISTINCT aid) AS conversions_watched_video,
        sum(distinct_titles) / count(DISTINCT aid) AS repertoire,
        STDDEV(distinct_titles) as repertoire_stddev,
        sum(content_starts) / count(DISTINCT aid) AS avg_starts,
        sum(total_hours) / count(DISTINCT aid) AS usage,
        STDDEV(total_hours) as usage_stdv,
        sum(content_completed) / sum(content_starts) AS avg_completion_rate,
        sum(content_starts) as all_starts,
        sum(content_completed) as all_completes
    FROM
        overall_stats
    GROUP BY
        audiencecohort,
        tenure_classification
)
SELECT
    *
FROM
    general_stats
