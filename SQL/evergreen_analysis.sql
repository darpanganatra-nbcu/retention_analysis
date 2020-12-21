declare campaign_start_period date default "2020-10-01";
declare analysis_window int64 default 7;
with overall_stats as (
    SELECT
        aid,
        audiencecohort,
        distinct_titles,
        content_starts,
        content_completed,
        total_hours
    FROM
        (
            SELECT
                aid,
                audiencecohort,
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
                            `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates`
                    ) AS usertable ON video.adobe_tracking_id = usertable.aid
                    AND video.adobe_timestamp >= datetime(campaign_start_period)
                    AND video.adobe_timestamp < datetime_add(
                        datetime(campaign_start_period),
                        INTERVAL analysis_window day
                    )
                )
            GROUP BY
                aid,
                audiencecohort
        )
),
general_stats as (
    SELECT
        audiencecohort,
        count(DISTINCT aid) AS conversions_watched_video,
        -- counts num. users per cohort who watched anything in 7 days after 4/29,
        sum(distinct_titles) / count(DISTINCT aid) AS repertoire,
        STDDEV(distinct_titles) as repertoire_stddev,
        sum(content_starts) / count(DISTINCT aid) AS avg_starts,
        sum(total_hours) / count(DISTINCT aid) AS usage,
        STDDEV(total_hours) as usage_stdv,
        sum(content_completed) / sum(content_starts) AS avg_completion_rate,
        sum(content_starts) as all_starts,
        -- to use for completion rate sig test - diff of proportions
        sum(content_completed) as all_completes -- to use for completoin rate sig test - diff of proportions
    FROM
        overall_stats
    GROUP BY
        audiencecohort
)
SELECT
    A.*,
    conversions_watched_video / cohort_size as conversion_rate,
    cohort.cohort_size
FROM
    general_stats A
    LEFT JOIN (
        SELECT
            audiencecohort,
            COUNT(DISTINCT aid) as cohort_size
        FROM
            `res-nbcupea-dev-ds-sandbox-001.dg_sandbox.all_audiencecohort_table_signup_dates`
        GROUP BY
            audiencecohort
    ) cohort ON A.audiencecohort = cohort.audiencecohort
