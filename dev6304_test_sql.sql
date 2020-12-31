SELECT
    transactions.activity,
    transactions.count,
    awards.is_fpds,
    awards.id
FROM awards
INNER JOIN (
    SELECT
        award_id,
        COUNT(*) as count,
        array_agg(DISTINCT to_char(action_date + INTERVAL '3' month, '"FY"YYYY-"Q"Q-MM')) as activity
    FROM transaction_normalized
    GROUP BY award_id
) AS transactions ON transactions.award_id = awards.id
WHERE
    (
        (awards.type IN ('07', '08') and total_subsidy_cost > 0)
        OR awards.type NOT IN ('07', '08')
    )
    AND (date_signed >= '2017-01-01' OR certified_date >= '2017-01-01')
;
-- w/o date cutoff: 105,594,770 rows in 885.5 s
-- w/ date cutoff:   50,236,888 rows in 704.6 s


WITH valid_file_d_awards AS (
    SELECT
        transactions.activity,
        transactions.count,
        awards.is_fpds,
        awards.id,
        awards.awarding_agency_id
    FROM awards
    INNER JOIN (
        SELECT
            award_id,
            COUNT(*) as count,
            array_agg(DISTINCT to_char(action_date + INTERVAL '3' month, '"FY"YYYY-"Q"Q-MM')) as activity
        FROM transaction_normalized
        WHERE action_date >= '2017-01-01'
        GROUP BY award_id
    ) AS transactions ON transactions.award_id = awards.id
    WHERE
        (
            (awards.type IN ('07', '08') and total_subsidy_cost > 0)
            OR awards.type NOT IN ('07', '08')
        )
        AND (date_signed >= '2017-01-01' OR certified_date >= '2017-01-01')
), unlinked_awards AS (
    SELECT
        unnest(activity) as period,
        id,
        is_fpds,
        awarding_agency_id
    FROM valid_file_d_awards
    WHERE NOT EXISTS (
        SELECT 1
        FROM financial_accounts_by_awards AS faba
        WHERE faba.award_id = valid_file_d_awards.id)
)
SELECT
    (SELECT name FROM agency a INNER JOIN toptier_agency USING (toptier_agency_id) where a.id = awarding_agency_id) as name,
    period,
    count(*),
    array_agg(id),
    COUNT(CASE WHEN is_fpds = True THEN 1 ELSE NULL END) AS procurement,
    COUNT(CASE WHEN is_fpds = False THEN 1 ELSE NULL END) AS assistance
FROM unlinked_awards
GROUP BY awarding_agency_id, period;
