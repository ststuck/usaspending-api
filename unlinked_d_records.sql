WITH valid_file_d_awards AS (
    SELECT
        transactions.activity,
        transactions.count,
        awards.*
    FROM awards
    INNER JOIN (
        SELECT
            award_id,
            COUNT(*) as count,
            array_agg(DISTINCT to_char(action_date + INTERVAL '3' month, 'YYYY-"Q"Q-MM')) as activity
        FROM transaction_normalized
        GROUP BY award_id
    ) AS transactions ON transactions.award_id = awards.id
    WHERE
        (awards.type IN ('07', '08') and total_subsidy_cost > 0)
        OR awards.type NOT IN ('07', '08')
), unlinked_awards AS (
    SELECT
        unnest(activity) as period,
        id,
        is_fpds
    FROM valid_file_d_awards
    WHERE NOT EXISTS (
        SELECT 1
        FROM financial_accounts_by_awards AS faba
        WHERE faba.award_id = valid_file_d_awards.id)
)
SELECT
    period,
    is_fpds,
    COUNT(*)
FROM unlinked_awards
GROUP BY period, is_fpds
;