WITH grouped_transaction_normalized AS
    (SELECT award_id, COALESCE(SUM(original_loan_subsidy_cost), 0) AS total_loan
    FROM transaction_normalized
    WHERE type IN ('07', '08')
    GROUP BY award_id),
c_to_d1_counts AS
    (SELECT ta.name || ' (' || ta.abbreviation || ')' AS agency,
        COUNT(1) AS "C to D1"
    FROM financial_accounts_by_awards AS faba
    JOIN submission_attributes AS sa ON sa.submission_id = faba.submission_id
    JOIN toptier_agency AS ta ON ta.toptier_code = sa.toptier_code
    WHERE reporting_fiscal_year = 2020
        AND reporting_fiscal_period = 9
        AND award_id IS NULL
        AND transaction_obligated_amount IS NOT NULL
        AND piid IS NOT NULL
    GROUP BY ta.name || ' (' || ta.abbreviation || ')'),
c_to_d2_counts AS
    (SELECT ta.name || ' (' || ta.abbreviation || ')' AS agency,
        COUNT(1) AS "C to D2"
    FROM financial_accounts_by_awards AS faba
    JOIN submission_attributes AS sa ON sa.submission_id = faba.submission_id
    JOIN toptier_agency AS ta ON ta.toptier_code = sa.toptier_code
    WHERE reporting_fiscal_year = 2020
        AND reporting_fiscal_period = 9
        AND award_id IS NULL
        AND transaction_obligated_amount IS NOT NULL
        AND piid IS NULL
    GROUP BY ta.name || ' (' || ta.abbreviation || ')'),
d1_to_c_counts AS
    (SELECT ta.name || ' (' || ta.abbreviation || ')' AS agency,
        COUNT(1) AS "D1 to C"
    FROM awards
    JOIN transaction_normalized AS tn ON tn.id = awards.latest_transaction_id
    JOIN agency ON agency.id = awards.awarding_agency_id
    JOIN toptier_agency AS ta ON agency.toptier_agency_id = ta.toptier_agency_id
    WHERE tn.action_date >= '2020-06-01'
        AND tn.action_date <= '2020-06-30'
        AND NOT EXISTS (
            SELECT 1
            FROM financial_accounts_by_awards AS faba
            WHERE faba.award_id = awards.id)
        AND NOT EXISTS (
            SELECT 1
            FROM grouped_transaction_normalized AS gtn
            WHERE tn.award_id = gtn.award_id
                AND gtn.total_loan < 0)
        AND piid IS NOT NULL
    GROUP BY ta.name || ' (' || ta.abbreviation || ')'),
d2_to_c_counts AS
    (SELECT ta.name || ' (' || ta.abbreviation || ')' AS agency, COUNT(1) AS "D2 to C"
    FROM awards
    JOIN transaction_normalized AS tn ON tn.id = awards.latest_transaction_id
    JOIN agency ON agency.id = awards.awarding_agency_id
    JOIN toptier_agency AS ta ON agency.toptier_agency_id = ta.toptier_agency_id
    WHERE tn.action_date >= '2020-06-01'
        AND tn.action_date <= '2020-06-30'
        AND NOT EXISTS (
            SELECT 1
            FROM financial_accounts_by_awards AS faba
            WHERE faba.award_id = awards.id)
        AND NOT EXISTS (
            SELECT 1
            FROM grouped_transaction_normalized AS gtn
            WHERE tn.award_id = gtn.award_id
                AND gtn.total_loan < 0)
        AND piid IS NULL
    GROUP BY ta.name || ' (' || ta.abbreviation || ')')
SELECT
    COALESCE(cd1.agency, cd2.agency, d1c.agency, d2c.agency) AS agency,
    COALESCE("C to D1", 0) AS "C to D1",
    COALESCE("D1 to C", 0) AS "D1 to C",
    (COALESCE("C to D1", 0) + COALESCE("D1 to C", 0)) AS "Sum C/D1",
    COALESCE("C to D2", 0) AS "C to D2",
    COALESCE("D2 to C", 0) AS "D2 to C",
    (COALESCE("C to D2", 0) + COALESCE("D2 to C", 0)) AS "Sum C/D2"
FROM c_to_d1_counts AS cd1
FULL OUTER JOIN c_to_d2_counts AS cd2 ON cd1.agency = cd2.agency
FULL OUTER JOIN d1_to_c_counts AS d1c ON cd1.agency = d1c.agency
FULL OUTER JOIN d2_to_c_counts AS d2c ON cd1.agency = d2c.agency;