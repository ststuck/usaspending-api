
DO $$ BEGIN RAISE NOTICE 'Total Counts by Type'; END $$;
SELECT system, COUNT(*) as records FROM temp_dev3319_transactions_with_diff GROUP BY system ORDER BY "records" DESC;

DO $$ BEGIN RAISE NOTICE 'Percentage of FABS to reload'; END $$;
SELECT (SELECT COUNT(*) FROM temp_dev3319_transactions_with_diff where system = 'fabs') * 1.0 / (SELECT COUNT(*) FROM transaction_fabs) * 100.0;  -- 20.07%

DO $$ BEGIN RAISE NOTICE 'Percentage of FPDS to reload'; END $$;
SELECT (SELECT COUNT(*) FROM temp_dev3319_transactions_with_diff where system = 'fpds') * 1.0 / (SELECT COUNT(*) FROM transaction_fpds) * 100.0;  --

DO $$ BEGIN RAISE NOTICE 'Find all empty JSON columns (VERY BAD IF ANY EXIST!!!)'; END $$;
SELECT * FROM temp_dev3319_transactions_with_diff where fields_diff_json::text = '{}';

DO $$ BEGIN RAISE NOTICE 'Group by source and FY/Q, providing counts'; END $$;
SELECT
    CONCAT('FY',
        EXTRACT(YEAR from action_date + INTERVAL '3 months'),
        'Q',
        EXTRACT(QUARTER from action_date + INTERVAL '3 months')
    ) AS "fiscal_quarter",
    "system" as source,
    count(*) as "count"
from temp_dev3319_transactions_with_diff
where action_date >= '2007-10-01'
GROUP BY "fiscal_quarter", "system"
order by "fiscal_quarter", "system";

DO $$ BEGIN RAISE NOTICE 'Group and rank all of the problem columns by source and FY/Q'; END $$;
SELECT
    CONCAT('FY',
        EXTRACT(YEAR from action_date + INTERVAL '3 months'),
        'Q',
        EXTRACT(QUARTER from action_date + INTERVAL '3 months')
    ) AS "fiscal_quarter",
    jsonb_object_keys(fields_diff_json) as "trouble_columns",
    system as source,
    count(*) as "count"
from temp_dev3319_transactions_with_diff
where action_date >= '2007-10-01'
GROUP BY "fiscal_quarter", "trouble_columns", "source"
order by "fiscal_quarter", "count" DESC;

DO $$ BEGIN RAISE NOTICE 'Counts of all instances of column discrepencies for FABS'; END $$;
SELECT jsonb_object_keys(fields_diff_json), count(*)
FROM temp_dev3319_transactions_with_diff
WHERE "system" = 'fabs'
GROUP BY jsonb_object_keys(fields_diff_json)
ORDER BY count(*) DESC;

DO $$ BEGIN RAISE NOTICE 'Counts of all instances of column discrepencies for FPDS'; END $$;
SELECT jsonb_object_keys(fields_diff_json), count(*)
FROM temp_dev3319_transactions_with_diff
WHERE "system" = 'fpds'
GROUP BY jsonb_object_keys(fields_diff_json)
ORDER BY count(*) DESC;


-- -- Examine all records in FY2019Q4 with incorrect `legal_entity_address_line1`
-- SELECT fields_diff_json->'legal_entity_address_line1'  FROM temp_dev3319_transactions_with_diff WHERE system = 'fabs' AND (action_date >= '2019-06-01' AND action_date < '2019-10-01') and fields_diff_json ? 'legal_entity_address_line1';


-- SELECT REGEXP_REPLACE('2201 C ST NW\nIO/OB - ROOM 1427', '\s{2,}|\\n', ' ');

-- SELECT REGEXP_REPLACE('2201 C ST NW\nIO/OB - ROOM 1427', '\s{2,}|\\n', ' ') = '2201 C ST NW IO/OB - ROOM 1427';


-- SELECT REGEXP_REPLACE('959 ROUTE 46 EAST SUITE 402
-- SUITE 402
-- PARSIPPANY, NJ 07054-3409', E'\s{2,}|\\n', ' ') = '959 ROUTE 46 EAST SUITE 402 SUITE 402 PARSIPPANY, NJ 07054-3409';


-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO readonly;

-- GRANT SELECT ON vw_award_search TO readonly;

-- SELECT count(*) from temp_dev3319_transactions_with_diff where system = 'fabs' and action_date >= '2018-10-01';  -- 2,824,548


-- SELECT
--     jsonb_object_keys(d.fields_diff_json),
--     d.fields_diff_json#>>'{legal_entity_address_line1, broker}' broker_val,
--     d.fields_diff_json#>>'{legal_entity_address_line1, usaspending}' usaspending_val,
--     d.*
-- FROM temp_dev3319_transactions_with_diff d
-- LEFT JOIN LATERAL
--     jsonb_object_keys(d.fields_diff_json) j ON j::text != 'legal_entity_address_line1'
-- WHERE
--     d.system = 'fabs'
--     AND d.action_date >= '2018-10-01'
--     AND REGEXP_REPLACE(d.fields_diff_json#>>'{legal_entity_address_line1, broker}', E'\s+|\\n', ' ') != d.fields_diff_json#>>'{legal_entity_address_line1, usaspending}';



-- SELECT
--     jsonb_object_keys(d.fields_diff_json),
--     d.fields_diff_json#>>'{legal_entity_address_line1, broker}' ,
--     d.fields_diff_json#>>'{legal_entity_address_line1, usaspending}',
--     d.*
-- FROM temp_dev3319_transactions_with_diff d
-- WHERE
--     d.system = 'fabs'
--     AND d.action_date >= '2018-09-01'
--     AND REGEXP_REPLACE(d.fields_diff_json#>>'{legal_entity_address_line1, broker}', E'\s{2,}|\\n'::text, ' '::text) != d.fields_diff_json#>>'{legal_entity_address_line1, usaspending}' AND
--     jsonb_object(ARRAY[fields_diff_json]);



-- SELECT * FROM temp_dev3319_transactions_with_diff limit 90;



-- SELECT
--     d.broker_surrogate_id as published_award_financial_assistance_id
-- --    jsonb_object_keys(d.fields_diff_json),
-- --    d.fields_diff_json#>>'{submission_id, broker}' broker_submission_val,
-- --    d.fields_diff_json#>>'{submission_id, usaspending}' usaspending_submission_val,
-- --    d.fields_diff_json#>>'{funding_agency_code, broker}' broker_funding_agency_code_val,
-- --    d.fields_diff_json#>>'{funding_agency_code, usaspending}' usaspending_funding_agency_code_val
-- FROM temp_dev3319_transactions_with_diff d
-- WHERE
--     d.system = 'fabs'
--     AND d.action_date >= '2018-10-01' AND d.action_date < '2019-01-01' AND
--     CASE WHEN d.fields_diff_json ? 'legal_entity_address_line1' THEN REGEXP_REPLACE(d.fields_diff_json#>>'{legal_entity_address_line1, broker}', E'\s+|\\n', ' ') != d.fields_diff_json#>>'{legal_entity_address_line1, usaspending}' ELSE TRUE END;


-- SELECT
--     string_agg(',', d.broker_surrogate_id::text)
-- FROM temp_dev3319_transactions_with_diff d
-- WHERE
--     d.system = 'fabs'
--     AND d.action_date >= '2018-10-01' AND d.action_date < '2019-01-01' AND
--     CASE WHEN d.fields_diff_json ? 'legal_entity_address_line1' THEN REGEXP_REPLACE(d.fields_diff_json#>>'{legal_entity_address_line1, broker}', E'\s+|\\n', ' ') != d.fields_diff_json#>>'{legal_entity_address_line1, usaspending}' ELSE TRUE END;


-- SELECT
--     count(*)
-- FROM temp_dev3319_transactions_with_diff d
-- WHERE
--     d.system = 'fabs'
--     AND d.action_date >= '2018-10-01' AND
--     CASE WHEN d.fields_diff_json ? 'legal_entity_address_line1' THEN REGEXP_REPLACE(d.fields_diff_json#>>'{legal_entity_address_line1, broker}', E'\s+|\\n', ' ') != d.fields_diff_json#>>'{legal_entity_address_line1, usaspending}' ELSE TRUE END;  -- 2823880



-- SELECT
--     COUNT(*)
-- FROM temp_dev3319_transactions_with_diff d
-- WHERE
--     d.system = 'fabs'
--     AND d.action_date >= '2018-10-01'; -- 2824548 rows

