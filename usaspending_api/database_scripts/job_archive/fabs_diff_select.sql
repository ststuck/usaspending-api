SELECT
    'fabs'::TEXT AS "system",
    usaspending.transaction_id,
    usaspending.published_award_financial_assistance_id AS "broker_surrogate_id",
    usaspending.afa_generated_unique AS "broker_derived_unique_key",
    concat_ws(' ', usaspending.fain, usaspending.uri) AS "piid_fain_uri",
    usaspending.unique_award_key,
    usaspending.action_date::date,
    usaspending.modified_at::date AS "record_last_modified",
    broker.created_at::TIMESTAMP WITHOUT TIME ZONE AS "broker_record_create",
    broker.updated_at::TIMESTAMP WITHOUT TIME ZONE AS "broker_record_update",
    transaction_normalized.create_date::TIMESTAMP WITH TIME ZONE AS "usaspending_record_create",
    transaction_normalized.update_date::TIMESTAMP WITH TIME ZONE AS "usaspending_record_update"
FROM transaction_fabs AS usaspending
INNER JOIN transaction_normalized ON usaspending.transaction_id = transaction_normalized.id
INNER JOIN
(
    SELECT * FROM dblink (
        'broker_server',
        'SELECT
            created_at::TIMESTAMP WITHOUT TIME ZONE,
            updated_at::TIMESTAMP WITHOUT TIME ZONE,
            published_award_financial_assistance_id,
            action_date::date::text,
            UPPER(action_type) AS action_type,
            UPPER(assistance_type) AS assistance_type,
            UPPER(award_description) AS award_description,
            UPPER(awardee_or_recipient_legal) AS awardee_or_recipient_legal,
            UPPER(awardee_or_recipient_uniqu) AS awardee_or_recipient_uniqu,
            UPPER(awarding_agency_code) AS awarding_agency_code,
            UPPER(awarding_office_code) AS awarding_office_code,
            UPPER(awarding_sub_tier_agency_c) AS awarding_sub_tier_agency_c,
            UPPER(award_modification_amendme) AS award_modification_amendme,
            UPPER(business_funds_indicator) AS business_funds_indicator,
            UPPER(business_types) AS business_types,
            UPPER(cfda_number) AS cfda_number,
            UPPER(correction_delete_indicatr) AS correction_delete_indicatr,
            face_value_loan_guarantee::numeric(23,2),
            UPPER(fain) AS fain,
            federal_action_obligation::numeric(23,2),
            UPPER(fiscal_year_and_quarter_co) AS fiscal_year_and_quarter_co,
            UPPER(funding_agency_code) AS funding_agency_code,
            UPPER(funding_office_code) AS funding_office_code,
            UPPER(funding_sub_tier_agency_co) AS funding_sub_tier_agency_co,
            UPPER(REGEXP_REPLACE(legal_entity_address_line1, E''\s+'', '' '')) AS legal_entity_address_line1,
            UPPER(REGEXP_REPLACE(legal_entity_address_line2, E''\s+'', '' '')) AS legal_entity_address_line2,
            UPPER(REGEXP_REPLACE(legal_entity_address_line3, E''\s+'', '' '')) AS legal_entity_address_line3,
            UPPER(REGEXP_REPLACE(legal_entity_country_code, E''\s+'', '' '')) AS legal_entity_country_code,
            UPPER(REGEXP_REPLACE(legal_entity_foreign_city, E''\s+'', '' '')) AS legal_entity_foreign_city,
            UPPER(REGEXP_REPLACE(legal_entity_foreign_posta, E''\s+'', '' '')) AS legal_entity_foreign_posta,
            UPPER(REGEXP_REPLACE(legal_entity_foreign_provi, E''\s+'', '' '')) AS legal_entity_foreign_provi,
            UPPER(REGEXP_REPLACE(legal_entity_zip5, E''\s+'', '' '')) AS legal_entity_zip5,
            UPPER(REGEXP_REPLACE(legal_entity_zip_last4, E''\s+'', '' '')) AS legal_entity_zip_last4,
            non_federal_funding_amount::numeric(23,2),
            original_loan_subsidy_cost::numeric(23,2),
            UPPER(period_of_performance_curr) AS period_of_performance_curr,
            UPPER(period_of_performance_star) AS period_of_performance_star,
            UPPER(REGEXP_REPLACE(place_of_performance_code, E''\s+'', '' '')) AS place_of_performance_code,
            UPPER(REGEXP_REPLACE(place_of_performance_congr, E''\s+'', '' '')) AS place_of_performance_congr,
            UPPER(REGEXP_REPLACE(place_of_perform_country_c, E''\s+'', '' '')) AS place_of_perform_country_c,
            UPPER(REGEXP_REPLACE(place_of_performance_forei, E''\s+'', '' '')) AS place_of_performance_forei,
            UPPER(REGEXP_REPLACE(place_of_performance_zip4a, E''\s+'', '' '')) AS place_of_performance_zip4a,
            record_type,
            UPPER(sai_number) AS sai_number,
            UPPER(uri) AS uri,
            UPPER(REGEXP_REPLACE(legal_entity_congressional, E''\s+'', '' '')) AS legal_entity_congressional,
            total_funding_amount,
            UPPER(cfda_title) AS cfda_title,
            UPPER(awarding_agency_name) AS awarding_agency_name,
            UPPER(awarding_sub_tier_agency_n) AS awarding_sub_tier_agency_n,
            UPPER(funding_agency_name) AS funding_agency_name,
            UPPER(funding_sub_tier_agency_na) AS funding_sub_tier_agency_na,
            is_historical,
            UPPER(REGEXP_REPLACE(place_of_perform_county_na, E''\s+'', '' '')) AS place_of_perform_county_na,
            UPPER(REGEXP_REPLACE(place_of_perform_state_nam, E''\s+'', '' '')) AS place_of_perform_state_nam,
            UPPER(REGEXP_REPLACE(place_of_performance_city, E''\s+'', '' '')) AS place_of_performance_city,
            UPPER(REGEXP_REPLACE(legal_entity_city_name, E''\s+'', '' '')) AS legal_entity_city_name,
            UPPER(REGEXP_REPLACE(legal_entity_county_code, E''\s+'', '' '')) AS legal_entity_county_code,
            UPPER(REGEXP_REPLACE(legal_entity_county_name, E''\s+'', '' '')) AS legal_entity_county_name,
            UPPER(REGEXP_REPLACE(legal_entity_state_code, E''\s+'', '' '')) AS legal_entity_state_code,
            UPPER(REGEXP_REPLACE(legal_entity_state_name, E''\s+'', '' '')) AS legal_entity_state_name,
            modified_at::TIMESTAMP WITHOUT TIME ZONE,
            UPPER(afa_generated_unique) AS afa_generated_unique,
            COALESCE(is_active::boolean, False) AS is_active,
            UPPER(awarding_office_name) AS awarding_office_name,
            UPPER(funding_office_name) AS funding_office_name,
            UPPER(REGEXP_REPLACE(legal_entity_city_code, E''\s+'', '' '')) AS legal_entity_city_code,
            UPPER(REGEXP_REPLACE(legal_entity_foreign_descr, E''\s+'', '' '')) AS legal_entity_foreign_descr,
            UPPER(REGEXP_REPLACE(legal_entity_country_name, E''\s+'', '' '')) AS legal_entity_country_name,
            UPPER(REGEXP_REPLACE(place_of_perform_country_n, E''\s+'', '' '')) AS place_of_perform_country_n,
            UPPER(REGEXP_REPLACE(place_of_perform_county_co, E''\s+'', '' '')) AS place_of_perform_county_co,
            submission_id::text,
            UPPER(REGEXP_REPLACE(place_of_perfor_state_code, E''\s+'', '' '')) AS place_of_perfor_state_code,
            UPPER(REGEXP_REPLACE(place_of_performance_zip5, E''\s+'', '' '')) AS place_of_performance_zip5,
            UPPER(REGEXP_REPLACE(place_of_perform_zip_last4, E''\s+'', '' '')) AS place_of_perform_zip_last4,
            UPPER(action_type_description) AS action_type_description,
            UPPER(assistance_type_desc) AS assistance_type_desc,
            UPPER(business_funds_ind_desc) AS business_funds_ind_desc,
            UPPER(business_types_desc) AS business_types_desc,
            UPPER(correction_delete_ind_desc) AS correction_delete_ind_desc,
            UPPER(record_type_description) AS record_type_description,
            UPPER(ultimate_parent_legal_enti) AS ultimate_parent_legal_enti,
            UPPER(ultimate_parent_unique_ide) AS ultimate_parent_unique_ide,
            UPPER(unique_award_key) AS unique_award_key,
            high_comp_officer1_amount::numeric(23,2),
            UPPER(high_comp_officer1_full_na) AS high_comp_officer1_full_na,
            high_comp_officer2_amount::numeric(23,2),
            UPPER(high_comp_officer2_full_na) AS high_comp_officer2_full_na,
            high_comp_officer3_amount::numeric(23,2),
            UPPER(high_comp_officer3_full_na) AS high_comp_officer3_full_na,
            high_comp_officer4_amount::numeric(23,2),
            UPPER(high_comp_officer4_full_na) AS high_comp_officer4_full_na,
            high_comp_officer5_amount::numeric(23,2),
            UPPER(high_comp_officer5_full_na) AS high_comp_officer5_full_na
        FROM published_award_financial_assistance
        WHERE published_award_financial_assistance_id BETWEEN {minid} AND {maxid}'
    ) AS broker(
        created_at TIMESTAMP WITHOUT TIME ZONE,
        updated_at TIMESTAMP WITHOUT TIME ZONE,
        published_award_financial_assistance_id integer,
        action_date text,
        action_type text,
        assistance_type text,
        award_description text,
        awardee_or_recipient_legal text,
        awardee_or_recipient_uniqu text,
        awarding_agency_code text,
        awarding_office_code text,
        awarding_sub_tier_agency_c text,
        award_modification_amendme text,
        business_funds_indicator text,
        business_types text,
        cfda_number text,
        correction_delete_indicatr text,
        face_value_loan_guarantee numeric(23,2),
        fain text,
        federal_action_obligation numeric(23,2),
        fiscal_year_and_quarter_co text,
        funding_agency_code text,
        funding_office_code text,
        funding_sub_tier_agency_co text,
        legal_entity_address_line1 text,
        legal_entity_address_line2 text,
        legal_entity_address_line3 text,
        legal_entity_country_code text,
        legal_entity_foreign_city text,
        legal_entity_foreign_posta text,
        legal_entity_foreign_provi text,
        legal_entity_zip5 text,
        legal_entity_zip_last4 text,
        non_federal_funding_amount numeric(23,2),
        original_loan_subsidy_cost numeric(23,2),
        period_of_performance_curr text,
        period_of_performance_star text,
        place_of_performance_code text,
        place_of_performance_congr text,
        place_of_perform_country_c text,
        place_of_performance_forei text,
        place_of_performance_zip4a text,
        record_type integer,
        sai_number text,
        uri text,
        legal_entity_congressional text,
        total_funding_amount text,
        cfda_title text,
        awarding_agency_name text,
        awarding_sub_tier_agency_n text,
        funding_agency_name text,
        funding_sub_tier_agency_na text,
        is_historical boolean,
        place_of_perform_county_na text,
        place_of_perform_state_nam text,
        place_of_performance_city text,
        legal_entity_city_name text,
        legal_entity_county_code text,
        legal_entity_county_name text,
        legal_entity_state_code text,
        legal_entity_state_name text,
        modified_at TIMESTAMP WITHOUT TIME ZONE,
        afa_generated_unique text,
        is_active boolean,
        awarding_office_name text,
        funding_office_name text,
        legal_entity_city_code text,
        legal_entity_foreign_descr text,
        legal_entity_country_name text,
        place_of_perform_country_n text,
        place_of_perform_county_co text,
        submission_id text,
        place_of_perfor_state_code text,
        place_of_performance_zip5 text,
        place_of_perform_zip_last4 text,
        action_type_description text,
        assistance_type_desc text,
        business_funds_ind_desc text,
        business_types_desc text,
        correction_delete_ind_desc text,
        record_type_description text,
        ultimate_parent_legal_enti text,
        ultimate_parent_unique_ide text,
        unique_award_key text,
        high_comp_officer1_amount numeric(23,2),
        high_comp_officer1_full_na text,
        high_comp_officer2_amount numeric(23,2),
        high_comp_officer2_full_na text,
        high_comp_officer3_amount numeric(23,2),
        high_comp_officer3_full_na text,
        high_comp_officer4_amount numeric(23,2),
        high_comp_officer4_full_na text,
        high_comp_officer5_amount numeric(23,2),
        high_comp_officer5_full_na text
    )
) AS broker ON (
    (broker.published_award_financial_assistance_id = usaspending.published_award_financial_assistance_id)
    AND (
         (broker.created_at IS DISTINCT FROM usaspending.created_at::TIMESTAMP WITHOUT TIME ZONE)
        OR (broker.updated_at IS DISTINCT FROM usaspending.updated_at::TIMESTAMP WITHOUT TIME ZONE)
        OR (broker.action_date IS DISTINCT FROM usaspending.action_date::date::text)
        OR (broker.action_type IS DISTINCT FROM usaspending.action_type)
        OR (broker.assistance_type IS DISTINCT FROM usaspending.assistance_type)
        OR (broker.award_description IS DISTINCT FROM usaspending.award_description)
        OR (broker.awardee_or_recipient_legal IS DISTINCT FROM usaspending.awardee_or_recipient_legal)
        OR (broker.awardee_or_recipient_uniqu IS DISTINCT FROM usaspending.awardee_or_recipient_uniqu)
        OR (broker.awarding_agency_code IS DISTINCT FROM usaspending.awarding_agency_code)
        OR (broker.awarding_office_code IS DISTINCT FROM usaspending.awarding_office_code)
        OR (broker.awarding_sub_tier_agency_c IS DISTINCT FROM usaspending.awarding_sub_tier_agency_c)
        OR (broker.award_modification_amendme IS DISTINCT FROM usaspending.award_modification_amendme)
        OR (broker.business_funds_indicator IS DISTINCT FROM usaspending.business_funds_indicator)
        OR (broker.business_types IS DISTINCT FROM usaspending.business_types)
        OR (broker.cfda_number IS DISTINCT FROM usaspending.cfda_number)
        OR (broker.correction_delete_indicatr IS DISTINCT FROM usaspending.correction_delete_indicatr)
        OR (broker.face_value_loan_guarantee IS DISTINCT FROM usaspending.face_value_loan_guarantee)
        OR (broker.fain IS DISTINCT FROM usaspending.fain)
        OR (broker.federal_action_obligation IS DISTINCT FROM usaspending.federal_action_obligation)
        OR (broker.fiscal_year_and_quarter_co IS DISTINCT FROM usaspending.fiscal_year_and_quarter_co)
        OR (broker.funding_agency_code IS DISTINCT FROM usaspending.funding_agency_code)
        OR (broker.funding_office_code IS DISTINCT FROM usaspending.funding_office_code)
        OR (broker.funding_sub_tier_agency_co IS DISTINCT FROM usaspending.funding_sub_tier_agency_co)
        OR (broker.legal_entity_address_line1 IS DISTINCT FROM usaspending.legal_entity_address_line1)
        OR (broker.legal_entity_address_line2 IS DISTINCT FROM usaspending.legal_entity_address_line2)
        OR (broker.legal_entity_address_line3 IS DISTINCT FROM usaspending.legal_entity_address_line3)
        OR (broker.legal_entity_country_code IS DISTINCT FROM usaspending.legal_entity_country_code)
        OR (broker.legal_entity_foreign_city IS DISTINCT FROM usaspending.legal_entity_foreign_city)
        OR (broker.legal_entity_foreign_posta IS DISTINCT FROM usaspending.legal_entity_foreign_posta)
        OR (broker.legal_entity_foreign_provi IS DISTINCT FROM usaspending.legal_entity_foreign_provi)
        OR (broker.legal_entity_zip5 IS DISTINCT FROM usaspending.legal_entity_zip5)
        OR (broker.legal_entity_zip_last4 IS DISTINCT FROM usaspending.legal_entity_zip_last4)
        OR (broker.non_federal_funding_amount IS DISTINCT FROM usaspending.non_federal_funding_amount)
        OR (broker.original_loan_subsidy_cost IS DISTINCT FROM usaspending.original_loan_subsidy_cost)
        OR (broker.period_of_performance_curr IS DISTINCT FROM usaspending.period_of_performance_curr)
        OR (broker.period_of_performance_star IS DISTINCT FROM usaspending.period_of_performance_star)
        OR (broker.place_of_performance_code IS DISTINCT FROM usaspending.place_of_performance_code)
        OR (broker.place_of_performance_congr IS DISTINCT FROM usaspending.place_of_performance_congr)
        OR (broker.place_of_perform_country_c IS DISTINCT FROM usaspending.place_of_perform_country_c)
        OR (broker.place_of_performance_forei IS DISTINCT FROM usaspending.place_of_performance_forei)
        OR (broker.place_of_performance_zip4a IS DISTINCT FROM usaspending.place_of_performance_zip4a)
        OR (broker.record_type IS DISTINCT FROM usaspending.record_type)
        OR (broker.sai_number IS DISTINCT FROM usaspending.sai_number)
        OR (broker.uri IS DISTINCT FROM usaspending.uri)
        OR (broker.legal_entity_congressional IS DISTINCT FROM usaspending.legal_entity_congressional)
        OR (broker.total_funding_amount IS DISTINCT FROM usaspending.total_funding_amount)
        OR (broker.cfda_title IS DISTINCT FROM usaspending.cfda_title)
        OR (broker.awarding_agency_name IS DISTINCT FROM usaspending.awarding_agency_name)
        OR (broker.awarding_sub_tier_agency_n IS DISTINCT FROM usaspending.awarding_sub_tier_agency_n)
        OR (broker.funding_agency_name IS DISTINCT FROM usaspending.funding_agency_name)
        OR (broker.funding_sub_tier_agency_na IS DISTINCT FROM usaspending.funding_sub_tier_agency_na)
        OR (broker.is_historical IS DISTINCT FROM usaspending.is_historical)
        OR (broker.place_of_perform_county_na IS DISTINCT FROM usaspending.place_of_perform_county_na)
        OR (broker.place_of_perform_state_nam IS DISTINCT FROM usaspending.place_of_perform_state_nam)
        OR (broker.place_of_performance_city IS DISTINCT FROM usaspending.place_of_performance_city)
        OR (broker.legal_entity_city_name IS DISTINCT FROM usaspending.legal_entity_city_name)
        OR (broker.legal_entity_county_code IS DISTINCT FROM usaspending.legal_entity_county_code)
        OR (broker.legal_entity_county_name IS DISTINCT FROM usaspending.legal_entity_county_name)
        OR (broker.legal_entity_state_code IS DISTINCT FROM usaspending.legal_entity_state_code)
        OR (broker.legal_entity_state_name IS DISTINCT FROM usaspending.legal_entity_state_name)
        OR (broker.modified_at IS DISTINCT FROM usaspending.modified_at::TIMESTAMP WITHOUT TIME ZONE)
        OR (broker.afa_generated_unique IS DISTINCT FROM usaspending.afa_generated_unique)
        OR (broker.is_active IS DISTINCT FROM usaspending.is_active)
        OR (broker.awarding_office_name IS DISTINCT FROM usaspending.awarding_office_name)
        OR (broker.funding_office_name IS DISTINCT FROM usaspending.funding_office_name)
        OR (broker.legal_entity_city_code IS DISTINCT FROM usaspending.legal_entity_city_code)
        OR (broker.legal_entity_foreign_descr IS DISTINCT FROM usaspending.legal_entity_foreign_descr)
        OR (broker.legal_entity_country_name IS DISTINCT FROM usaspending.legal_entity_country_name)
        OR (broker.place_of_perform_country_n IS DISTINCT FROM usaspending.place_of_perform_country_n)
        OR (broker.place_of_perform_county_co IS DISTINCT FROM usaspending.place_of_perform_county_co)
        OR (broker.submission_id IS DISTINCT FROM usaspending.submission_id::text)
        OR (broker.place_of_perfor_state_code IS DISTINCT FROM usaspending.place_of_perfor_state_code)
        OR (broker.place_of_performance_zip5 IS DISTINCT FROM usaspending.place_of_performance_zip5)
        OR (broker.place_of_perform_zip_last4 IS DISTINCT FROM usaspending.place_of_perform_zip_last4)
        OR (broker.action_type_description IS DISTINCT FROM usaspending.action_type_description)
        OR (broker.assistance_type_desc IS DISTINCT FROM usaspending.assistance_type_desc)
        OR (broker.business_funds_ind_desc IS DISTINCT FROM usaspending.business_funds_ind_desc)
        OR (broker.business_types_desc IS DISTINCT FROM usaspending.business_types_desc)
        OR (broker.correction_delete_ind_desc IS DISTINCT FROM usaspending.correction_delete_ind_desc)
        OR (broker.record_type_description IS DISTINCT FROM usaspending.record_type_description)
        OR (broker.ultimate_parent_legal_enti IS DISTINCT FROM usaspending.ultimate_parent_legal_enti)
        OR (broker.ultimate_parent_unique_ide IS DISTINCT FROM usaspending.ultimate_parent_unique_ide)
        OR (broker.unique_award_key IS DISTINCT FROM usaspending.unique_award_key)
        OR (broker.high_comp_officer1_amount IS DISTINCT FROM usaspending.officer_1_amount)
        OR (broker.high_comp_officer1_full_na IS DISTINCT FROM usaspending.officer_1_name)
        OR (broker.high_comp_officer2_amount IS DISTINCT FROM usaspending.officer_2_amount)
        OR (broker.high_comp_officer2_full_na IS DISTINCT FROM usaspending.officer_2_name)
        OR (broker.high_comp_officer3_amount IS DISTINCT FROM usaspending.officer_3_amount)
        OR (broker.high_comp_officer3_full_na IS DISTINCT FROM usaspending.officer_3_name)
        OR (broker.high_comp_officer4_amount IS DISTINCT FROM usaspending.officer_4_amount)
        OR (broker.high_comp_officer4_full_na IS DISTINCT FROM usaspending.officer_4_name)
        OR (broker.high_comp_officer5_amount IS DISTINCT FROM usaspending.officer_5_amount)
        OR (broker.high_comp_officer5_full_na IS DISTINCT FROM usaspending.officer_5_name)
    )
)