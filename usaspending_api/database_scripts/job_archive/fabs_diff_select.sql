SELECT
    'fabs'::TEXT as "system",
    usaspending.transaction_id,
    usaspending.published_award_financial_assistance_id AS "broker_surrogate_id",
    usaspending.afa_generated_unique AS "broker_derived_unique_key",
    concat(usaspending.fain, usaspending.uri) AS "piid_fain_uri",
    usaspending.unique_award_key,
    usaspending.action_date::date,
    usaspending.modified_at::date AS "record_last_modified",
    broker.created_at::timestamp with time zone AS "broker_record_create",
    broker.updated_at::timestamp with time zone AS "broker_record_update",
    transaction_normalized.create_date::timestamp with time zone AS "usaspending_record_create",
    transaction_normalized.update_date::timestamp with time zone AS "usaspending_record_update"
FROM transaction_fabs AS usaspending
INNER JOIN transaction_normalized ON usaspending.transaction_id = transaction_normalized.id
INNER JOIN
(
    SELECT * FROM dblink (
        'broker_server',
        'SELECT * FROM published_award_financial_assistance WHERE published_award_financial_assistance_id BETWEEN {minid} AND {maxid}'
    ) AS broker(
        created_at timestamp without time zone,
        updated_at timestamp without time zone,
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
        face_value_loan_guarantee numeric,
        fain text,
        federal_action_obligation numeric,
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
        non_federal_funding_amount numeric,
        original_loan_subsidy_cost numeric,
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
        modified_at timestamp without time zone,
        afa_generated_unique text,
        is_active boolean,
        awarding_office_name text,
        funding_office_name text,
        legal_entity_city_code text,
        legal_entity_foreign_descr text,
        legal_entity_country_name text,
        place_of_perform_country_n text,
        place_of_perform_county_co text,
        submission_id numeric,
        place_of_perfor_state_code text,
        place_of_performance_zip5 text,
        place_of_perform_zip_last4 text,
        business_categories text[],
        action_type_description text,
        assistance_type_desc text,
        business_funds_ind_desc text,
        business_types_desc text,
        correction_delete_ind_desc text,
        record_type_description text,
        ultimate_parent_legal_enti text,
        ultimate_parent_unique_ide text,
        unique_award_key text,
        high_comp_officer1_amount text,
        high_comp_officer1_full_na text,
        high_comp_officer2_amount text,
        high_comp_officer2_full_na text,
        high_comp_officer3_amount text,
        high_comp_officer3_full_na text,
        high_comp_officer4_amount text,
        high_comp_officer4_full_na text,
        high_comp_officer5_amount text,
        high_comp_officer5_full_na text)
    ) AS broker ON (
        (broker.published_award_financial_assistance_id = usaspending.published_award_financial_assistance_id) AND (
        (broker.action_date IS DISTINCT FROM usaspending.action_date) OR
        (broker.action_type IS DISTINCT FROM usaspending.action_type) OR
        (broker.assistance_type IS DISTINCT FROM usaspending.assistance_type) OR
        (broker.award_description IS DISTINCT FROM usaspending.award_description) OR
        (broker.awardee_or_recipient_legal IS DISTINCT FROM usaspending.awardee_or_recipient_legal) OR
        (broker.awardee_or_recipient_uniqu IS DISTINCT FROM usaspending.awardee_or_recipient_uniqu) OR
        (broker.awarding_agency_code IS DISTINCT FROM usaspending.awarding_agency_code) OR
        (broker.awarding_office_code IS DISTINCT FROM usaspending.awarding_office_code) OR
        (broker.awarding_sub_tier_agency_c IS DISTINCT FROM usaspending.awarding_sub_tier_agency_c) OR
        (broker.award_modification_amendme IS DISTINCT FROM usaspending.award_modification_amendme) OR
        (broker.business_funds_indicator IS DISTINCT FROM usaspending.business_funds_indicator) OR
        (broker.cfda_number IS DISTINCT FROM usaspending.business_types) OR
        (broker.correction_delete_indicatr IS DISTINCT FROM usaspending.correction_delete_indicatr) OR
        (broker.face_value_loan_guarantee IS DISTINCT FROM usaspending.face_value_loan_guarantee) OR
        (broker.fain IS DISTINCT FROM usaspending.fain) OR
        (broker.federal_action_obligation IS DISTINCT FROM usaspending.federal_action_obligation) OR
        (broker.fiscal_year_and_quarter_co IS DISTINCT FROM usaspending.fiscal_year_and_quarter_co) OR
        (broker.funding_agency_code IS DISTINCT FROM usaspending.funding_agency_code) OR
        (broker.funding_office_code IS DISTINCT FROM usaspending.funding_office_code) OR
        (broker.funding_sub_tier_agency_co IS DISTINCT FROM usaspending.funding_sub_tier_agency_co) OR
        (broker.legal_entity_address_line1 IS DISTINCT FROM usaspending.legal_entity_address_line1) OR
        (broker.legal_entity_address_line2 IS DISTINCT FROM usaspending.legal_entity_address_line2) OR
        (broker.legal_entity_address_line3 IS DISTINCT FROM usaspending.legal_entity_address_line3) OR
        (broker.legal_entity_country_code IS DISTINCT FROM usaspending.legal_entity_country_code) OR
        (broker.legal_entity_foreign_city IS DISTINCT FROM usaspending.legal_entity_foreign_city) OR
        (broker.legal_entity_foreign_posta IS DISTINCT FROM usaspending.legal_entity_foreign_posta) OR
        (broker.legal_entity_foreign_provi IS DISTINCT FROM usaspending.legal_entity_foreign_provi) OR
        (broker.legal_entity_zip5 IS DISTINCT FROM usaspending.legal_entity_zip5) OR
        (broker.legal_entity_zip_last4 IS DISTINCT FROM usaspending.legal_entity_zip_last4) OR
        (broker.non_federal_funding_amount IS DISTINCT FROM usaspending.non_federal_funding_amount) OR
        (broker.original_loan_subsidy_cost IS DISTINCT FROM usaspending.original_loan_subsidy_cost) OR
        (broker.period_of_performance_curr IS DISTINCT FROM usaspending.period_of_performance_curr) OR
        (broker.period_of_performance_star IS DISTINCT FROM usaspending.period_of_performance_star) OR
        (broker.place_of_performance_code IS DISTINCT FROM usaspending.place_of_performance_code) OR
        (broker.place_of_performance_congr IS DISTINCT FROM usaspending.place_of_performance_congr) OR
        (broker.place_of_perform_country_c IS DISTINCT FROM usaspending.place_of_perform_country_c) OR
        (broker.place_of_performance_forei IS DISTINCT FROM usaspending.place_of_performance_forei) OR
        (broker.place_of_performance_zip4a IS DISTINCT FROM usaspending.place_of_performance_zip4a) OR
        (broker.record_type IS DISTINCT FROM usaspending.record_type) OR
        (broker.sai_number IS DISTINCT FROM usaspending.sai_number) OR
        (broker.uri IS DISTINCT FROM usaspending.uri) OR
        (broker.legal_entity_congressional IS DISTINCT FROM usaspending.legal_entity_congressional) OR
        (broker.total_funding_amount IS DISTINCT FROM usaspending.total_funding_amount) OR
        (broker.cfda_title IS DISTINCT FROM usaspending.cfda_title) OR
        (broker.awarding_agency_name IS DISTINCT FROM usaspending.awarding_agency_name) OR
        (broker.awarding_sub_tier_agency_n IS DISTINCT FROM usaspending.awarding_sub_tier_agency_n) OR
        (broker.funding_agency_name IS DISTINCT FROM usaspending.funding_agency_name) OR
        (broker.funding_sub_tier_agency_na IS DISTINCT FROM usaspending.funding_sub_tier_agency_na) OR
        (broker.is_historical IS DISTINCT FROM usaspending.is_historical) OR
        (broker.place_of_perform_county_na IS DISTINCT FROM usaspending.place_of_perform_county_na) OR
        (broker.place_of_perform_state_nam IS DISTINCT FROM usaspending.place_of_perform_state_nam) OR
        (broker.place_of_performance_city IS DISTINCT FROM usaspending.place_of_performance_city) OR
        (broker.legal_entity_city_name IS DISTINCT FROM usaspending.legal_entity_city_name) OR
        (broker.legal_entity_county_code IS DISTINCT FROM usaspending.legal_entity_county_code) OR
        (broker.legal_entity_county_name IS DISTINCT FROM usaspending.legal_entity_county_name) OR
        (broker.legal_entity_state_code IS DISTINCT FROM usaspending.legal_entity_state_code) OR
        (broker.legal_entity_state_name IS DISTINCT FROM usaspending.legal_entity_state_name) OR
        (broker.modified_at IS DISTINCT FROM usaspending.modified_at) OR
        (broker.afa_generated_unique IS DISTINCT FROM usaspending.afa_generated_unique) OR
        (broker.is_active IS DISTINCT FROM usaspending.is_active) OR
        (broker.awarding_office_name IS DISTINCT FROM usaspending.awarding_office_name) OR
        (broker.funding_office_name IS DISTINCT FROM usaspending.funding_office_name) OR
        (broker.legal_entity_city_code IS DISTINCT FROM usaspending.legal_entity_city_code) OR
        (broker.legal_entity_foreign_descr IS DISTINCT FROM usaspending.legal_entity_foreign_descr) OR
        (broker.legal_entity_country_name IS DISTINCT FROM usaspending.legal_entity_country_name) OR
        (broker.place_of_perform_country_n IS DISTINCT FROM usaspending.place_of_perform_country_n) OR
        (broker.place_of_perform_county_co IS DISTINCT FROM usaspending.place_of_perform_county_co) OR
        (broker.submission_id IS DISTINCT FROM usaspending.submission_id) OR
        (broker.place_of_perfor_state_code IS DISTINCT FROM usaspending.place_of_perfor_state_code) OR
        (broker.place_of_performance_zip5 IS DISTINCT FROM usaspending.place_of_performance_zip5) OR
        (broker.place_of_perform_zip_last4 IS DISTINCT FROM usaspending.place_of_perform_zip_last4) OR
        (broker.action_type_description IS DISTINCT FROM usaspending.action_type_description) OR
        (broker.assistance_type_desc IS DISTINCT FROM usaspending.assistance_type_desc) OR
        (broker.business_funds_ind_desc IS DISTINCT FROM usaspending.business_funds_ind_desc) OR
        (broker.business_types_desc IS DISTINCT FROM usaspending.business_types_desc) OR
        (broker.correction_delete_ind_desc IS DISTINCT FROM usaspending.correction_delete_ind_desc) OR
        (broker.record_type_description IS DISTINCT FROM usaspending.record_type_description) OR
        (broker.ultimate_parent_legal_enti IS DISTINCT FROM usaspending.ultimate_parent_legal_enti) OR
        (broker.ultimate_parent_unique_ide IS DISTINCT FROM usaspending.ultimate_parent_unique_ide) OR
        (broker.unique_award_key IS DISTINCT FROM usaspending.unique_award_key)
    )
)