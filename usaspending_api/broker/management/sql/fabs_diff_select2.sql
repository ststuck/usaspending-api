SELECT
    'fabs' AS "system",
    usaspending.published_award_financial_assistance_id AS "broker_surrogate_id",
    usaspending.afa_generated_unique AS "broker_derived_unique_key",
    concat_ws(' ', usaspending.fain, usaspending.uri) AS "piid_fain_uri",
    usaspending.unique_award_key,
    usaspending.action_date::date,
    usaspending.updated_at AS "record_last_modified",
    usaspending.created_at AS "record_created",
    jsonb_strip_nulls(
        jsonb_build_object(
            'created_at', CASE WHEN broker.created_at IS DISTINCT FROM usaspending.created_at THEN jsonb_build_object('broker', broker.created_at, 'usaspending', usaspending.created_at) ELSE null END,
            'updated_at', CASE WHEN broker.updated_at IS DISTINCT FROM usaspending.updated_at THEN jsonb_build_object('broker', broker.updated_at, 'usaspending', usaspending.updated_at) ELSE null END,
            'action_date', CASE WHEN broker.action_date IS DISTINCT FROM usaspending.action_date THEN jsonb_build_object('broker', broker.action_date, 'usaspending', usaspending.action_date) ELSE null END,
            'action_type', CASE WHEN broker.action_type IS DISTINCT FROM usaspending.action_type THEN jsonb_build_object('broker', broker.action_type, 'usaspending', usaspending.action_type) ELSE null END,
            'assistance_type', CASE WHEN broker.assistance_type IS DISTINCT FROM usaspending.assistance_type THEN jsonb_build_object('broker', broker.assistance_type, 'usaspending', usaspending.assistance_type) ELSE null END,
            'award_description', CASE WHEN broker.award_description IS DISTINCT FROM usaspending.award_description THEN jsonb_build_object('broker', broker.award_description, 'usaspending', usaspending.award_description) ELSE null END,
            'awardee_or_recipient_legal', CASE WHEN broker.awardee_or_recipient_legal IS DISTINCT FROM usaspending.awardee_or_recipient_legal THEN jsonb_build_object('broker', broker.awardee_or_recipient_legal, 'usaspending', usaspending.awardee_or_recipient_legal) ELSE null END,
            'awardee_or_recipient_uniqu', CASE WHEN broker.awardee_or_recipient_uniqu IS DISTINCT FROM usaspending.awardee_or_recipient_uniqu THEN jsonb_build_object('broker', broker.awardee_or_recipient_uniqu, 'usaspending', usaspending.awardee_or_recipient_uniqu) ELSE null END,
            'awarding_agency_code', CASE WHEN broker.awarding_agency_code IS DISTINCT FROM usaspending.awarding_agency_code THEN jsonb_build_object('broker', broker.awarding_agency_code, 'usaspending', usaspending.awarding_agency_code) ELSE null END,
            'awarding_office_code', CASE WHEN broker.awarding_office_code IS DISTINCT FROM usaspending.awarding_office_code THEN jsonb_build_object('broker', broker.awarding_office_code, 'usaspending', usaspending.awarding_office_code) ELSE null END,
            'awarding_sub_tier_agency_c', CASE WHEN broker.awarding_sub_tier_agency_c IS DISTINCT FROM usaspending.awarding_sub_tier_agency_c THEN jsonb_build_object('broker', broker.awarding_sub_tier_agency_c, 'usaspending', usaspending.awarding_sub_tier_agency_c) ELSE null END,
            'award_modification_amendme', CASE WHEN broker.award_modification_amendme IS DISTINCT FROM usaspending.award_modification_amendme THEN jsonb_build_object('broker', broker.award_modification_amendme, 'usaspending', usaspending.award_modification_amendme) ELSE null END,
            'business_funds_indicator', CASE WHEN broker.business_funds_indicator IS DISTINCT FROM usaspending.business_funds_indicator THEN jsonb_build_object('broker', broker.business_funds_indicator, 'usaspending', usaspending.business_funds_indicator) ELSE null END,
            'business_types', CASE WHEN broker.business_types IS DISTINCT FROM usaspending.business_types THEN jsonb_build_object('broker', broker.business_types, 'usaspending', usaspending.business_types) ELSE null END,
            'cfda_number', CASE WHEN broker.cfda_number IS DISTINCT FROM usaspending.cfda_number THEN jsonb_build_object('broker', broker.cfda_number, 'usaspending', usaspending.cfda_number) ELSE null END,
            'correction_delete_indicatr', CASE WHEN broker.correction_delete_indicatr IS DISTINCT FROM usaspending.correction_delete_indicatr THEN jsonb_build_object('broker', broker.correction_delete_indicatr, 'usaspending', usaspending.correction_delete_indicatr) ELSE null END,
            'face_value_loan_guarantee', CASE WHEN broker.face_value_loan_guarantee IS DISTINCT FROM usaspending.face_value_loan_guarantee THEN jsonb_build_object('broker', broker.face_value_loan_guarantee, 'usaspending', usaspending.face_value_loan_guarantee) ELSE null END,
            'fain', CASE WHEN broker.fain IS DISTINCT FROM usaspending.fain THEN jsonb_build_object('broker', broker.fain, 'usaspending', usaspending.fain) ELSE null END,
            'federal_action_obligation', CASE WHEN broker.federal_action_obligation IS DISTINCT FROM usaspending.federal_action_obligation THEN jsonb_build_object('broker', broker.federal_action_obligation, 'usaspending', usaspending.federal_action_obligation) ELSE null END,
            'fiscal_year_and_quarter_co', CASE WHEN broker.fiscal_year_and_quarter_co IS DISTINCT FROM usaspending.fiscal_year_and_quarter_co THEN jsonb_build_object('broker', broker.fiscal_year_and_quarter_co, 'usaspending', usaspending.fiscal_year_and_quarter_co) ELSE null END,
            'funding_agency_code', CASE WHEN broker.funding_agency_code IS DISTINCT FROM usaspending.funding_agency_code THEN jsonb_build_object('broker', broker.funding_agency_code, 'usaspending', usaspending.funding_agency_code) ELSE null END,
            'funding_office_code', CASE WHEN broker.funding_office_code IS DISTINCT FROM usaspending.funding_office_code THEN jsonb_build_object('broker', broker.funding_office_code, 'usaspending', usaspending.funding_office_code) ELSE null END,
            'funding_sub_tier_agency_co', CASE WHEN broker.funding_sub_tier_agency_co IS DISTINCT FROM usaspending.funding_sub_tier_agency_co THEN jsonb_build_object('broker', broker.funding_sub_tier_agency_co, 'usaspending', usaspending.funding_sub_tier_agency_co) ELSE null END,
            'legal_entity_address_line1', CASE WHEN broker.legal_entity_address_line1 IS DISTINCT FROM usaspending.legal_entity_address_line1 THEN jsonb_build_object('broker', broker.legal_entity_address_line1, 'usaspending', usaspending.legal_entity_address_line1) ELSE null END,
            'legal_entity_address_line2', CASE WHEN broker.legal_entity_address_line2 IS DISTINCT FROM usaspending.legal_entity_address_line2 THEN jsonb_build_object('broker', broker.legal_entity_address_line2, 'usaspending', usaspending.legal_entity_address_line2) ELSE null END,
            'legal_entity_address_line3', CASE WHEN broker.legal_entity_address_line3 IS DISTINCT FROM usaspending.legal_entity_address_line3 THEN jsonb_build_object('broker', broker.legal_entity_address_line3, 'usaspending', usaspending.legal_entity_address_line3) ELSE null END,
            'legal_entity_country_code', CASE WHEN broker.legal_entity_country_code IS DISTINCT FROM usaspending.legal_entity_country_code THEN jsonb_build_object('broker', broker.legal_entity_country_code, 'usaspending', usaspending.legal_entity_country_code) ELSE null END,
            'legal_entity_foreign_city', CASE WHEN broker.legal_entity_foreign_city IS DISTINCT FROM usaspending.legal_entity_foreign_city THEN jsonb_build_object('broker', broker.legal_entity_foreign_city, 'usaspending', usaspending.legal_entity_foreign_city) ELSE null END,
            'legal_entity_foreign_posta', CASE WHEN broker.legal_entity_foreign_posta IS DISTINCT FROM usaspending.legal_entity_foreign_posta THEN jsonb_build_object('broker', broker.legal_entity_foreign_posta, 'usaspending', usaspending.legal_entity_foreign_posta) ELSE null END,
            'legal_entity_foreign_provi', CASE WHEN broker.legal_entity_foreign_provi IS DISTINCT FROM usaspending.legal_entity_foreign_provi THEN jsonb_build_object('broker', broker.legal_entity_foreign_provi, 'usaspending', usaspending.legal_entity_foreign_provi) ELSE null END,
            'legal_entity_zip5', CASE WHEN broker.legal_entity_zip5 IS DISTINCT FROM usaspending.legal_entity_zip5 THEN jsonb_build_object('broker', broker.legal_entity_zip5, 'usaspending', usaspending.legal_entity_zip5) ELSE null END,
            'legal_entity_zip_last4', CASE WHEN broker.legal_entity_zip_last4 IS DISTINCT FROM usaspending.legal_entity_zip_last4 THEN jsonb_build_object('broker', broker.legal_entity_zip_last4, 'usaspending', usaspending.legal_entity_zip_last4) ELSE null END,
            'non_federal_funding_amount', CASE WHEN broker.non_federal_funding_amount IS DISTINCT FROM usaspending.non_federal_funding_amount THEN jsonb_build_object('broker', broker.non_federal_funding_amount, 'usaspending', usaspending.non_federal_funding_amount) ELSE null END,
            'original_loan_subsidy_cost', CASE WHEN broker.original_loan_subsidy_cost IS DISTINCT FROM usaspending.original_loan_subsidy_cost THEN jsonb_build_object('broker', broker.original_loan_subsidy_cost, 'usaspending', usaspending.original_loan_subsidy_cost) ELSE null END,
            'period_of_performance_curr', CASE WHEN broker.period_of_performance_curr IS DISTINCT FROM usaspending.period_of_performance_curr THEN jsonb_build_object('broker', broker.period_of_performance_curr, 'usaspending', usaspending.period_of_performance_curr) ELSE null END,
            'period_of_performance_star', CASE WHEN broker.period_of_performance_star IS DISTINCT FROM usaspending.period_of_performance_star THEN jsonb_build_object('broker', broker.period_of_performance_star, 'usaspending', usaspending.period_of_performance_star) ELSE null END,
            'place_of_performance_code', CASE WHEN broker.place_of_performance_code IS DISTINCT FROM usaspending.place_of_performance_code THEN jsonb_build_object('broker', broker.place_of_performance_code, 'usaspending', usaspending.place_of_performance_code) ELSE null END,
            'place_of_performance_congr', CASE WHEN broker.place_of_performance_congr IS DISTINCT FROM usaspending.place_of_performance_congr THEN jsonb_build_object('broker', broker.place_of_performance_congr, 'usaspending', usaspending.place_of_performance_congr) ELSE null END,
            'place_of_perform_country_c', CASE WHEN broker.place_of_perform_country_c IS DISTINCT FROM usaspending.place_of_perform_country_c THEN jsonb_build_object('broker', broker.place_of_perform_country_c, 'usaspending', usaspending.place_of_perform_country_c) ELSE null END,
            'place_of_performance_forei', CASE WHEN broker.place_of_performance_forei IS DISTINCT FROM usaspending.place_of_performance_forei THEN jsonb_build_object('broker', broker.place_of_performance_forei, 'usaspending', usaspending.place_of_performance_forei) ELSE null END,
            'place_of_performance_zip4a', CASE WHEN broker.place_of_performance_zip4a IS DISTINCT FROM usaspending.place_of_performance_zip4a THEN jsonb_build_object('broker', broker.place_of_performance_zip4a, 'usaspending', usaspending.place_of_performance_zip4a) ELSE null END,
            'record_type', CASE WHEN broker.record_type IS DISTINCT FROM usaspending.record_type THEN jsonb_build_object('broker', broker.record_type, 'usaspending', usaspending.record_type) ELSE null END,
            'sai_number', CASE WHEN broker.sai_number IS DISTINCT FROM usaspending.sai_number THEN jsonb_build_object('broker', broker.sai_number, 'usaspending', usaspending.sai_number) ELSE null END,
            'uri', CASE WHEN broker.uri IS DISTINCT FROM usaspending.uri THEN jsonb_build_object('broker', broker.uri, 'usaspending', usaspending.uri) ELSE null END,
            'legal_entity_congressional', CASE WHEN broker.legal_entity_congressional IS DISTINCT FROM usaspending.legal_entity_congressional THEN jsonb_build_object('broker', broker.legal_entity_congressional, 'usaspending', usaspending.legal_entity_congressional) ELSE null END,
            'total_funding_amount', CASE WHEN broker.total_funding_amount IS DISTINCT FROM usaspending.total_funding_amount THEN jsonb_build_object('broker', broker.total_funding_amount, 'usaspending', usaspending.total_funding_amount) ELSE null END,
            'cfda_title', CASE WHEN broker.cfda_title IS DISTINCT FROM usaspending.cfda_title THEN jsonb_build_object('broker', broker.cfda_title, 'usaspending', usaspending.cfda_title) ELSE null END
        ) || jsonb_build_object(
            'awarding_agency_name', CASE WHEN broker.awarding_agency_name IS DISTINCT FROM usaspending.awarding_agency_name THEN jsonb_build_object('broker', broker.awarding_agency_name, 'usaspending', usaspending.awarding_agency_name) ELSE null END,
            'awarding_sub_tier_agency_n', CASE WHEN broker.awarding_sub_tier_agency_n IS DISTINCT FROM usaspending.awarding_sub_tier_agency_n THEN jsonb_build_object('broker', broker.awarding_sub_tier_agency_n, 'usaspending', usaspending.awarding_sub_tier_agency_n) ELSE null END,
            'funding_agency_name', CASE WHEN broker.funding_agency_name IS DISTINCT FROM usaspending.funding_agency_name THEN jsonb_build_object('broker', broker.funding_agency_name, 'usaspending', usaspending.funding_agency_name) ELSE null END,
            'funding_sub_tier_agency_na', CASE WHEN broker.funding_sub_tier_agency_na IS DISTINCT FROM usaspending.funding_sub_tier_agency_na THEN jsonb_build_object('broker', broker.funding_sub_tier_agency_na, 'usaspending', usaspending.funding_sub_tier_agency_na) ELSE null END,
            'is_historical', CASE WHEN broker.is_historical IS DISTINCT FROM usaspending.is_historical THEN jsonb_build_object('broker', broker.is_historical, 'usaspending', usaspending.is_historical) ELSE null END,
            'place_of_perform_county_na', CASE WHEN broker.place_of_perform_county_na IS DISTINCT FROM usaspending.place_of_perform_county_na THEN jsonb_build_object('broker', broker.place_of_perform_county_na, 'usaspending', usaspending.place_of_perform_county_na) ELSE null END,
            'place_of_perform_state_nam', CASE WHEN broker.place_of_perform_state_nam IS DISTINCT FROM usaspending.place_of_perform_state_nam THEN jsonb_build_object('broker', broker.place_of_perform_state_nam, 'usaspending', usaspending.place_of_perform_state_nam) ELSE null END,
            'place_of_performance_city', CASE WHEN broker.place_of_performance_city IS DISTINCT FROM usaspending.place_of_performance_city THEN jsonb_build_object('broker', broker.place_of_performance_city, 'usaspending', usaspending.place_of_performance_city) ELSE null END,
            'legal_entity_city_name', CASE WHEN broker.legal_entity_city_name IS DISTINCT FROM usaspending.legal_entity_city_name THEN jsonb_build_object('broker', broker.legal_entity_city_name, 'usaspending', usaspending.legal_entity_city_name) ELSE null END,
            'legal_entity_county_code', CASE WHEN broker.legal_entity_county_code IS DISTINCT FROM usaspending.legal_entity_county_code THEN jsonb_build_object('broker', broker.legal_entity_county_code, 'usaspending', usaspending.legal_entity_county_code) ELSE null END,
            'legal_entity_county_name', CASE WHEN broker.legal_entity_county_name IS DISTINCT FROM usaspending.legal_entity_county_name THEN jsonb_build_object('broker', broker.legal_entity_county_name, 'usaspending', usaspending.legal_entity_county_name) ELSE null END,
            'legal_entity_state_code', CASE WHEN broker.legal_entity_state_code IS DISTINCT FROM usaspending.legal_entity_state_code THEN jsonb_build_object('broker', broker.legal_entity_state_code, 'usaspending', usaspending.legal_entity_state_code) ELSE null END,
            'legal_entity_state_name', CASE WHEN broker.legal_entity_state_name IS DISTINCT FROM usaspending.legal_entity_state_name THEN jsonb_build_object('broker', broker.legal_entity_state_name, 'usaspending', usaspending.legal_entity_state_name) ELSE null END,
            'modified_at', CASE WHEN broker.modified_at IS DISTINCT FROM usaspending.modified_at THEN jsonb_build_object('broker', broker.modified_at, 'usaspending', usaspending.modified_at) ELSE null END,
            'afa_generated_unique', CASE WHEN UPPER(broker.afa_generated_unique) IS DISTINCT FROM usaspending.afa_generated_unique THEN jsonb_build_object('broker', broker.afa_generated_unique, 'usaspending', usaspending.afa_generated_unique) ELSE null END,
            'is_active', CASE WHEN broker.is_active IS DISTINCT FROM usaspending.is_active THEN jsonb_build_object('broker', broker.is_active, 'usaspending', usaspending.is_active) ELSE null END,
            'awarding_office_name', CASE WHEN broker.awarding_office_name IS DISTINCT FROM usaspending.awarding_office_name THEN jsonb_build_object('broker', broker.awarding_office_name, 'usaspending', usaspending.awarding_office_name) ELSE null END,
            'funding_office_name', CASE WHEN broker.funding_office_name IS DISTINCT FROM usaspending.funding_office_name THEN jsonb_build_object('broker', broker.funding_office_name, 'usaspending', usaspending.funding_office_name) ELSE null END,
            'legal_entity_city_code', CASE WHEN broker.legal_entity_city_code IS DISTINCT FROM usaspending.legal_entity_city_code THEN jsonb_build_object('broker', broker.legal_entity_city_code, 'usaspending', usaspending.legal_entity_city_code) ELSE null END,
            'legal_entity_foreign_descr', CASE WHEN broker.legal_entity_foreign_descr IS DISTINCT FROM usaspending.legal_entity_foreign_descr THEN jsonb_build_object('broker', broker.legal_entity_foreign_descr, 'usaspending', usaspending.legal_entity_foreign_descr) ELSE null END,
            'legal_entity_country_name', CASE WHEN broker.legal_entity_country_name IS DISTINCT FROM usaspending.legal_entity_country_name THEN jsonb_build_object('broker', broker.legal_entity_country_name, 'usaspending', usaspending.legal_entity_country_name) ELSE null END,
            'place_of_perform_country_n', CASE WHEN broker.place_of_perform_country_n IS DISTINCT FROM usaspending.place_of_perform_country_n THEN jsonb_build_object('broker', broker.place_of_perform_country_n, 'usaspending', usaspending.place_of_perform_country_n) ELSE null END,
            'place_of_perform_county_co', CASE WHEN broker.place_of_perform_county_co IS DISTINCT FROM usaspending.place_of_perform_county_co THEN jsonb_build_object('broker', broker.place_of_perform_county_co, 'usaspending', usaspending.place_of_perform_county_co) ELSE null END,
            'submission_id', CASE WHEN broker.submission_id IS DISTINCT FROM usaspending.submission_id THEN jsonb_build_object('broker', broker.submission_id, 'usaspending', usaspending.submission_id) ELSE null END,
            'place_of_perfor_state_code', CASE WHEN broker.place_of_perfor_state_code IS DISTINCT FROM usaspending.place_of_perfor_state_code THEN jsonb_build_object('broker', broker.place_of_perfor_state_code, 'usaspending', usaspending.place_of_perfor_state_code) ELSE null END,
            'place_of_performance_zip5', CASE WHEN broker.place_of_performance_zip5 IS DISTINCT FROM usaspending.place_of_performance_zip5 THEN jsonb_build_object('broker', broker.place_of_performance_zip5, 'usaspending', usaspending.place_of_performance_zip5) ELSE null END,
            'place_of_perform_zip_last4', CASE WHEN broker.place_of_perform_zip_last4 IS DISTINCT FROM usaspending.place_of_perform_zip_last4 THEN jsonb_build_object('broker', broker.place_of_perform_zip_last4, 'usaspending', usaspending.place_of_perform_zip_last4) ELSE null END,
            'action_type_description', CASE WHEN broker.action_type_description IS DISTINCT FROM usaspending.action_type_description THEN jsonb_build_object('broker', broker.action_type_description, 'usaspending', usaspending.action_type_description) ELSE null END,
            'assistance_type_desc', CASE WHEN broker.assistance_type_desc IS DISTINCT FROM usaspending.assistance_type_desc THEN jsonb_build_object('broker', broker.assistance_type_desc, 'usaspending', usaspending.assistance_type_desc) ELSE null END,
            'business_funds_ind_desc', CASE WHEN broker.business_funds_ind_desc IS DISTINCT FROM usaspending.business_funds_ind_desc THEN jsonb_build_object('broker', broker.business_funds_ind_desc, 'usaspending', usaspending.business_funds_ind_desc) ELSE null END,
            'business_types_desc', CASE WHEN broker.business_types_desc IS DISTINCT FROM usaspending.business_types_desc THEN jsonb_build_object('broker', broker.business_types_desc, 'usaspending', usaspending.business_types_desc) ELSE null END,
            'correction_delete_ind_desc', CASE WHEN broker.correction_delete_ind_desc IS DISTINCT FROM usaspending.correction_delete_ind_desc THEN jsonb_build_object('broker', broker.correction_delete_ind_desc, 'usaspending', usaspending.correction_delete_ind_desc) ELSE null END,
            'record_type_description', CASE WHEN broker.record_type_description IS DISTINCT FROM usaspending.record_type_description THEN jsonb_build_object('broker', broker.record_type_description, 'usaspending', usaspending.record_type_description) ELSE null END,
            'ultimate_parent_legal_enti', CASE WHEN broker.ultimate_parent_legal_enti IS DISTINCT FROM usaspending.ultimate_parent_legal_enti THEN jsonb_build_object('broker', broker.ultimate_parent_legal_enti, 'usaspending', usaspending.ultimate_parent_legal_enti) ELSE null END,
            'ultimate_parent_unique_ide', CASE WHEN broker.ultimate_parent_unique_ide IS DISTINCT FROM usaspending.ultimate_parent_unique_ide THEN jsonb_build_object('broker', broker.ultimate_parent_unique_ide, 'usaspending', usaspending.ultimate_parent_unique_ide) ELSE null END,
            'unique_award_key', CASE WHEN broker.unique_award_key IS DISTINCT FROM usaspending.unique_award_key THEN jsonb_build_object('broker', broker.unique_award_key, 'usaspending', usaspending.unique_award_key) ELSE null END,
            'high_comp_officer1_amount', CASE WHEN broker.high_comp_officer1_amount IS DISTINCT FROM usaspending.high_comp_officer1_amount THEN jsonb_build_object('broker', broker.high_comp_officer1_amount, 'usaspending', usaspending.high_comp_officer1_amount) ELSE null END,
            'high_comp_officer1_full_na', CASE WHEN broker.high_comp_officer1_full_na IS DISTINCT FROM usaspending.high_comp_officer1_full_na THEN jsonb_build_object('broker', broker.high_comp_officer1_full_na, 'usaspending', usaspending.high_comp_officer1_full_na) ELSE null END,
            'high_comp_officer2_amount', CASE WHEN broker.high_comp_officer2_amount IS DISTINCT FROM usaspending.high_comp_officer2_amount THEN jsonb_build_object('broker', broker.high_comp_officer2_amount, 'usaspending', usaspending.high_comp_officer2_amount) ELSE null END,
            'high_comp_officer2_full_na', CASE WHEN broker.high_comp_officer2_full_na IS DISTINCT FROM usaspending.high_comp_officer2_full_na THEN jsonb_build_object('broker', broker.high_comp_officer2_full_na, 'usaspending', usaspending.high_comp_officer2_full_na) ELSE null END,
            'high_comp_officer3_amount', CASE WHEN broker.high_comp_officer3_amount IS DISTINCT FROM usaspending.high_comp_officer3_amount THEN jsonb_build_object('broker', broker.high_comp_officer3_amount, 'usaspending', usaspending.high_comp_officer3_amount) ELSE null END,
            'high_comp_officer3_full_na', CASE WHEN broker.high_comp_officer3_full_na IS DISTINCT FROM usaspending.high_comp_officer3_full_na THEN jsonb_build_object('broker', broker.high_comp_officer3_full_na, 'usaspending', usaspending.high_comp_officer3_full_na) ELSE null END,
            'high_comp_officer4_amount', CASE WHEN broker.high_comp_officer4_amount IS DISTINCT FROM usaspending.high_comp_officer4_amount THEN jsonb_build_object('broker', broker.high_comp_officer4_amount, 'usaspending', usaspending.high_comp_officer4_amount) ELSE null END,
            'high_comp_officer4_full_na', CASE WHEN broker.high_comp_officer4_full_na IS DISTINCT FROM usaspending.high_comp_officer4_full_na THEN jsonb_build_object('broker', broker.high_comp_officer4_full_na, 'usaspending', usaspending.high_comp_officer4_full_na) ELSE null END,
            'high_comp_officer5_amount', CASE WHEN broker.high_comp_officer5_amount IS DISTINCT FROM usaspending.high_comp_officer5_amount THEN jsonb_build_object('broker', broker.high_comp_officer5_amount, 'usaspending', usaspending.high_comp_officer5_amount) ELSE null END,
            'high_comp_officer5_full_na', CASE WHEN broker.high_comp_officer5_full_na IS DISTINCT FROM usaspending.high_comp_officer5_full_na THEN jsonb_build_object('broker', broker.high_comp_officer5_full_na, 'usaspending', usaspending.high_comp_officer5_full_na) ELSE null END,
            'place_of_performance_scope', CASE WHEN broker.place_of_performance_scope IS DISTINCT FROM usaspending.place_of_performance_scope THEN jsonb_build_object('broker', broker.place_of_performance_scope, 'usaspending', usaspending.place_of_performance_scope) ELSE null END,
            'business_categories', CASE WHEN broker.business_categories IS DISTINCT FROM usaspending.business_categories THEN jsonb_build_object('broker', broker.business_categories, 'usaspending', usaspending.business_categories) ELSE null END
        )
    ) as fields_diff_json
FROM source_assistance_transaction AS usaspending
INNER JOIN
(
    SELECT * FROM dblink (
        'broker_server',
        'SELECT
            action_date,
            action_type,
            action_type_description,
            afa_generated_unique,
            assistance_type,
            assistance_type_desc,
            award_description,
            award_modification_amendme,
            awardee_or_recipient_legal,
            awardee_or_recipient_uniqu,
            awarding_agency_code,
            awarding_agency_name,
            awarding_office_code,
            awarding_office_name,
            awarding_sub_tier_agency_c,
            awarding_sub_tier_agency_n,
            business_categories,
            business_funds_ind_desc,
            business_funds_indicator,
            business_types,
            business_types_desc,
            cfda_number,
            cfda_title,
            correction_delete_ind_desc,
            correction_delete_indicatr,
            created_at,
            face_value_loan_guarantee,
            fain,
            federal_action_obligation,
            fiscal_year_and_quarter_co,
            funding_agency_code,
            funding_agency_name,
            funding_office_code,
            funding_office_name,
            funding_sub_tier_agency_co,
            funding_sub_tier_agency_na,
            high_comp_officer1_amount,
            high_comp_officer1_full_na,
            high_comp_officer2_amount,
            high_comp_officer2_full_na,
            high_comp_officer3_amount,
            high_comp_officer3_full_na,
            high_comp_officer4_amount,
            high_comp_officer4_full_na,
            high_comp_officer5_amount,
            high_comp_officer5_full_na,
            is_active,
            is_historical,
            legal_entity_address_line1,
            legal_entity_address_line2,
            legal_entity_address_line3,
            legal_entity_city_code,
            legal_entity_city_name,
            legal_entity_congressional,
            legal_entity_country_code,
            legal_entity_country_name,
            legal_entity_county_code,
            legal_entity_county_name,
            legal_entity_foreign_city,
            legal_entity_foreign_descr,
            legal_entity_foreign_posta,
            legal_entity_foreign_provi,
            legal_entity_state_code,
            legal_entity_state_name,
            legal_entity_zip5,
            legal_entity_zip_last4,
            modified_at,
            non_federal_funding_amount,
            original_loan_subsidy_cost,
            period_of_performance_curr,
            period_of_performance_star,
            place_of_perfor_state_code,
            place_of_perform_country_c,
            place_of_perform_country_n,
            place_of_perform_county_co,
            place_of_perform_county_na,
            place_of_perform_state_nam,
            place_of_perform_zip_last4,
            place_of_performance_city,
            place_of_performance_code,
            place_of_performance_congr,
            place_of_performance_forei,
            place_of_performance_scope,
            place_of_performance_zip4a,
            place_of_performance_zip5,
            published_award_financial_assistance_id,
            record_type,
            record_type_description,
            sai_number,
            submission_id,
            total_funding_amount,
            ultimate_parent_legal_enti,
            ultimate_parent_unique_ide,
            unique_award_key,
            updated_at,
            uri
        FROM published_award_financial_assistance
        WHERE {predicate}'
    ) AS broker(
        action_date text,
        action_type text,
        action_type_description text,
        afa_generated_unique text,
        assistance_type text,
        assistance_type_desc text,
        award_description text,
        award_modification_amendme text,
        awardee_or_recipient_legal text,
        awardee_or_recipient_uniqu text,
        awarding_agency_code text,
        awarding_agency_name text,
        awarding_office_code text,
        awarding_office_name text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        business_categories text[],
        business_funds_ind_desc text,
        business_funds_indicator text,
        business_types text,
        business_types_desc text,
        cfda_number text,
        cfda_title text,
        correction_delete_ind_desc text,
        correction_delete_indicatr text,
        created_at timestamp without time zone,
        face_value_loan_guarantee numeric,
        fain text,
        federal_action_obligation numeric,
        fiscal_year_and_quarter_co text,
        funding_agency_code text,
        funding_agency_name text,
        funding_office_code text,
        funding_office_name text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        high_comp_officer1_amount text,
        high_comp_officer1_full_na text,
        high_comp_officer2_amount text,
        high_comp_officer2_full_na text,
        high_comp_officer3_amount text,
        high_comp_officer3_full_na text,
        high_comp_officer4_amount text,
        high_comp_officer4_full_na text,
        high_comp_officer5_amount text,
        high_comp_officer5_full_na text,
        is_active boolean,
        is_historical boolean,
        legal_entity_address_line1 text,
        legal_entity_address_line2 text,
        legal_entity_address_line3 text,
        legal_entity_city_code text,
        legal_entity_city_name text,
        legal_entity_congressional text,
        legal_entity_country_code text,
        legal_entity_country_name text,
        legal_entity_county_code text,
        legal_entity_county_name text,
        legal_entity_foreign_city text,
        legal_entity_foreign_descr text,
        legal_entity_foreign_posta text,
        legal_entity_foreign_provi text,
        legal_entity_state_code text,
        legal_entity_state_name text,
        legal_entity_zip5 text,
        legal_entity_zip_last4 text,
        modified_at timestamp without time zone,
        non_federal_funding_amount numeric,
        original_loan_subsidy_cost numeric,
        period_of_performance_curr text,
        period_of_performance_star text,
        place_of_perfor_state_code text,
        place_of_perform_country_c text,
        place_of_perform_country_n text,
        place_of_perform_county_co text,
        place_of_perform_county_na text,
        place_of_perform_state_nam text,
        place_of_perform_zip_last4 text,
        place_of_performance_city text,
        place_of_performance_code text,
        place_of_performance_congr text,
        place_of_performance_forei text,
        place_of_performance_scope text,
        place_of_performance_zip4a text,
        place_of_performance_zip5 text,
        published_award_financial_assistance_id integer,
        record_type integer,
        record_type_description text,
        sai_number text,
        submission_id numeric,
        total_funding_amount text,
        ultimate_parent_legal_enti text,
        ultimate_parent_unique_ide text,
        unique_award_key text,
        updated_at timestamp without time zone,
        uri text
    )
) AS broker ON (
    (broker.published_award_financial_assistance_id = usaspending.published_award_financial_assistance_id)
    AND (
           (broker.action_date IS DISTINCT FROM usaspending.action_date)
        OR (broker.action_type IS DISTINCT FROM usaspending.action_type)
        OR (broker.action_type_description IS DISTINCT FROM usaspending.action_type_description)
        OR (broker.afa_generated_unique IS DISTINCT FROM usaspending.afa_generated_unique)
        OR (broker.assistance_type IS DISTINCT FROM usaspending.assistance_type)
        OR (broker.assistance_type_desc IS DISTINCT FROM usaspending.assistance_type_desc)
        OR (broker.award_description IS DISTINCT FROM usaspending.award_description)
        OR (broker.award_modification_amendme IS DISTINCT FROM usaspending.award_modification_amendme)
        OR (broker.awardee_or_recipient_legal IS DISTINCT FROM usaspending.awardee_or_recipient_legal)
        OR (broker.awardee_or_recipient_uniqu IS DISTINCT FROM usaspending.awardee_or_recipient_uniqu)
        OR (broker.awarding_agency_code IS DISTINCT FROM usaspending.awarding_agency_code)
        OR (broker.awarding_agency_name IS DISTINCT FROM usaspending.awarding_agency_name)
        OR (broker.awarding_office_code IS DISTINCT FROM usaspending.awarding_office_code)
        OR (broker.awarding_office_name IS DISTINCT FROM usaspending.awarding_office_name)
        OR (broker.awarding_sub_tier_agency_c IS DISTINCT FROM usaspending.awarding_sub_tier_agency_c)
        OR (broker.awarding_sub_tier_agency_n IS DISTINCT FROM usaspending.awarding_sub_tier_agency_n)
        OR (broker.business_categories IS DISTINCT FROM usaspending.business_categories)
        OR (broker.business_funds_ind_desc IS DISTINCT FROM usaspending.business_funds_ind_desc)
        OR (broker.business_funds_indicator IS DISTINCT FROM usaspending.business_funds_indicator)
        OR (broker.business_types IS DISTINCT FROM usaspending.business_types)
        OR (broker.business_types_desc IS DISTINCT FROM usaspending.business_types_desc)
        OR (broker.cfda_number IS DISTINCT FROM usaspending.cfda_number)
        OR (broker.cfda_title IS DISTINCT FROM usaspending.cfda_title)
        OR (broker.correction_delete_ind_desc IS DISTINCT FROM usaspending.correction_delete_ind_desc)
        OR (broker.correction_delete_indicatr IS DISTINCT FROM usaspending.correction_delete_indicatr)
        OR (broker.created_at IS DISTINCT FROM usaspending.created_at)
        OR (broker.face_value_loan_guarantee IS DISTINCT FROM usaspending.face_value_loan_guarantee)
        OR (broker.fain IS DISTINCT FROM usaspending.fain)
        OR (broker.federal_action_obligation IS DISTINCT FROM usaspending.federal_action_obligation)
        OR (broker.fiscal_year_and_quarter_co IS DISTINCT FROM usaspending.fiscal_year_and_quarter_co)
        OR (broker.funding_agency_code IS DISTINCT FROM usaspending.funding_agency_code)
        OR (broker.funding_agency_name IS DISTINCT FROM usaspending.funding_agency_name)
        OR (broker.funding_office_code IS DISTINCT FROM usaspending.funding_office_code)
        OR (broker.funding_office_name IS DISTINCT FROM usaspending.funding_office_name)
        OR (broker.funding_sub_tier_agency_co IS DISTINCT FROM usaspending.funding_sub_tier_agency_co)
        OR (broker.funding_sub_tier_agency_na IS DISTINCT FROM usaspending.funding_sub_tier_agency_na)
        OR (broker.high_comp_officer1_amount IS DISTINCT FROM usaspending.high_comp_officer1_amount)
        OR (broker.high_comp_officer1_full_na IS DISTINCT FROM usaspending.high_comp_officer1_full_na)
        OR (broker.high_comp_officer2_amount IS DISTINCT FROM usaspending.high_comp_officer2_amount)
        OR (broker.high_comp_officer2_full_na IS DISTINCT FROM usaspending.high_comp_officer2_full_na)
        OR (broker.high_comp_officer3_amount IS DISTINCT FROM usaspending.high_comp_officer3_amount)
        OR (broker.high_comp_officer3_full_na IS DISTINCT FROM usaspending.high_comp_officer3_full_na)
        OR (broker.high_comp_officer4_amount IS DISTINCT FROM usaspending.high_comp_officer4_amount)
        OR (broker.high_comp_officer4_full_na IS DISTINCT FROM usaspending.high_comp_officer4_full_na)
        OR (broker.high_comp_officer5_amount IS DISTINCT FROM usaspending.high_comp_officer5_amount)
        OR (broker.high_comp_officer5_full_na IS DISTINCT FROM usaspending.high_comp_officer5_full_na)
        OR (broker.is_active IS DISTINCT FROM usaspending.is_active)
        OR (broker.is_historical IS DISTINCT FROM usaspending.is_historical)
        OR (broker.legal_entity_address_line1 IS DISTINCT FROM usaspending.legal_entity_address_line1)
        OR (broker.legal_entity_address_line2 IS DISTINCT FROM usaspending.legal_entity_address_line2)
        OR (broker.legal_entity_address_line3 IS DISTINCT FROM usaspending.legal_entity_address_line3)
        OR (broker.legal_entity_city_code IS DISTINCT FROM usaspending.legal_entity_city_code)
        OR (broker.legal_entity_city_name IS DISTINCT FROM usaspending.legal_entity_city_name)
        OR (broker.legal_entity_congressional IS DISTINCT FROM usaspending.legal_entity_congressional)
        OR (broker.legal_entity_country_code IS DISTINCT FROM usaspending.legal_entity_country_code)
        OR (broker.legal_entity_country_name IS DISTINCT FROM usaspending.legal_entity_country_name)
        OR (broker.legal_entity_county_code IS DISTINCT FROM usaspending.legal_entity_county_code)
        OR (broker.legal_entity_county_name IS DISTINCT FROM usaspending.legal_entity_county_name)
        OR (broker.legal_entity_foreign_city IS DISTINCT FROM usaspending.legal_entity_foreign_city)
        OR (broker.legal_entity_foreign_descr IS DISTINCT FROM usaspending.legal_entity_foreign_descr)
        OR (broker.legal_entity_foreign_posta IS DISTINCT FROM usaspending.legal_entity_foreign_posta)
        OR (broker.legal_entity_foreign_provi IS DISTINCT FROM usaspending.legal_entity_foreign_provi)
        OR (broker.legal_entity_state_code IS DISTINCT FROM usaspending.legal_entity_state_code)
        OR (broker.legal_entity_state_name IS DISTINCT FROM usaspending.legal_entity_state_name)
        OR (broker.legal_entity_zip5 IS DISTINCT FROM usaspending.legal_entity_zip5)
        OR (broker.legal_entity_zip_last4 IS DISTINCT FROM usaspending.legal_entity_zip_last4)
        OR (broker.modified_at IS DISTINCT FROM usaspending.modified_at)
        OR (broker.non_federal_funding_amount IS DISTINCT FROM usaspending.non_federal_funding_amount)
        OR (broker.original_loan_subsidy_cost IS DISTINCT FROM usaspending.original_loan_subsidy_cost)
        OR (broker.period_of_performance_curr IS DISTINCT FROM usaspending.period_of_performance_curr)
        OR (broker.period_of_performance_star IS DISTINCT FROM usaspending.period_of_performance_star)
        OR (broker.place_of_perfor_state_code IS DISTINCT FROM usaspending.place_of_perfor_state_code)
        OR (broker.place_of_perform_country_c IS DISTINCT FROM usaspending.place_of_perform_country_c)
        OR (broker.place_of_perform_country_n IS DISTINCT FROM usaspending.place_of_perform_country_n)
        OR (broker.place_of_perform_county_co IS DISTINCT FROM usaspending.place_of_perform_county_co)
        OR (broker.place_of_perform_county_na IS DISTINCT FROM usaspending.place_of_perform_county_na)
        OR (broker.place_of_perform_state_nam IS DISTINCT FROM usaspending.place_of_perform_state_nam)
        OR (broker.place_of_perform_zip_last4 IS DISTINCT FROM usaspending.place_of_perform_zip_last4)
        OR (broker.place_of_performance_city IS DISTINCT FROM usaspending.place_of_performance_city)
        OR (broker.place_of_performance_code IS DISTINCT FROM usaspending.place_of_performance_code)
        OR (broker.place_of_performance_congr IS DISTINCT FROM usaspending.place_of_performance_congr)
        OR (broker.place_of_performance_forei IS DISTINCT FROM usaspending.place_of_performance_forei)
        OR (broker.place_of_performance_scope IS DISTINCT FROM usaspending.place_of_performance_scope)
        OR (broker.place_of_performance_zip4a IS DISTINCT FROM usaspending.place_of_performance_zip4a)
        OR (broker.place_of_performance_zip5 IS DISTINCT FROM usaspending.place_of_performance_zip5)
        OR (broker.record_type IS DISTINCT FROM usaspending.record_type)
        OR (broker.record_type_description IS DISTINCT FROM usaspending.record_type_description)
        OR (broker.sai_number IS DISTINCT FROM usaspending.sai_number)
        OR (broker.submission_id IS DISTINCT FROM usaspending.submission_id)
        OR (broker.total_funding_amount IS DISTINCT FROM usaspending.total_funding_amount)
        OR (broker.ultimate_parent_legal_enti IS DISTINCT FROM usaspending.ultimate_parent_legal_enti)
        OR (broker.ultimate_parent_unique_ide IS DISTINCT FROM usaspending.ultimate_parent_unique_ide)
        OR (broker.unique_award_key IS DISTINCT FROM usaspending.unique_award_key)
        OR (broker.updated_at IS DISTINCT FROM usaspending.updated_at)
        OR (broker.uri IS DISTINCT FROM usaspending.uri)
    )
)