SELECT
    'fpds'::TEXT as "system",
    usaspending.transaction_id,
    usaspending.detached_award_procurement_id AS "broker_surrogate_id",
    usaspending.detached_award_proc_unique AS "broker_derived_unique_key",
    usaspending.piid AS "piid_fain_uri",
    usaspending.unique_award_key,
    usaspending.action_date::date,
    usaspending.updated_at::date AS "record_last_modified",
    broker.created_at::timestamp with time zone AS "broker_record_create",
    broker.updated_at::timestamp with time zone AS "broker_record_update",
    transaction_normalized.create_date::timestamp with time zone AS "usaspending_record_create",
    transaction_normalized.update_date::timestamp with time zone AS "usaspending_record_update",
    jsonb_strip_nulls(
        jsonb_build_object(
            'piid', CASE WHEN broker.piid IS DISTINCT FROM usaspending.piid THEN jsonb_build_object('broker', broker.piid, 'usaspending', usaspending.piid) ELSE null END,
            'detached_award_proc_unique', CASE WHEN broker.detached_award_proc_unique IS DISTINCT FROM usaspending.detached_award_proc_unique THEN jsonb_build_object('broker', broker.detached_award_proc_unique, 'usaspending', usaspending.detached_award_proc_unique) ELSE null END,
            'agency_id', CASE WHEN broker.agency_id IS DISTINCT FROM usaspending.agency_id THEN jsonb_build_object('broker', broker.agency_id, 'usaspending', usaspending.agency_id) ELSE null END,
            'awarding_sub_tier_agency_c', CASE WHEN broker.awarding_sub_tier_agency_c IS DISTINCT FROM usaspending.awarding_sub_tier_agency_c THEN jsonb_build_object('broker', broker.awarding_sub_tier_agency_c, 'usaspending', usaspending.awarding_sub_tier_agency_c) ELSE null END,
            'awarding_sub_tier_agency_n', CASE WHEN broker.awarding_sub_tier_agency_n IS DISTINCT FROM usaspending.awarding_sub_tier_agency_n THEN jsonb_build_object('broker', broker.awarding_sub_tier_agency_n, 'usaspending', usaspending.awarding_sub_tier_agency_n) ELSE null END,
            'awarding_agency_code', CASE WHEN broker.awarding_agency_code IS DISTINCT FROM usaspending.awarding_agency_code THEN jsonb_build_object('broker', broker.awarding_agency_code, 'usaspending', usaspending.awarding_agency_code) ELSE null END,
            'awarding_agency_name', CASE WHEN broker.awarding_agency_name IS DISTINCT FROM usaspending.awarding_agency_name THEN jsonb_build_object('broker', broker.awarding_agency_name, 'usaspending', usaspending.awarding_agency_name) ELSE null END,
            'parent_award_id', CASE WHEN broker.parent_award_id IS DISTINCT FROM usaspending.parent_award_id THEN jsonb_build_object('broker', broker.parent_award_id, 'usaspending', usaspending.parent_award_id) ELSE null END,
            'award_modification_amendme', CASE WHEN broker.award_modification_amendme IS DISTINCT FROM usaspending.award_modification_amendme THEN jsonb_build_object('broker', broker.award_modification_amendme, 'usaspending', usaspending.award_modification_amendme) ELSE null END,
            'type_of_contract_pricing', CASE WHEN broker.type_of_contract_pricing IS DISTINCT FROM usaspending.type_of_contract_pricing THEN jsonb_build_object('broker', broker.type_of_contract_pricing, 'usaspending', usaspending.type_of_contract_pricing) ELSE null END,
            'type_of_contract_pric_desc', CASE WHEN broker.type_of_contract_pric_desc IS DISTINCT FROM usaspending.type_of_contract_pric_desc THEN jsonb_build_object('broker', broker.type_of_contract_pric_desc, 'usaspending', usaspending.type_of_contract_pric_desc) ELSE null END,
            'contract_award_type', CASE WHEN broker.contract_award_type IS DISTINCT FROM usaspending.contract_award_type THEN jsonb_build_object('broker', broker.contract_award_type, 'usaspending', usaspending.contract_award_type) ELSE null END,
            'contract_award_type_desc', CASE WHEN broker.contract_award_type_desc IS DISTINCT FROM usaspending.contract_award_type_desc THEN jsonb_build_object('broker', broker.contract_award_type_desc, 'usaspending', usaspending.contract_award_type_desc) ELSE null END,
            'naics', CASE WHEN broker.naics IS DISTINCT FROM usaspending.naics THEN jsonb_build_object('broker', broker.naics, 'usaspending', usaspending.naics) ELSE null END,
            'naics_description', CASE WHEN broker.naics_description IS DISTINCT FROM usaspending.naics_description THEN jsonb_build_object('broker', broker.naics_description, 'usaspending', usaspending.naics_description) ELSE null END,
            'awardee_or_recipient_uniqu', CASE WHEN broker.awardee_or_recipient_uniqu IS DISTINCT FROM usaspending.awardee_or_recipient_uniqu THEN jsonb_build_object('broker', broker.awardee_or_recipient_uniqu, 'usaspending', usaspending.awardee_or_recipient_uniqu) ELSE null END,
            'ultimate_parent_legal_enti', CASE WHEN broker.ultimate_parent_legal_enti IS DISTINCT FROM usaspending.ultimate_parent_legal_enti THEN jsonb_build_object('broker', broker.ultimate_parent_legal_enti, 'usaspending', usaspending.ultimate_parent_legal_enti) ELSE null END,
            'ultimate_parent_unique_ide', CASE WHEN broker.ultimate_parent_unique_ide IS DISTINCT FROM usaspending.ultimate_parent_unique_ide THEN jsonb_build_object('broker', broker.ultimate_parent_unique_ide, 'usaspending', usaspending.ultimate_parent_unique_ide) ELSE null END,
            'award_description', CASE WHEN broker.award_description IS DISTINCT FROM usaspending.award_description THEN jsonb_build_object('broker', broker.award_description, 'usaspending', usaspending.award_description) ELSE null END,
            'place_of_performance_zip4a', CASE WHEN broker.place_of_performance_zip4a IS DISTINCT FROM usaspending.place_of_performance_zip4a THEN jsonb_build_object('broker', broker.place_of_performance_zip4a, 'usaspending', usaspending.place_of_performance_zip4a) ELSE null END,
            'place_of_perform_city_name', CASE WHEN broker.place_of_perform_city_name IS DISTINCT FROM usaspending.place_of_perform_city_name THEN jsonb_build_object('broker', broker.place_of_perform_city_name, 'usaspending', usaspending.place_of_perform_city_name) ELSE null END,
            'place_of_perform_county_na', CASE WHEN broker.place_of_perform_county_na IS DISTINCT FROM usaspending.place_of_perform_county_na THEN jsonb_build_object('broker', broker.place_of_perform_county_na, 'usaspending', usaspending.place_of_perform_county_na) ELSE null END,
            'place_of_performance_congr', CASE WHEN broker.place_of_performance_congr IS DISTINCT FROM usaspending.place_of_performance_congr THEN jsonb_build_object('broker', broker.place_of_performance_congr, 'usaspending', usaspending.place_of_performance_congr) ELSE null END,
            'awardee_or_recipient_legal', CASE WHEN broker.awardee_or_recipient_legal IS DISTINCT FROM usaspending.awardee_or_recipient_legal THEN jsonb_build_object('broker', broker.awardee_or_recipient_legal, 'usaspending', usaspending.awardee_or_recipient_legal) ELSE null END,
            'legal_entity_city_name', CASE WHEN broker.legal_entity_city_name IS DISTINCT FROM usaspending.legal_entity_city_name THEN jsonb_build_object('broker', broker.legal_entity_city_name, 'usaspending', usaspending.legal_entity_city_name) ELSE null END,
            'legal_entity_state_code', CASE WHEN broker.legal_entity_state_code IS DISTINCT FROM usaspending.legal_entity_state_code THEN jsonb_build_object('broker', broker.legal_entity_state_code, 'usaspending', usaspending.legal_entity_state_code) ELSE null END,
            'legal_entity_state_descrip', CASE WHEN broker.legal_entity_state_descrip IS DISTINCT FROM usaspending.legal_entity_state_descrip THEN jsonb_build_object('broker', broker.legal_entity_state_descrip, 'usaspending', usaspending.legal_entity_state_descrip) ELSE null END,
            'legal_entity_zip4', CASE WHEN broker.legal_entity_zip4 IS DISTINCT FROM usaspending.legal_entity_zip4 THEN jsonb_build_object('broker', broker.legal_entity_zip4, 'usaspending', usaspending.legal_entity_zip4) ELSE null END,
            'legal_entity_congressional', CASE WHEN broker.legal_entity_congressional IS DISTINCT FROM usaspending.legal_entity_congressional THEN jsonb_build_object('broker', broker.legal_entity_congressional, 'usaspending', usaspending.legal_entity_congressional) ELSE null END,
            'legal_entity_address_line1', CASE WHEN broker.legal_entity_address_line1 IS DISTINCT FROM usaspending.legal_entity_address_line1 THEN jsonb_build_object('broker', broker.legal_entity_address_line1, 'usaspending', usaspending.legal_entity_address_line1) ELSE null END,
            'legal_entity_address_line2', CASE WHEN broker.legal_entity_address_line2 IS DISTINCT FROM usaspending.legal_entity_address_line2 THEN jsonb_build_object('broker', broker.legal_entity_address_line2, 'usaspending', usaspending.legal_entity_address_line2) ELSE null END,
            'legal_entity_address_line3', CASE WHEN broker.legal_entity_address_line3 IS DISTINCT FROM usaspending.legal_entity_address_line3 THEN jsonb_build_object('broker', broker.legal_entity_address_line3, 'usaspending', usaspending.legal_entity_address_line3) ELSE null END,
            'legal_entity_country_code', CASE WHEN broker.legal_entity_country_code IS DISTINCT FROM usaspending.legal_entity_country_code THEN jsonb_build_object('broker', broker.legal_entity_country_code, 'usaspending', usaspending.legal_entity_country_code) ELSE null END,
            'legal_entity_country_name', CASE WHEN broker.legal_entity_country_name IS DISTINCT FROM usaspending.legal_entity_country_name THEN jsonb_build_object('broker', broker.legal_entity_country_name, 'usaspending', usaspending.legal_entity_country_name) ELSE null END,
            'period_of_performance_star', CASE WHEN broker.period_of_performance_star IS DISTINCT FROM usaspending.period_of_performance_star THEN jsonb_build_object('broker', broker.period_of_performance_star, 'usaspending', usaspending.period_of_performance_star) ELSE null END,
            'period_of_performance_curr', CASE WHEN broker.period_of_performance_curr IS DISTINCT FROM usaspending.period_of_performance_curr THEN jsonb_build_object('broker', broker.period_of_performance_curr, 'usaspending', usaspending.period_of_performance_curr) ELSE null END,
            'period_of_perf_potential_e', CASE WHEN broker.period_of_perf_potential_e IS DISTINCT FROM usaspending.period_of_perf_potential_e THEN jsonb_build_object('broker', broker.period_of_perf_potential_e, 'usaspending', usaspending.period_of_perf_potential_e) ELSE null END,
            'ordering_period_end_date', CASE WHEN broker.ordering_period_end_date IS DISTINCT FROM usaspending.ordering_period_end_date THEN jsonb_build_object('broker', broker.ordering_period_end_date, 'usaspending', usaspending.ordering_period_end_date) ELSE null END,
            'action_date', CASE WHEN broker.action_date IS DISTINCT FROM usaspending.action_date THEN jsonb_build_object('broker', broker.action_date, 'usaspending', usaspending.action_date) ELSE null END,
            'action_type', CASE WHEN broker.action_type IS DISTINCT FROM usaspending.action_type THEN jsonb_build_object('broker', broker.action_type, 'usaspending', usaspending.action_type) ELSE null END,
            'action_type_description', CASE WHEN broker.action_type_description IS DISTINCT FROM usaspending.action_type_description THEN jsonb_build_object('broker', broker.action_type_description, 'usaspending', usaspending.action_type_description) ELSE null END,
            'federal_action_obligation', CASE WHEN broker.federal_action_obligation IS DISTINCT FROM usaspending.federal_action_obligation THEN jsonb_build_object('broker', broker.federal_action_obligation, 'usaspending', usaspending.federal_action_obligation) ELSE null END,
            'current_total_value_award', CASE WHEN broker.current_total_value_award IS DISTINCT FROM usaspending.current_total_value_award THEN jsonb_build_object('broker', broker.current_total_value_award, 'usaspending', usaspending.current_total_value_award) ELSE null END,
            'potential_total_value_awar', CASE WHEN broker.potential_total_value_awar IS DISTINCT FROM usaspending.potential_total_value_awar THEN jsonb_build_object('broker', broker.potential_total_value_awar, 'usaspending', usaspending.potential_total_value_awar) ELSE null END,
            'funding_sub_tier_agency_co', CASE WHEN broker.funding_sub_tier_agency_co IS DISTINCT FROM usaspending.funding_sub_tier_agency_co THEN jsonb_build_object('broker', broker.funding_sub_tier_agency_co, 'usaspending', usaspending.funding_sub_tier_agency_co) ELSE null END
        ) || jsonb_build_object(
            'funding_sub_tier_agency_na', CASE WHEN broker.funding_sub_tier_agency_na IS DISTINCT FROM usaspending.funding_sub_tier_agency_na THEN jsonb_build_object('broker', broker.funding_sub_tier_agency_na, 'usaspending', usaspending.funding_sub_tier_agency_na) ELSE null END,
            'funding_office_code', CASE WHEN broker.funding_office_code IS DISTINCT FROM usaspending.funding_office_code THEN jsonb_build_object('broker', broker.funding_office_code, 'usaspending', usaspending.funding_office_code) ELSE null END,
            'funding_office_name', CASE WHEN broker.funding_office_name IS DISTINCT FROM usaspending.funding_office_name THEN jsonb_build_object('broker', broker.funding_office_name, 'usaspending', usaspending.funding_office_name) ELSE null END,
            'awarding_office_code', CASE WHEN broker.awarding_office_code IS DISTINCT FROM usaspending.awarding_office_code THEN jsonb_build_object('broker', broker.awarding_office_code, 'usaspending', usaspending.awarding_office_code) ELSE null END,
            'awarding_office_name', CASE WHEN broker.awarding_office_name IS DISTINCT FROM usaspending.awarding_office_name THEN jsonb_build_object('broker', broker.awarding_office_name, 'usaspending', usaspending.awarding_office_name) ELSE null END,
            'referenced_idv_agency_iden', CASE WHEN broker.referenced_idv_agency_iden IS DISTINCT FROM usaspending.referenced_idv_agency_iden THEN jsonb_build_object('broker', broker.referenced_idv_agency_iden, 'usaspending', usaspending.referenced_idv_agency_iden) ELSE null END,
            'referenced_idv_agency_desc', CASE WHEN broker.referenced_idv_agency_desc IS DISTINCT FROM usaspending.referenced_idv_agency_desc THEN jsonb_build_object('broker', broker.referenced_idv_agency_desc, 'usaspending', usaspending.referenced_idv_agency_desc) ELSE null END,
            'funding_agency_code', CASE WHEN broker.funding_agency_code IS DISTINCT FROM usaspending.funding_agency_code THEN jsonb_build_object('broker', broker.funding_agency_code, 'usaspending', usaspending.funding_agency_code) ELSE null END,
            'funding_agency_name', CASE WHEN broker.funding_agency_name IS DISTINCT FROM usaspending.funding_agency_name THEN jsonb_build_object('broker', broker.funding_agency_name, 'usaspending', usaspending.funding_agency_name) ELSE null END,
            'place_of_performance_locat', CASE WHEN broker.place_of_performance_locat IS DISTINCT FROM usaspending.place_of_performance_locat THEN jsonb_build_object('broker', broker.place_of_performance_locat, 'usaspending', usaspending.place_of_performance_locat) ELSE null END,
            'place_of_performance_state', CASE WHEN broker.place_of_performance_state IS DISTINCT FROM usaspending.place_of_performance_state THEN jsonb_build_object('broker', broker.place_of_performance_state, 'usaspending', usaspending.place_of_performance_state) ELSE null END,
            'place_of_perfor_state_desc', CASE WHEN broker.place_of_perfor_state_desc IS DISTINCT FROM usaspending.place_of_perfor_state_desc THEN jsonb_build_object('broker', broker.place_of_perfor_state_desc, 'usaspending', usaspending.place_of_perfor_state_desc) ELSE null END,
            'place_of_perform_country_c', CASE WHEN broker.place_of_perform_country_c IS DISTINCT FROM usaspending.place_of_perform_country_c THEN jsonb_build_object('broker', broker.place_of_perform_country_c, 'usaspending', usaspending.place_of_perform_country_c) ELSE null END,
            'place_of_perf_country_desc', CASE WHEN broker.place_of_perf_country_desc IS DISTINCT FROM usaspending.place_of_perf_country_desc THEN jsonb_build_object('broker', broker.place_of_perf_country_desc, 'usaspending', usaspending.place_of_perf_country_desc) ELSE null END,
            'idv_type', CASE WHEN broker.idv_type IS DISTINCT FROM usaspending.idv_type THEN jsonb_build_object('broker', broker.idv_type, 'usaspending', usaspending.idv_type) ELSE null END,
            'idv_type_description', CASE WHEN broker.idv_type_description IS DISTINCT FROM usaspending.idv_type_description THEN jsonb_build_object('broker', broker.idv_type_description, 'usaspending', usaspending.idv_type_description) ELSE null END,
            'referenced_idv_type', CASE WHEN broker.referenced_idv_type IS DISTINCT FROM usaspending.referenced_idv_type THEN jsonb_build_object('broker', broker.referenced_idv_type, 'usaspending', usaspending.referenced_idv_type) ELSE null END,
            'referenced_idv_type_desc', CASE WHEN broker.referenced_idv_type_desc IS DISTINCT FROM usaspending.referenced_idv_type_desc THEN jsonb_build_object('broker', broker.referenced_idv_type_desc, 'usaspending', usaspending.referenced_idv_type_desc) ELSE null END,
            'vendor_doing_as_business_n', CASE WHEN broker.vendor_doing_as_business_n IS DISTINCT FROM usaspending.vendor_doing_as_business_n THEN jsonb_build_object('broker', broker.vendor_doing_as_business_n, 'usaspending', usaspending.vendor_doing_as_business_n) ELSE null END,
            'vendor_phone_number', CASE WHEN broker.vendor_phone_number IS DISTINCT FROM usaspending.vendor_phone_number THEN jsonb_build_object('broker', broker.vendor_phone_number, 'usaspending', usaspending.vendor_phone_number) ELSE null END,
            'vendor_fax_number', CASE WHEN broker.vendor_fax_number IS DISTINCT FROM usaspending.vendor_fax_number THEN jsonb_build_object('broker', broker.vendor_fax_number, 'usaspending', usaspending.vendor_fax_number) ELSE null END,
            'multiple_or_single_award_i', CASE WHEN broker.multiple_or_single_award_i IS DISTINCT FROM usaspending.multiple_or_single_award_i THEN jsonb_build_object('broker', broker.multiple_or_single_award_i, 'usaspending', usaspending.multiple_or_single_award_i) ELSE null END,
            'multiple_or_single_aw_desc', CASE WHEN broker.multiple_or_single_aw_desc IS DISTINCT FROM usaspending.multiple_or_single_aw_desc THEN jsonb_build_object('broker', broker.multiple_or_single_aw_desc, 'usaspending', usaspending.multiple_or_single_aw_desc) ELSE null END,
            'referenced_mult_or_single', CASE WHEN broker.referenced_mult_or_single IS DISTINCT FROM usaspending.referenced_mult_or_single THEN jsonb_build_object('broker', broker.referenced_mult_or_single, 'usaspending', usaspending.referenced_mult_or_single) ELSE null END,
            'referenced_mult_or_si_desc', CASE WHEN broker.referenced_mult_or_si_desc IS DISTINCT FROM usaspending.referenced_mult_or_si_desc THEN jsonb_build_object('broker', broker.referenced_mult_or_si_desc, 'usaspending', usaspending.referenced_mult_or_si_desc) ELSE null END,
            'type_of_idc', CASE WHEN broker.type_of_idc IS DISTINCT FROM usaspending.type_of_idc THEN jsonb_build_object('broker', broker.type_of_idc, 'usaspending', usaspending.type_of_idc) ELSE null END,
            'type_of_idc_description', CASE WHEN broker.type_of_idc_description IS DISTINCT FROM usaspending.type_of_idc_description THEN jsonb_build_object('broker', broker.type_of_idc_description, 'usaspending', usaspending.type_of_idc_description) ELSE null END,
            'a_76_fair_act_action', CASE WHEN broker.a_76_fair_act_action IS DISTINCT FROM usaspending.a_76_fair_act_action THEN jsonb_build_object('broker', broker.a_76_fair_act_action, 'usaspending', usaspending.a_76_fair_act_action) ELSE null END,
            'a_76_fair_act_action_desc', CASE WHEN broker.a_76_fair_act_action_desc IS DISTINCT FROM usaspending.a_76_fair_act_action_desc THEN jsonb_build_object('broker', broker.a_76_fair_act_action_desc, 'usaspending', usaspending.a_76_fair_act_action_desc) ELSE null END,
            'dod_claimant_program_code', CASE WHEN broker.dod_claimant_program_code IS DISTINCT FROM usaspending.dod_claimant_program_code THEN jsonb_build_object('broker', broker.dod_claimant_program_code, 'usaspending', usaspending.dod_claimant_program_code) ELSE null END,
            'dod_claimant_prog_cod_desc', CASE WHEN broker.dod_claimant_prog_cod_desc IS DISTINCT FROM usaspending.dod_claimant_prog_cod_desc THEN jsonb_build_object('broker', broker.dod_claimant_prog_cod_desc, 'usaspending', usaspending.dod_claimant_prog_cod_desc) ELSE null END,
            'clinger_cohen_act_planning', CASE WHEN broker.clinger_cohen_act_planning IS DISTINCT FROM usaspending.clinger_cohen_act_planning THEN jsonb_build_object('broker', broker.clinger_cohen_act_planning, 'usaspending', usaspending.clinger_cohen_act_planning) ELSE null END,
            'clinger_cohen_act_pla_desc', CASE WHEN broker.clinger_cohen_act_pla_desc IS DISTINCT FROM usaspending.clinger_cohen_act_pla_desc THEN jsonb_build_object('broker', broker.clinger_cohen_act_pla_desc, 'usaspending', usaspending.clinger_cohen_act_pla_desc) ELSE null END,
            'commercial_item_acquisitio', CASE WHEN broker.commercial_item_acquisitio IS DISTINCT FROM usaspending.commercial_item_acquisitio THEN jsonb_build_object('broker', broker.commercial_item_acquisitio, 'usaspending', usaspending.commercial_item_acquisitio) ELSE null END,
            'commercial_item_acqui_desc', CASE WHEN broker.commercial_item_acqui_desc IS DISTINCT FROM usaspending.commercial_item_acqui_desc THEN jsonb_build_object('broker', broker.commercial_item_acqui_desc, 'usaspending', usaspending.commercial_item_acqui_desc) ELSE null END,
            'commercial_item_test_progr', CASE WHEN broker.commercial_item_test_progr IS DISTINCT FROM usaspending.commercial_item_test_progr THEN jsonb_build_object('broker', broker.commercial_item_test_progr, 'usaspending', usaspending.commercial_item_test_progr) ELSE null END,
            'commercial_item_test_desc', CASE WHEN broker.commercial_item_test_desc IS DISTINCT FROM usaspending.commercial_item_test_desc THEN jsonb_build_object('broker', broker.commercial_item_test_desc, 'usaspending', usaspending.commercial_item_test_desc) ELSE null END,
            'consolidated_contract', CASE WHEN broker.consolidated_contract IS DISTINCT FROM usaspending.consolidated_contract THEN jsonb_build_object('broker', broker.consolidated_contract, 'usaspending', usaspending.consolidated_contract) ELSE null END,
            'consolidated_contract_desc', CASE WHEN broker.consolidated_contract_desc IS DISTINCT FROM usaspending.consolidated_contract_desc THEN jsonb_build_object('broker', broker.consolidated_contract_desc, 'usaspending', usaspending.consolidated_contract_desc) ELSE null END,
            'contingency_humanitarian_o', CASE WHEN broker.contingency_humanitarian_o IS DISTINCT FROM usaspending.contingency_humanitarian_o THEN jsonb_build_object('broker', broker.contingency_humanitarian_o, 'usaspending', usaspending.contingency_humanitarian_o) ELSE null END,
            'contingency_humanitar_desc', CASE WHEN broker.contingency_humanitar_desc IS DISTINCT FROM usaspending.contingency_humanitar_desc THEN jsonb_build_object('broker', broker.contingency_humanitar_desc, 'usaspending', usaspending.contingency_humanitar_desc) ELSE null END,
            'contract_bundling', CASE WHEN broker.contract_bundling IS DISTINCT FROM usaspending.contract_bundling THEN jsonb_build_object('broker', broker.contract_bundling, 'usaspending', usaspending.contract_bundling) ELSE null END,
            'contract_bundling_descrip', CASE WHEN broker.contract_bundling_descrip IS DISTINCT FROM usaspending.contract_bundling_descrip THEN jsonb_build_object('broker', broker.contract_bundling_descrip, 'usaspending', usaspending.contract_bundling_descrip) ELSE null END,
            'contract_financing', CASE WHEN broker.contract_financing IS DISTINCT FROM usaspending.contract_financing THEN jsonb_build_object('broker', broker.contract_financing, 'usaspending', usaspending.contract_financing) ELSE null END,
            'contract_financing_descrip', CASE WHEN broker.contract_financing_descrip IS DISTINCT FROM usaspending.contract_financing_descrip THEN jsonb_build_object('broker', broker.contract_financing_descrip, 'usaspending', usaspending.contract_financing_descrip) ELSE null END
        ) || jsonb_build_object(
            'contracting_officers_deter', CASE WHEN broker.contracting_officers_deter IS DISTINCT FROM usaspending.contracting_officers_deter THEN jsonb_build_object('broker', broker.contracting_officers_deter, 'usaspending', usaspending.contracting_officers_deter) ELSE null END,
            'contracting_officers_desc', CASE WHEN broker.contracting_officers_desc IS DISTINCT FROM usaspending.contracting_officers_desc THEN jsonb_build_object('broker', broker.contracting_officers_desc, 'usaspending', usaspending.contracting_officers_desc) ELSE null END,
            'cost_accounting_standards', CASE WHEN broker.cost_accounting_standards IS DISTINCT FROM usaspending.cost_accounting_standards THEN jsonb_build_object('broker', broker.cost_accounting_standards, 'usaspending', usaspending.cost_accounting_standards) ELSE null END,
            'cost_accounting_stand_desc', CASE WHEN broker.cost_accounting_stand_desc IS DISTINCT FROM usaspending.cost_accounting_stand_desc THEN jsonb_build_object('broker', broker.cost_accounting_stand_desc, 'usaspending', usaspending.cost_accounting_stand_desc) ELSE null END,
            'cost_or_pricing_data', CASE WHEN broker.cost_or_pricing_data IS DISTINCT FROM usaspending.cost_or_pricing_data THEN jsonb_build_object('broker', broker.cost_or_pricing_data, 'usaspending', usaspending.cost_or_pricing_data) ELSE null END,
            'cost_or_pricing_data_desc', CASE WHEN broker.cost_or_pricing_data_desc IS DISTINCT FROM usaspending.cost_or_pricing_data_desc THEN jsonb_build_object('broker', broker.cost_or_pricing_data_desc, 'usaspending', usaspending.cost_or_pricing_data_desc) ELSE null END,
            'country_of_product_or_serv', CASE WHEN broker.country_of_product_or_serv IS DISTINCT FROM usaspending.country_of_product_or_serv THEN jsonb_build_object('broker', broker.country_of_product_or_serv, 'usaspending', usaspending.country_of_product_or_serv) ELSE null END,
            'country_of_product_or_desc', CASE WHEN broker.country_of_product_or_desc IS DISTINCT FROM usaspending.country_of_product_or_desc THEN jsonb_build_object('broker', broker.country_of_product_or_desc, 'usaspending', usaspending.country_of_product_or_desc) ELSE null END,
            'construction_wage_rate_req', CASE WHEN broker.construction_wage_rate_req IS DISTINCT FROM usaspending.construction_wage_rate_req THEN jsonb_build_object('broker', broker.construction_wage_rate_req, 'usaspending', usaspending.construction_wage_rate_req) ELSE null END,
            'construction_wage_rat_desc', CASE WHEN broker.construction_wage_rat_desc IS DISTINCT FROM usaspending.construction_wage_rat_desc THEN jsonb_build_object('broker', broker.construction_wage_rat_desc, 'usaspending', usaspending.construction_wage_rat_desc) ELSE null END,
            'evaluated_preference', CASE WHEN broker.evaluated_preference IS DISTINCT FROM usaspending.evaluated_preference THEN jsonb_build_object('broker', broker.evaluated_preference, 'usaspending', usaspending.evaluated_preference) ELSE null END,
            'evaluated_preference_desc', CASE WHEN broker.evaluated_preference_desc IS DISTINCT FROM usaspending.evaluated_preference_desc THEN jsonb_build_object('broker', broker.evaluated_preference_desc, 'usaspending', usaspending.evaluated_preference_desc) ELSE null END,
            'extent_competed', CASE WHEN broker.extent_competed IS DISTINCT FROM usaspending.extent_competed THEN jsonb_build_object('broker', broker.extent_competed, 'usaspending', usaspending.extent_competed) ELSE null END,
            'extent_compete_description', CASE WHEN broker.extent_compete_description IS DISTINCT FROM usaspending.extent_compete_description THEN jsonb_build_object('broker', broker.extent_compete_description, 'usaspending', usaspending.extent_compete_description) ELSE null END,
            'fed_biz_opps', CASE WHEN broker.fed_biz_opps IS DISTINCT FROM usaspending.fed_biz_opps THEN jsonb_build_object('broker', broker.fed_biz_opps, 'usaspending', usaspending.fed_biz_opps) ELSE null END,
            'fed_biz_opps_description', CASE WHEN broker.fed_biz_opps_description IS DISTINCT FROM usaspending.fed_biz_opps_description THEN jsonb_build_object('broker', broker.fed_biz_opps_description, 'usaspending', usaspending.fed_biz_opps_description) ELSE null END,
            'foreign_funding', CASE WHEN broker.foreign_funding IS DISTINCT FROM usaspending.foreign_funding THEN jsonb_build_object('broker', broker.foreign_funding, 'usaspending', usaspending.foreign_funding) ELSE null END,
            'foreign_funding_desc', CASE WHEN broker.foreign_funding_desc IS DISTINCT FROM usaspending.foreign_funding_desc THEN jsonb_build_object('broker', broker.foreign_funding_desc, 'usaspending', usaspending.foreign_funding_desc) ELSE null END,
            'government_furnished_prope', CASE WHEN broker.government_furnished_prope IS DISTINCT FROM usaspending.government_furnished_prope THEN jsonb_build_object('broker', broker.government_furnished_prope, 'usaspending', usaspending.government_furnished_prope) ELSE null END,
            'government_furnished_desc', CASE WHEN broker.government_furnished_desc IS DISTINCT FROM usaspending.government_furnished_desc THEN jsonb_build_object('broker', broker.government_furnished_desc, 'usaspending', usaspending.government_furnished_desc) ELSE null END,
            'information_technology_com', CASE WHEN broker.information_technology_com IS DISTINCT FROM usaspending.information_technology_com THEN jsonb_build_object('broker', broker.information_technology_com, 'usaspending', usaspending.information_technology_com) ELSE null END,
            'information_technolog_desc', CASE WHEN broker.information_technolog_desc IS DISTINCT FROM usaspending.information_technolog_desc THEN jsonb_build_object('broker', broker.information_technolog_desc, 'usaspending', usaspending.information_technolog_desc) ELSE null END,
            'interagency_contracting_au', CASE WHEN broker.interagency_contracting_au IS DISTINCT FROM usaspending.interagency_contracting_au THEN jsonb_build_object('broker', broker.interagency_contracting_au, 'usaspending', usaspending.interagency_contracting_au) ELSE null END,
            'interagency_contract_desc', CASE WHEN broker.interagency_contract_desc IS DISTINCT FROM usaspending.interagency_contract_desc THEN jsonb_build_object('broker', broker.interagency_contract_desc, 'usaspending', usaspending.interagency_contract_desc) ELSE null END,
            'local_area_set_aside', CASE WHEN broker.local_area_set_aside IS DISTINCT FROM usaspending.local_area_set_aside THEN jsonb_build_object('broker', broker.local_area_set_aside, 'usaspending', usaspending.local_area_set_aside) ELSE null END,
            'local_area_set_aside_desc', CASE WHEN broker.local_area_set_aside_desc IS DISTINCT FROM usaspending.local_area_set_aside_desc THEN jsonb_build_object('broker', broker.local_area_set_aside_desc, 'usaspending', usaspending.local_area_set_aside_desc) ELSE null END,
            'major_program', CASE WHEN broker.major_program IS DISTINCT FROM usaspending.major_program THEN jsonb_build_object('broker', broker.major_program, 'usaspending', usaspending.major_program) ELSE null END,
            'purchase_card_as_payment_m', CASE WHEN broker.purchase_card_as_payment_m IS DISTINCT FROM usaspending.purchase_card_as_payment_m THEN jsonb_build_object('broker', broker.purchase_card_as_payment_m, 'usaspending', usaspending.purchase_card_as_payment_m) ELSE null END,
            'purchase_card_as_paym_desc', CASE WHEN broker.purchase_card_as_paym_desc IS DISTINCT FROM usaspending.purchase_card_as_paym_desc THEN jsonb_build_object('broker', broker.purchase_card_as_paym_desc, 'usaspending', usaspending.purchase_card_as_paym_desc) ELSE null END,
            'multi_year_contract', CASE WHEN broker.multi_year_contract IS DISTINCT FROM usaspending.multi_year_contract THEN jsonb_build_object('broker', broker.multi_year_contract, 'usaspending', usaspending.multi_year_contract) ELSE null END,
            'multi_year_contract_desc', CASE WHEN broker.multi_year_contract_desc IS DISTINCT FROM usaspending.multi_year_contract_desc THEN jsonb_build_object('broker', broker.multi_year_contract_desc, 'usaspending', usaspending.multi_year_contract_desc) ELSE null END,
            'national_interest_action', CASE WHEN broker.national_interest_action IS DISTINCT FROM usaspending.national_interest_action THEN jsonb_build_object('broker', broker.national_interest_action, 'usaspending', usaspending.national_interest_action) ELSE null END,
            'national_interest_desc', CASE WHEN broker.national_interest_desc IS DISTINCT FROM usaspending.national_interest_desc THEN jsonb_build_object('broker', broker.national_interest_desc, 'usaspending', usaspending.national_interest_desc) ELSE null END,
            'number_of_actions', CASE WHEN broker.number_of_actions IS DISTINCT FROM usaspending.number_of_actions THEN jsonb_build_object('broker', broker.number_of_actions, 'usaspending', usaspending.number_of_actions) ELSE null END,
            'number_of_offers_received', CASE WHEN broker.number_of_offers_received IS DISTINCT FROM usaspending.number_of_offers_received THEN jsonb_build_object('broker', broker.number_of_offers_received, 'usaspending', usaspending.number_of_offers_received) ELSE null END,
            'other_statutory_authority', CASE WHEN broker.other_statutory_authority IS DISTINCT FROM usaspending.other_statutory_authority THEN jsonb_build_object('broker', broker.other_statutory_authority, 'usaspending', usaspending.other_statutory_authority) ELSE null END,
            'performance_based_service', CASE WHEN broker.performance_based_service IS DISTINCT FROM usaspending.performance_based_service THEN jsonb_build_object('broker', broker.performance_based_service, 'usaspending', usaspending.performance_based_service) ELSE null END,
            'performance_based_se_desc', CASE WHEN broker.performance_based_se_desc IS DISTINCT FROM usaspending.performance_based_se_desc THEN jsonb_build_object('broker', broker.performance_based_se_desc, 'usaspending', usaspending.performance_based_se_desc) ELSE null END,
            'place_of_manufacture', CASE WHEN broker.place_of_manufacture IS DISTINCT FROM usaspending.place_of_manufacture THEN jsonb_build_object('broker', broker.place_of_manufacture, 'usaspending', usaspending.place_of_manufacture) ELSE null END,
            'place_of_manufacture_desc', CASE WHEN broker.place_of_manufacture_desc IS DISTINCT FROM usaspending.place_of_manufacture_desc THEN jsonb_build_object('broker', broker.place_of_manufacture_desc, 'usaspending', usaspending.place_of_manufacture_desc) ELSE null END,
            'price_evaluation_adjustmen', CASE WHEN broker.price_evaluation_adjustmen IS DISTINCT FROM usaspending.price_evaluation_adjustmen THEN jsonb_build_object('broker', broker.price_evaluation_adjustmen, 'usaspending', usaspending.price_evaluation_adjustmen) ELSE null END,
            'product_or_service_code', CASE WHEN broker.product_or_service_code IS DISTINCT FROM usaspending.product_or_service_code THEN jsonb_build_object('broker', broker.product_or_service_code, 'usaspending', usaspending.product_or_service_code) ELSE null END,
            'product_or_service_co_desc', CASE WHEN broker.product_or_service_co_desc IS DISTINCT FROM usaspending.product_or_service_co_desc THEN jsonb_build_object('broker', broker.product_or_service_co_desc, 'usaspending', usaspending.product_or_service_co_desc) ELSE null END,
            'program_acronym', CASE WHEN broker.program_acronym IS DISTINCT FROM usaspending.program_acronym THEN jsonb_build_object('broker', broker.program_acronym, 'usaspending', usaspending.program_acronym) ELSE null END,
            'other_than_full_and_open_c', CASE WHEN broker.other_than_full_and_open_c IS DISTINCT FROM usaspending.other_than_full_and_open_c THEN jsonb_build_object('broker', broker.other_than_full_and_open_c, 'usaspending', usaspending.other_than_full_and_open_c) ELSE null END
        ) || jsonb_build_object(
            'other_than_full_and_o_desc', CASE WHEN broker.other_than_full_and_o_desc IS DISTINCT FROM usaspending.other_than_full_and_o_desc THEN jsonb_build_object('broker', broker.other_than_full_and_o_desc, 'usaspending', usaspending.other_than_full_and_o_desc) ELSE null END,
            'recovered_materials_sustai', CASE WHEN broker.recovered_materials_sustai IS DISTINCT FROM usaspending.recovered_materials_sustai THEN jsonb_build_object('broker', broker.recovered_materials_sustai, 'usaspending', usaspending.recovered_materials_sustai) ELSE null END,
            'recovered_materials_s_desc', CASE WHEN broker.recovered_materials_s_desc IS DISTINCT FROM usaspending.recovered_materials_s_desc THEN jsonb_build_object('broker', broker.recovered_materials_s_desc, 'usaspending', usaspending.recovered_materials_s_desc) ELSE null END,
            'research', CASE WHEN broker.research IS DISTINCT FROM usaspending.research THEN jsonb_build_object('broker', broker.research, 'usaspending', usaspending.research) ELSE null END,
            'research_description', CASE WHEN broker.research_description IS DISTINCT FROM usaspending.research_description THEN jsonb_build_object('broker', broker.research_description, 'usaspending', usaspending.research_description) ELSE null END,
            'sea_transportation', CASE WHEN broker.sea_transportation IS DISTINCT FROM usaspending.sea_transportation THEN jsonb_build_object('broker', broker.sea_transportation, 'usaspending', usaspending.sea_transportation) ELSE null END,
            'sea_transportation_desc', CASE WHEN broker.sea_transportation_desc IS DISTINCT FROM usaspending.sea_transportation_desc THEN jsonb_build_object('broker', broker.sea_transportation_desc, 'usaspending', usaspending.sea_transportation_desc) ELSE null END,
            'labor_standards', CASE WHEN broker.labor_standards IS DISTINCT FROM usaspending.labor_standards THEN jsonb_build_object('broker', broker.labor_standards, 'usaspending', usaspending.labor_standards) ELSE null END,
            'labor_standards_descrip', CASE WHEN broker.labor_standards_descrip IS DISTINCT FROM usaspending.labor_standards_descrip THEN jsonb_build_object('broker', broker.labor_standards_descrip, 'usaspending', usaspending.labor_standards_descrip) ELSE null END,
            'small_business_competitive', CASE WHEN broker.small_business_competitive IS DISTINCT FROM usaspending.small_business_competitive THEN jsonb_build_object('broker', broker.small_business_competitive, 'usaspending', usaspending.small_business_competitive) ELSE null END,
            'solicitation_identifier', CASE WHEN broker.solicitation_identifier IS DISTINCT FROM usaspending.solicitation_identifier THEN jsonb_build_object('broker', broker.solicitation_identifier, 'usaspending', usaspending.solicitation_identifier) ELSE null END,
            'solicitation_procedures', CASE WHEN broker.solicitation_procedures IS DISTINCT FROM usaspending.solicitation_procedures THEN jsonb_build_object('broker', broker.solicitation_procedures, 'usaspending', usaspending.solicitation_procedures) ELSE null END,
            'solicitation_procedur_desc', CASE WHEN broker.solicitation_procedur_desc IS DISTINCT FROM usaspending.solicitation_procedur_desc THEN jsonb_build_object('broker', broker.solicitation_procedur_desc, 'usaspending', usaspending.solicitation_procedur_desc) ELSE null END,
            'fair_opportunity_limited_s', CASE WHEN broker.fair_opportunity_limited_s IS DISTINCT FROM usaspending.fair_opportunity_limited_s THEN jsonb_build_object('broker', broker.fair_opportunity_limited_s, 'usaspending', usaspending.fair_opportunity_limited_s) ELSE null END,
            'fair_opportunity_limi_desc', CASE WHEN broker.fair_opportunity_limi_desc IS DISTINCT FROM usaspending.fair_opportunity_limi_desc THEN jsonb_build_object('broker', broker.fair_opportunity_limi_desc, 'usaspending', usaspending.fair_opportunity_limi_desc) ELSE null END,
            'subcontracting_plan', CASE WHEN broker.subcontracting_plan IS DISTINCT FROM usaspending.subcontracting_plan THEN jsonb_build_object('broker', broker.subcontracting_plan, 'usaspending', usaspending.subcontracting_plan) ELSE null END,
            'subcontracting_plan_desc', CASE WHEN broker.subcontracting_plan_desc IS DISTINCT FROM usaspending.subcontracting_plan_desc THEN jsonb_build_object('broker', broker.subcontracting_plan_desc, 'usaspending', usaspending.subcontracting_plan_desc) ELSE null END,
            'program_system_or_equipmen', CASE WHEN broker.program_system_or_equipmen IS DISTINCT FROM usaspending.program_system_or_equipmen THEN jsonb_build_object('broker', broker.program_system_or_equipmen, 'usaspending', usaspending.program_system_or_equipmen) ELSE null END,
            'program_system_or_equ_desc', CASE WHEN broker.program_system_or_equ_desc IS DISTINCT FROM usaspending.program_system_or_equ_desc THEN jsonb_build_object('broker', broker.program_system_or_equ_desc, 'usaspending', usaspending.program_system_or_equ_desc) ELSE null END,
            'type_set_aside', CASE WHEN broker.type_set_aside IS DISTINCT FROM usaspending.type_set_aside THEN jsonb_build_object('broker', broker.type_set_aside, 'usaspending', usaspending.type_set_aside) ELSE null END,
            'type_set_aside_description', CASE WHEN broker.type_set_aside_description IS DISTINCT FROM usaspending.type_set_aside_description THEN jsonb_build_object('broker', broker.type_set_aside_description, 'usaspending', usaspending.type_set_aside_description) ELSE null END,
            'epa_designated_product', CASE WHEN broker.epa_designated_product IS DISTINCT FROM usaspending.epa_designated_product THEN jsonb_build_object('broker', broker.epa_designated_product, 'usaspending', usaspending.epa_designated_product) ELSE null END,
            'epa_designated_produc_desc', CASE WHEN broker.epa_designated_produc_desc IS DISTINCT FROM usaspending.epa_designated_produc_desc THEN jsonb_build_object('broker', broker.epa_designated_produc_desc, 'usaspending', usaspending.epa_designated_produc_desc) ELSE null END,
            'materials_supplies_article', CASE WHEN broker.materials_supplies_article IS DISTINCT FROM usaspending.materials_supplies_article THEN jsonb_build_object('broker', broker.materials_supplies_article, 'usaspending', usaspending.materials_supplies_article) ELSE null END,
            'materials_supplies_descrip', CASE WHEN broker.materials_supplies_descrip IS DISTINCT FROM usaspending.materials_supplies_descrip THEN jsonb_build_object('broker', broker.materials_supplies_descrip, 'usaspending', usaspending.materials_supplies_descrip) ELSE null END,
            'transaction_number', CASE WHEN broker.transaction_number IS DISTINCT FROM usaspending.transaction_number THEN jsonb_build_object('broker', broker.transaction_number, 'usaspending', usaspending.transaction_number) ELSE null END,
            'sam_exception', CASE WHEN broker.sam_exception IS DISTINCT FROM usaspending.sam_exception THEN jsonb_build_object('broker', broker.sam_exception, 'usaspending', usaspending.sam_exception) ELSE null END,
            'sam_exception_description', CASE WHEN broker.sam_exception_description IS DISTINCT FROM usaspending.sam_exception_description THEN jsonb_build_object('broker', broker.sam_exception_description, 'usaspending', usaspending.sam_exception_description) ELSE null END,
            'city_local_government', CASE WHEN broker.city_local_government IS DISTINCT FROM usaspending.city_local_government THEN jsonb_build_object('broker', broker.city_local_government, 'usaspending', usaspending.city_local_government) ELSE null END,
            'county_local_government', CASE WHEN broker.county_local_government IS DISTINCT FROM usaspending.county_local_government THEN jsonb_build_object('broker', broker.county_local_government, 'usaspending', usaspending.county_local_government) ELSE null END,
            'inter_municipal_local_gove', CASE WHEN broker.inter_municipal_local_gove IS DISTINCT FROM usaspending.inter_municipal_local_gove THEN jsonb_build_object('broker', broker.inter_municipal_local_gove, 'usaspending', usaspending.inter_municipal_local_gove) ELSE null END,
            'local_government_owned', CASE WHEN broker.local_government_owned IS DISTINCT FROM usaspending.local_government_owned THEN jsonb_build_object('broker', broker.local_government_owned, 'usaspending', usaspending.local_government_owned) ELSE null END,
            'municipality_local_governm', CASE WHEN broker.municipality_local_governm IS DISTINCT FROM usaspending.municipality_local_governm THEN jsonb_build_object('broker', broker.municipality_local_governm, 'usaspending', usaspending.municipality_local_governm) ELSE null END,
            'school_district_local_gove', CASE WHEN broker.school_district_local_gove IS DISTINCT FROM usaspending.school_district_local_gove THEN jsonb_build_object('broker', broker.school_district_local_gove, 'usaspending', usaspending.school_district_local_gove) ELSE null END,
            'township_local_government', CASE WHEN broker.township_local_government IS DISTINCT FROM usaspending.township_local_government THEN jsonb_build_object('broker', broker.township_local_government, 'usaspending', usaspending.township_local_government) ELSE null END,
            'us_state_government', CASE WHEN broker.us_state_government IS DISTINCT FROM usaspending.us_state_government THEN jsonb_build_object('broker', broker.us_state_government, 'usaspending', usaspending.us_state_government) ELSE null END,
            'us_federal_government', CASE WHEN broker.us_federal_government IS DISTINCT FROM usaspending.us_federal_government THEN jsonb_build_object('broker', broker.us_federal_government, 'usaspending', usaspending.us_federal_government) ELSE null END,
            'federal_agency', CASE WHEN broker.federal_agency IS DISTINCT FROM usaspending.federal_agency THEN jsonb_build_object('broker', broker.federal_agency, 'usaspending', usaspending.federal_agency) ELSE null END,
            'federally_funded_research', CASE WHEN broker.federally_funded_research IS DISTINCT FROM usaspending.federally_funded_research THEN jsonb_build_object('broker', broker.federally_funded_research, 'usaspending', usaspending.federally_funded_research) ELSE null END,
            'us_tribal_government', CASE WHEN broker.us_tribal_government IS DISTINCT FROM usaspending.us_tribal_government THEN jsonb_build_object('broker', broker.us_tribal_government, 'usaspending', usaspending.us_tribal_government) ELSE null END,
            'foreign_government', CASE WHEN broker.foreign_government IS DISTINCT FROM usaspending.foreign_government THEN jsonb_build_object('broker', broker.foreign_government, 'usaspending', usaspending.foreign_government) ELSE null END,
            'community_developed_corpor', CASE WHEN broker.community_developed_corpor IS DISTINCT FROM usaspending.community_developed_corpor THEN jsonb_build_object('broker', broker.community_developed_corpor, 'usaspending', usaspending.community_developed_corpor) ELSE null END,
            'labor_surplus_area_firm', CASE WHEN broker.labor_surplus_area_firm IS DISTINCT FROM usaspending.labor_surplus_area_firm THEN jsonb_build_object('broker', broker.labor_surplus_area_firm, 'usaspending', usaspending.labor_surplus_area_firm) ELSE null END,
            'corporate_entity_not_tax_e', CASE WHEN broker.corporate_entity_not_tax_e IS DISTINCT FROM usaspending.corporate_entity_not_tax_e THEN jsonb_build_object('broker', broker.corporate_entity_not_tax_e, 'usaspending', usaspending.corporate_entity_not_tax_e) ELSE null END,
            'corporate_entity_tax_exemp', CASE WHEN broker.corporate_entity_tax_exemp IS DISTINCT FROM usaspending.corporate_entity_tax_exemp THEN jsonb_build_object('broker', broker.corporate_entity_tax_exemp, 'usaspending', usaspending.corporate_entity_tax_exemp) ELSE null END
        ) || jsonb_build_object(
            'partnership_or_limited_lia', CASE WHEN broker.partnership_or_limited_lia IS DISTINCT FROM usaspending.partnership_or_limited_lia THEN jsonb_build_object('broker', broker.partnership_or_limited_lia, 'usaspending', usaspending.partnership_or_limited_lia) ELSE null END,
            'sole_proprietorship', CASE WHEN broker.sole_proprietorship IS DISTINCT FROM usaspending.sole_proprietorship THEN jsonb_build_object('broker', broker.sole_proprietorship, 'usaspending', usaspending.sole_proprietorship) ELSE null END,
            'small_agricultural_coopera', CASE WHEN broker.small_agricultural_coopera IS DISTINCT FROM usaspending.small_agricultural_coopera THEN jsonb_build_object('broker', broker.small_agricultural_coopera, 'usaspending', usaspending.small_agricultural_coopera) ELSE null END,
            'international_organization', CASE WHEN broker.international_organization IS DISTINCT FROM usaspending.international_organization THEN jsonb_build_object('broker', broker.international_organization, 'usaspending', usaspending.international_organization) ELSE null END,
            'us_government_entity', CASE WHEN broker.us_government_entity IS DISTINCT FROM usaspending.us_government_entity THEN jsonb_build_object('broker', broker.us_government_entity, 'usaspending', usaspending.us_government_entity) ELSE null END,
            'emerging_small_business', CASE WHEN broker.emerging_small_business IS DISTINCT FROM usaspending.emerging_small_business THEN jsonb_build_object('broker', broker.emerging_small_business, 'usaspending', usaspending.emerging_small_business) ELSE null END,
            'c8a_program_participant', CASE WHEN broker.c8a_program_participant IS DISTINCT FROM usaspending.c8a_program_participant THEN jsonb_build_object('broker', broker.c8a_program_participant, 'usaspending', usaspending.c8a_program_participant) ELSE null END,
            'sba_certified_8_a_joint_ve', CASE WHEN broker.sba_certified_8_a_joint_ve IS DISTINCT FROM usaspending.sba_certified_8_a_joint_ve THEN jsonb_build_object('broker', broker.sba_certified_8_a_joint_ve, 'usaspending', usaspending.sba_certified_8_a_joint_ve) ELSE null END,
            'dot_certified_disadvantage', CASE WHEN broker.dot_certified_disadvantage IS DISTINCT FROM usaspending.dot_certified_disadvantage THEN jsonb_build_object('broker', broker.dot_certified_disadvantage, 'usaspending', usaspending.dot_certified_disadvantage) ELSE null END,
            'self_certified_small_disad', CASE WHEN broker.self_certified_small_disad IS DISTINCT FROM usaspending.self_certified_small_disad THEN jsonb_build_object('broker', broker.self_certified_small_disad, 'usaspending', usaspending.self_certified_small_disad) ELSE null END,
            'historically_underutilized', CASE WHEN broker.historically_underutilized IS DISTINCT FROM usaspending.historically_underutilized THEN jsonb_build_object('broker', broker.historically_underutilized, 'usaspending', usaspending.historically_underutilized) ELSE null END,
            'small_disadvantaged_busine', CASE WHEN broker.small_disadvantaged_busine IS DISTINCT FROM usaspending.small_disadvantaged_busine THEN jsonb_build_object('broker', broker.small_disadvantaged_busine, 'usaspending', usaspending.small_disadvantaged_busine) ELSE null END,
            'the_ability_one_program', CASE WHEN broker.the_ability_one_program IS DISTINCT FROM usaspending.the_ability_one_program THEN jsonb_build_object('broker', broker.the_ability_one_program, 'usaspending', usaspending.the_ability_one_program) ELSE null END,
            'historically_black_college', CASE WHEN broker.historically_black_college IS DISTINCT FROM usaspending.historically_black_college THEN jsonb_build_object('broker', broker.historically_black_college, 'usaspending', usaspending.historically_black_college) ELSE null END,
            'c1862_land_grant_college', CASE WHEN broker.c1862_land_grant_college IS DISTINCT FROM usaspending.c1862_land_grant_college THEN jsonb_build_object('broker', broker.c1862_land_grant_college, 'usaspending', usaspending.c1862_land_grant_college) ELSE null END,
            'c1890_land_grant_college', CASE WHEN broker.c1890_land_grant_college IS DISTINCT FROM usaspending.c1890_land_grant_college THEN jsonb_build_object('broker', broker.c1890_land_grant_college, 'usaspending', usaspending.c1890_land_grant_college) ELSE null END,
            'c1994_land_grant_college', CASE WHEN broker.c1994_land_grant_college IS DISTINCT FROM usaspending.c1994_land_grant_college THEN jsonb_build_object('broker', broker.c1994_land_grant_college, 'usaspending', usaspending.c1994_land_grant_college) ELSE null END,
            'minority_institution', CASE WHEN broker.minority_institution IS DISTINCT FROM usaspending.minority_institution THEN jsonb_build_object('broker', broker.minority_institution, 'usaspending', usaspending.minority_institution) ELSE null END,
            'private_university_or_coll', CASE WHEN broker.private_university_or_coll IS DISTINCT FROM usaspending.private_university_or_coll THEN jsonb_build_object('broker', broker.private_university_or_coll, 'usaspending', usaspending.private_university_or_coll) ELSE null END,
            'school_of_forestry', CASE WHEN broker.school_of_forestry IS DISTINCT FROM usaspending.school_of_forestry THEN jsonb_build_object('broker', broker.school_of_forestry, 'usaspending', usaspending.school_of_forestry) ELSE null END,
            'state_controlled_instituti', CASE WHEN broker.state_controlled_instituti IS DISTINCT FROM usaspending.state_controlled_instituti THEN jsonb_build_object('broker', broker.state_controlled_instituti, 'usaspending', usaspending.state_controlled_instituti) ELSE null END,
            'tribal_college', CASE WHEN broker.tribal_college IS DISTINCT FROM usaspending.tribal_college THEN jsonb_build_object('broker', broker.tribal_college, 'usaspending', usaspending.tribal_college) ELSE null END,
            'veterinary_college', CASE WHEN broker.veterinary_college IS DISTINCT FROM usaspending.veterinary_college THEN jsonb_build_object('broker', broker.veterinary_college, 'usaspending', usaspending.veterinary_college) ELSE null END,
            'educational_institution', CASE WHEN broker.educational_institution IS DISTINCT FROM usaspending.educational_institution THEN jsonb_build_object('broker', broker.educational_institution, 'usaspending', usaspending.educational_institution) ELSE null END,
            'alaskan_native_servicing_i', CASE WHEN broker.alaskan_native_servicing_i IS DISTINCT FROM usaspending.alaskan_native_servicing_i THEN jsonb_build_object('broker', broker.alaskan_native_servicing_i, 'usaspending', usaspending.alaskan_native_servicing_i) ELSE null END,
            'community_development_corp', CASE WHEN broker.community_development_corp IS DISTINCT FROM usaspending.community_development_corp THEN jsonb_build_object('broker', broker.community_development_corp, 'usaspending', usaspending.community_development_corp) ELSE null END,
            'native_hawaiian_servicing', CASE WHEN broker.native_hawaiian_servicing IS DISTINCT FROM usaspending.native_hawaiian_servicing THEN jsonb_build_object('broker', broker.native_hawaiian_servicing, 'usaspending', usaspending.native_hawaiian_servicing) ELSE null END,
            'domestic_shelter', CASE WHEN broker.domestic_shelter IS DISTINCT FROM usaspending.domestic_shelter THEN jsonb_build_object('broker', broker.domestic_shelter, 'usaspending', usaspending.domestic_shelter) ELSE null END,
            'manufacturer_of_goods', CASE WHEN broker.manufacturer_of_goods IS DISTINCT FROM usaspending.manufacturer_of_goods THEN jsonb_build_object('broker', broker.manufacturer_of_goods, 'usaspending', usaspending.manufacturer_of_goods) ELSE null END,
            'hospital_flag', CASE WHEN broker.hospital_flag IS DISTINCT FROM usaspending.hospital_flag THEN jsonb_build_object('broker', broker.hospital_flag, 'usaspending', usaspending.hospital_flag) ELSE null END,
            'veterinary_hospital', CASE WHEN broker.veterinary_hospital IS DISTINCT FROM usaspending.veterinary_hospital THEN jsonb_build_object('broker', broker.veterinary_hospital, 'usaspending', usaspending.veterinary_hospital) ELSE null END,
            'hispanic_servicing_institu', CASE WHEN broker.hispanic_servicing_institu IS DISTINCT FROM usaspending.hispanic_servicing_institu THEN jsonb_build_object('broker', broker.hispanic_servicing_institu, 'usaspending', usaspending.hispanic_servicing_institu) ELSE null END,
            'foundation', CASE WHEN broker.foundation IS DISTINCT FROM usaspending.foundation THEN jsonb_build_object('broker', broker.foundation, 'usaspending', usaspending.foundation) ELSE null END,
            'woman_owned_business', CASE WHEN broker.woman_owned_business IS DISTINCT FROM usaspending.woman_owned_business THEN jsonb_build_object('broker', broker.woman_owned_business, 'usaspending', usaspending.woman_owned_business) ELSE null END,
            'minority_owned_business', CASE WHEN broker.minority_owned_business IS DISTINCT FROM usaspending.minority_owned_business THEN jsonb_build_object('broker', broker.minority_owned_business, 'usaspending', usaspending.minority_owned_business) ELSE null END,
            'women_owned_small_business', CASE WHEN broker.women_owned_small_business IS DISTINCT FROM usaspending.women_owned_small_business THEN jsonb_build_object('broker', broker.women_owned_small_business, 'usaspending', usaspending.women_owned_small_business) ELSE null END,
            'economically_disadvantaged', CASE WHEN broker.economically_disadvantaged IS DISTINCT FROM usaspending.economically_disadvantaged THEN jsonb_build_object('broker', broker.economically_disadvantaged, 'usaspending', usaspending.economically_disadvantaged) ELSE null END,
            'joint_venture_women_owned', CASE WHEN broker.joint_venture_women_owned IS DISTINCT FROM usaspending.joint_venture_women_owned THEN jsonb_build_object('broker', broker.joint_venture_women_owned, 'usaspending', usaspending.joint_venture_women_owned) ELSE null END,
            'joint_venture_economically', CASE WHEN broker.joint_venture_economically IS DISTINCT FROM usaspending.joint_venture_economically THEN jsonb_build_object('broker', broker.joint_venture_economically, 'usaspending', usaspending.joint_venture_economically) ELSE null END,
            'veteran_owned_business', CASE WHEN broker.veteran_owned_business IS DISTINCT FROM usaspending.veteran_owned_business THEN jsonb_build_object('broker', broker.veteran_owned_business, 'usaspending', usaspending.veteran_owned_business) ELSE null END,
            'service_disabled_veteran_o', CASE WHEN broker.service_disabled_veteran_o IS DISTINCT FROM usaspending.service_disabled_veteran_o THEN jsonb_build_object('broker', broker.service_disabled_veteran_o, 'usaspending', usaspending.service_disabled_veteran_o) ELSE null END,
            'contracts', CASE WHEN broker.contracts IS DISTINCT FROM usaspending.contracts THEN jsonb_build_object('broker', broker.contracts, 'usaspending', usaspending.contracts) ELSE null END,
            'grants', CASE WHEN broker.grants IS DISTINCT FROM usaspending.grants THEN jsonb_build_object('broker', broker.grants, 'usaspending', usaspending.grants) ELSE null END,
            'receives_contracts_and_gra', CASE WHEN broker.receives_contracts_and_gra IS DISTINCT FROM usaspending.receives_contracts_and_gra THEN jsonb_build_object('broker', broker.receives_contracts_and_gra, 'usaspending', usaspending.receives_contracts_and_gra) ELSE null END,
            'airport_authority', CASE WHEN broker.airport_authority IS DISTINCT FROM usaspending.airport_authority THEN jsonb_build_object('broker', broker.airport_authority, 'usaspending', usaspending.airport_authority) ELSE null END
        ) || jsonb_build_object(
            'council_of_governments', CASE WHEN broker.council_of_governments IS DISTINCT FROM usaspending.council_of_governments THEN jsonb_build_object('broker', broker.council_of_governments, 'usaspending', usaspending.council_of_governments) ELSE null END,
            'housing_authorities_public', CASE WHEN broker.housing_authorities_public IS DISTINCT FROM usaspending.housing_authorities_public THEN jsonb_build_object('broker', broker.housing_authorities_public, 'usaspending', usaspending.housing_authorities_public) ELSE null END,
            'interstate_entity', CASE WHEN broker.interstate_entity IS DISTINCT FROM usaspending.interstate_entity THEN jsonb_build_object('broker', broker.interstate_entity, 'usaspending', usaspending.interstate_entity) ELSE null END,
            'planning_commission', CASE WHEN broker.planning_commission IS DISTINCT FROM usaspending.planning_commission THEN jsonb_build_object('broker', broker.planning_commission, 'usaspending', usaspending.planning_commission) ELSE null END,
            'port_authority', CASE WHEN broker.port_authority IS DISTINCT FROM usaspending.port_authority THEN jsonb_build_object('broker', broker.port_authority, 'usaspending', usaspending.port_authority) ELSE null END,
            'transit_authority', CASE WHEN broker.transit_authority IS DISTINCT FROM usaspending.transit_authority THEN jsonb_build_object('broker', broker.transit_authority, 'usaspending', usaspending.transit_authority) ELSE null END,
            'subchapter_s_corporation', CASE WHEN broker.subchapter_s_corporation IS DISTINCT FROM usaspending.subchapter_s_corporation THEN jsonb_build_object('broker', broker.subchapter_s_corporation, 'usaspending', usaspending.subchapter_s_corporation) ELSE null END,
            'limited_liability_corporat', CASE WHEN broker.limited_liability_corporat IS DISTINCT FROM usaspending.limited_liability_corporat THEN jsonb_build_object('broker', broker.limited_liability_corporat, 'usaspending', usaspending.limited_liability_corporat) ELSE null END,
            'foreign_owned_and_located', CASE WHEN broker.foreign_owned_and_located IS DISTINCT FROM usaspending.foreign_owned_and_located THEN jsonb_build_object('broker', broker.foreign_owned_and_located, 'usaspending', usaspending.foreign_owned_and_located) ELSE null END,
            'american_indian_owned_busi', CASE WHEN broker.american_indian_owned_busi IS DISTINCT FROM usaspending.american_indian_owned_busi THEN jsonb_build_object('broker', broker.american_indian_owned_busi, 'usaspending', usaspending.american_indian_owned_busi) ELSE null END,
            'alaskan_native_owned_corpo', CASE WHEN broker.alaskan_native_owned_corpo IS DISTINCT FROM usaspending.alaskan_native_owned_corpo THEN jsonb_build_object('broker', broker.alaskan_native_owned_corpo, 'usaspending', usaspending.alaskan_native_owned_corpo) ELSE null END,
            'indian_tribe_federally_rec', CASE WHEN broker.indian_tribe_federally_rec IS DISTINCT FROM usaspending.indian_tribe_federally_rec THEN jsonb_build_object('broker', broker.indian_tribe_federally_rec, 'usaspending', usaspending.indian_tribe_federally_rec) ELSE null END,
            'native_hawaiian_owned_busi', CASE WHEN broker.native_hawaiian_owned_busi IS DISTINCT FROM usaspending.native_hawaiian_owned_busi THEN jsonb_build_object('broker', broker.native_hawaiian_owned_busi, 'usaspending', usaspending.native_hawaiian_owned_busi) ELSE null END,
            'tribally_owned_business', CASE WHEN broker.tribally_owned_business IS DISTINCT FROM usaspending.tribally_owned_business THEN jsonb_build_object('broker', broker.tribally_owned_business, 'usaspending', usaspending.tribally_owned_business) ELSE null END,
            'asian_pacific_american_own', CASE WHEN broker.asian_pacific_american_own IS DISTINCT FROM usaspending.asian_pacific_american_own THEN jsonb_build_object('broker', broker.asian_pacific_american_own, 'usaspending', usaspending.asian_pacific_american_own) ELSE null END,
            'black_american_owned_busin', CASE WHEN broker.black_american_owned_busin IS DISTINCT FROM usaspending.black_american_owned_busin THEN jsonb_build_object('broker', broker.black_american_owned_busin, 'usaspending', usaspending.black_american_owned_busin) ELSE null END,
            'hispanic_american_owned_bu', CASE WHEN broker.hispanic_american_owned_bu IS DISTINCT FROM usaspending.hispanic_american_owned_bu THEN jsonb_build_object('broker', broker.hispanic_american_owned_bu, 'usaspending', usaspending.hispanic_american_owned_bu) ELSE null END,
            'native_american_owned_busi', CASE WHEN broker.native_american_owned_busi IS DISTINCT FROM usaspending.native_american_owned_busi THEN jsonb_build_object('broker', broker.native_american_owned_busi, 'usaspending', usaspending.native_american_owned_busi) ELSE null END,
            'subcontinent_asian_asian_i', CASE WHEN broker.subcontinent_asian_asian_i IS DISTINCT FROM usaspending.subcontinent_asian_asian_i THEN jsonb_build_object('broker', broker.subcontinent_asian_asian_i, 'usaspending', usaspending.subcontinent_asian_asian_i) ELSE null END,
            'other_minority_owned_busin', CASE WHEN broker.other_minority_owned_busin IS DISTINCT FROM usaspending.other_minority_owned_busin THEN jsonb_build_object('broker', broker.other_minority_owned_busin, 'usaspending', usaspending.other_minority_owned_busin) ELSE null END,
            'for_profit_organization', CASE WHEN broker.for_profit_organization IS DISTINCT FROM usaspending.for_profit_organization THEN jsonb_build_object('broker', broker.for_profit_organization, 'usaspending', usaspending.for_profit_organization) ELSE null END,
            'nonprofit_organization', CASE WHEN broker.nonprofit_organization IS DISTINCT FROM usaspending.nonprofit_organization THEN jsonb_build_object('broker', broker.nonprofit_organization, 'usaspending', usaspending.nonprofit_organization) ELSE null END,
            'other_not_for_profit_organ', CASE WHEN broker.other_not_for_profit_organ IS DISTINCT FROM usaspending.other_not_for_profit_organ THEN jsonb_build_object('broker', broker.other_not_for_profit_organ, 'usaspending', usaspending.other_not_for_profit_organ) ELSE null END,
            'us_local_government', CASE WHEN broker.us_local_government IS DISTINCT FROM usaspending.us_local_government THEN jsonb_build_object('broker', broker.us_local_government, 'usaspending', usaspending.us_local_government) ELSE null END,
            'referenced_idv_modificatio', CASE WHEN broker.referenced_idv_modificatio IS DISTINCT FROM usaspending.referenced_idv_modificatio THEN jsonb_build_object('broker', broker.referenced_idv_modificatio, 'usaspending', usaspending.referenced_idv_modificatio) ELSE null END,
            'undefinitized_action', CASE WHEN broker.undefinitized_action IS DISTINCT FROM usaspending.undefinitized_action THEN jsonb_build_object('broker', broker.undefinitized_action, 'usaspending', usaspending.undefinitized_action) ELSE null END,
            'undefinitized_action_desc', CASE WHEN broker.undefinitized_action_desc IS DISTINCT FROM usaspending.undefinitized_action_desc THEN jsonb_build_object('broker', broker.undefinitized_action_desc, 'usaspending', usaspending.undefinitized_action_desc) ELSE null END,
            'domestic_or_foreign_entity', CASE WHEN broker.domestic_or_foreign_entity IS DISTINCT FROM usaspending.domestic_or_foreign_entity THEN jsonb_build_object('broker', broker.domestic_or_foreign_entity, 'usaspending', usaspending.domestic_or_foreign_entity) ELSE null END,
            'domestic_or_foreign_e_desc', CASE WHEN broker.domestic_or_foreign_e_desc IS DISTINCT FROM usaspending.domestic_or_foreign_e_desc THEN jsonb_build_object('broker', broker.domestic_or_foreign_e_desc, 'usaspending', usaspending.domestic_or_foreign_e_desc) ELSE null END,
            'pulled_from', CASE WHEN broker.pulled_from IS DISTINCT FROM usaspending.pulled_from THEN jsonb_build_object('broker', broker.pulled_from, 'usaspending', usaspending.pulled_from) ELSE null END,
            'last_modified', CASE WHEN broker.last_modified IS DISTINCT FROM usaspending.last_modified THEN jsonb_build_object('broker', broker.last_modified, 'usaspending', usaspending.last_modified) ELSE null END,
            'annual_revenue', CASE WHEN broker.annual_revenue IS DISTINCT FROM usaspending.annual_revenue THEN jsonb_build_object('broker', broker.annual_revenue, 'usaspending', usaspending.annual_revenue) ELSE null END,
            'division_name', CASE WHEN broker.division_name IS DISTINCT FROM usaspending.division_name THEN jsonb_build_object('broker', broker.division_name, 'usaspending', usaspending.division_name) ELSE null END,
            'division_number_or_office', CASE WHEN broker.division_number_or_office IS DISTINCT FROM usaspending.division_number_or_office THEN jsonb_build_object('broker', broker.division_number_or_office, 'usaspending', usaspending.division_number_or_office) ELSE null END,
            'number_of_employees', CASE WHEN broker.number_of_employees IS DISTINCT FROM usaspending.number_of_employees THEN jsonb_build_object('broker', broker.number_of_employees, 'usaspending', usaspending.number_of_employees) ELSE null END,
            'vendor_alternate_name', CASE WHEN broker.vendor_alternate_name IS DISTINCT FROM usaspending.vendor_alternate_name THEN jsonb_build_object('broker', broker.vendor_alternate_name, 'usaspending', usaspending.vendor_alternate_name) ELSE null END,
            'vendor_alternate_site_code', CASE WHEN broker.vendor_alternate_site_code IS DISTINCT FROM usaspending.vendor_alternate_site_code THEN jsonb_build_object('broker', broker.vendor_alternate_site_code, 'usaspending', usaspending.vendor_alternate_site_code) ELSE null END,
            'vendor_enabled', CASE WHEN broker.vendor_enabled IS DISTINCT FROM usaspending.vendor_enabled THEN jsonb_build_object('broker', broker.vendor_enabled, 'usaspending', usaspending.vendor_enabled) ELSE null END,
            'vendor_legal_org_name', CASE WHEN broker.vendor_legal_org_name IS DISTINCT FROM usaspending.vendor_legal_org_name THEN jsonb_build_object('broker', broker.vendor_legal_org_name, 'usaspending', usaspending.vendor_legal_org_name) ELSE null END,
            'vendor_location_disabled_f', CASE WHEN broker.vendor_location_disabled_f IS DISTINCT FROM usaspending.vendor_location_disabled_f THEN jsonb_build_object('broker', broker.vendor_location_disabled_f, 'usaspending', usaspending.vendor_location_disabled_f) ELSE null END,
            'vendor_site_code', CASE WHEN broker.vendor_site_code IS DISTINCT FROM usaspending.vendor_site_code THEN jsonb_build_object('broker', broker.vendor_site_code, 'usaspending', usaspending.vendor_site_code) ELSE null END,
            'initial_report_date', CASE WHEN broker.initial_report_date IS DISTINCT FROM usaspending.initial_report_date THEN jsonb_build_object('broker', broker.initial_report_date, 'usaspending', usaspending.initial_report_date) ELSE null END,
            'base_and_all_options_value', CASE WHEN broker.base_and_all_options_value IS DISTINCT FROM usaspending.base_and_all_options_value THEN jsonb_build_object('broker', broker.base_and_all_options_value, 'usaspending', usaspending.base_and_all_options_value) ELSE null END,
            'base_exercised_options_val', CASE WHEN broker.base_exercised_options_val IS DISTINCT FROM usaspending.base_exercised_options_val THEN jsonb_build_object('broker', broker.base_exercised_options_val, 'usaspending', usaspending.base_exercised_options_val) ELSE null END,
            'total_obligated_amount', CASE WHEN broker.total_obligated_amount IS DISTINCT FROM usaspending.total_obligated_amount THEN jsonb_build_object('broker', broker.total_obligated_amount, 'usaspending', usaspending.total_obligated_amount) ELSE null END
        ) || jsonb_build_object(
            'place_of_perform_country_n', CASE WHEN broker.place_of_perform_country_n IS DISTINCT FROM usaspending.place_of_perform_country_n THEN jsonb_build_object('broker', broker.place_of_perform_country_n, 'usaspending', usaspending.place_of_perform_country_n) ELSE null END,
            'place_of_perform_state_nam', CASE WHEN broker.place_of_perform_state_nam IS DISTINCT FROM usaspending.place_of_perform_state_nam THEN jsonb_build_object('broker', broker.place_of_perform_state_nam, 'usaspending', usaspending.place_of_perform_state_nam) ELSE null END,
            'referenced_idv_agency_name', CASE WHEN broker.referenced_idv_agency_name IS DISTINCT FROM usaspending.referenced_idv_agency_name THEN jsonb_build_object('broker', broker.referenced_idv_agency_name, 'usaspending', usaspending.referenced_idv_agency_name) ELSE null END,
            'award_or_idv_flag', CASE WHEN broker.award_or_idv_flag IS DISTINCT FROM usaspending.award_or_idv_flag THEN jsonb_build_object('broker', broker.award_or_idv_flag, 'usaspending', usaspending.award_or_idv_flag) ELSE null END,
            'legal_entity_county_code', CASE WHEN broker.legal_entity_county_code IS DISTINCT FROM usaspending.legal_entity_county_code THEN jsonb_build_object('broker', broker.legal_entity_county_code, 'usaspending', usaspending.legal_entity_county_code) ELSE null END,
            'legal_entity_county_name', CASE WHEN broker.legal_entity_county_name IS DISTINCT FROM usaspending.legal_entity_county_name THEN jsonb_build_object('broker', broker.legal_entity_county_name, 'usaspending', usaspending.legal_entity_county_name) ELSE null END,
            'legal_entity_zip5', CASE WHEN broker.legal_entity_zip5 IS DISTINCT FROM usaspending.legal_entity_zip5 THEN jsonb_build_object('broker', broker.legal_entity_zip5, 'usaspending', usaspending.legal_entity_zip5) ELSE null END,
            'legal_entity_zip_last4', CASE WHEN broker.legal_entity_zip_last4 IS DISTINCT FROM usaspending.legal_entity_zip_last4 THEN jsonb_build_object('broker', broker.legal_entity_zip_last4, 'usaspending', usaspending.legal_entity_zip_last4) ELSE null END,
            'place_of_perform_county_co', CASE WHEN broker.place_of_perform_county_co IS DISTINCT FROM usaspending.place_of_perform_county_co THEN jsonb_build_object('broker', broker.place_of_perform_county_co, 'usaspending', usaspending.place_of_perform_county_co) ELSE null END,
            'place_of_performance_zip5', CASE WHEN broker.place_of_performance_zip5 IS DISTINCT FROM usaspending.place_of_performance_zip5 THEN jsonb_build_object('broker', broker.place_of_performance_zip5, 'usaspending', usaspending.place_of_performance_zip5) ELSE null END,
            'place_of_perform_zip_last4', CASE WHEN broker.place_of_perform_zip_last4 IS DISTINCT FROM usaspending.place_of_perform_zip_last4 THEN jsonb_build_object('broker', broker.place_of_perform_zip_last4, 'usaspending', usaspending.place_of_perform_zip_last4) ELSE null END,
            'cage_code', CASE WHEN broker.cage_code IS DISTINCT FROM usaspending.cage_code THEN jsonb_build_object('broker', broker.cage_code, 'usaspending', usaspending.cage_code) ELSE null END,
            'inherently_government_func', CASE WHEN broker.inherently_government_func IS DISTINCT FROM usaspending.inherently_government_func THEN jsonb_build_object('broker', broker.inherently_government_func, 'usaspending', usaspending.inherently_government_func) ELSE null END,
            'organizational_type', CASE WHEN broker.organizational_type IS DISTINCT FROM usaspending.organizational_type THEN jsonb_build_object('broker', broker.organizational_type, 'usaspending', usaspending.organizational_type) ELSE null END,
            'inherently_government_desc', CASE WHEN broker.inherently_government_desc IS DISTINCT FROM usaspending.inherently_government_desc THEN jsonb_build_object('broker', broker.inherently_government_desc, 'usaspending', usaspending.inherently_government_desc) ELSE null END,
            'unique_award_key', CASE WHEN broker.unique_award_key IS DISTINCT FROM usaspending.unique_award_key THEN jsonb_build_object('broker', broker.unique_award_key, 'usaspending', usaspending.unique_award_key) ELSE null END,
            'high_comp_officer1_amount', CASE WHEN broker.high_comp_officer1_amount IS DISTINCT FROM usaspending.officer_1_amount THEN jsonb_build_object('broker', broker.high_comp_officer1_amount, 'usaspending', usaspending.officer_1_amount) ELSE null END,
            'high_comp_officer1_full_na', CASE WHEN broker.high_comp_officer1_full_na IS DISTINCT FROM usaspending.officer_1_name THEN jsonb_build_object('broker', broker.high_comp_officer1_full_na, 'usaspending', usaspending.officer_1_name) ELSE null END,
            'high_comp_officer2_amount', CASE WHEN broker.high_comp_officer2_amount IS DISTINCT FROM usaspending.officer_2_amount THEN jsonb_build_object('broker', broker.high_comp_officer2_amount, 'usaspending', usaspending.officer_2_amount) ELSE null END,
            'high_comp_officer2_full_na', CASE WHEN broker.high_comp_officer2_full_na IS DISTINCT FROM usaspending.officer_2_name THEN jsonb_build_object('broker', broker.high_comp_officer2_full_na, 'usaspending', usaspending.officer_2_name) ELSE null END,
            'high_comp_officer3_amount', CASE WHEN broker.high_comp_officer3_amount IS DISTINCT FROM usaspending.officer_3_amount THEN jsonb_build_object('broker', broker.high_comp_officer3_amount, 'usaspending', usaspending.officer_3_amount) ELSE null END,
            'high_comp_officer3_full_na', CASE WHEN broker.high_comp_officer3_full_na IS DISTINCT FROM usaspending.officer_3_name THEN jsonb_build_object('broker', broker.high_comp_officer3_full_na, 'usaspending', usaspending.officer_3_name) ELSE null END,
            'high_comp_officer4_amount', CASE WHEN broker.high_comp_officer4_amount IS DISTINCT FROM usaspending.officer_4_amount THEN jsonb_build_object('broker', broker.high_comp_officer4_amount, 'usaspending', usaspending.officer_4_amount) ELSE null END,
            'high_comp_officer4_full_na', CASE WHEN broker.high_comp_officer4_full_na IS DISTINCT FROM usaspending.officer_4_name THEN jsonb_build_object('broker', broker.high_comp_officer4_full_na, 'usaspending', usaspending.officer_4_name) ELSE null END,
            'high_comp_officer5_amount', CASE WHEN broker.high_comp_officer5_amount IS DISTINCT FROM usaspending.officer_5_amount THEN jsonb_build_object('broker', broker.high_comp_officer5_amount, 'usaspending', usaspending.officer_5_amount) ELSE null END,
            'high_comp_officer5_full_na', CASE WHEN broker.high_comp_officer5_full_na IS DISTINCT FROM usaspending.officer_5_name THEN jsonb_build_object('broker', broker.high_comp_officer5_full_na, 'usaspending', usaspending.officer_5_name) ELSE null END
        )
    ) as fields_diff_json
FROM transaction_fpds AS usaspending
INNER JOIN transaction_normalized ON usaspending.transaction_id = transaction_normalized.id
INNER JOIN
(
    SELECT * FROM dblink (
        'broker_server',
        'SELECT
            created_at::TIMESTAMP WITHOUT TIME ZONE,
            updated_at::TIMESTAMP WITHOUT TIME ZONE,
            UPPER(piid) AS piid,
            detached_award_procurement_id,
            UPPER(detached_award_proc_unique) AS detached_award_proc_unique,
            UPPER(agency_id) AS agency_id,
            UPPER(awarding_sub_tier_agency_c) AS awarding_sub_tier_agency_c,
            UPPER(awarding_sub_tier_agency_n) AS awarding_sub_tier_agency_n,
            UPPER(awarding_agency_code) AS awarding_agency_code,
            UPPER(awarding_agency_name) AS awarding_agency_name,
            UPPER(parent_award_id) AS parent_award_id,
            UPPER(award_modification_amendme) AS award_modification_amendme,
            UPPER(type_of_contract_pricing) AS type_of_contract_pricing,
            UPPER(type_of_contract_pric_desc) AS type_of_contract_pric_desc,
            UPPER(contract_award_type) AS contract_award_type,
            UPPER(contract_award_type_desc) AS contract_award_type_desc,
            UPPER(naics) AS naics,
            UPPER(naics_description) AS naics_description,
            UPPER(awardee_or_recipient_uniqu) AS awardee_or_recipient_uniqu,
            UPPER(ultimate_parent_legal_enti) AS ultimate_parent_legal_enti,
            UPPER(ultimate_parent_unique_ide) AS ultimate_parent_unique_ide,
            UPPER(award_description) AS award_description,
            UPPER(REGEXP_REPLACE(place_of_performance_zip4a, E''\\s{{2,}}'', '' '')) AS place_of_performance_zip4a,
            UPPER(REGEXP_REPLACE(place_of_perform_city_name, E''\\s{{2,}}'', '' '')) AS place_of_perform_city_name,
            UPPER(REGEXP_REPLACE(place_of_perform_county_na, E''\\s{{2,}}'', '' '')) AS place_of_perform_county_na,
            UPPER(REGEXP_REPLACE(place_of_performance_congr, E''\\s{{2,}}'', '' '')) AS place_of_performance_congr,
            UPPER(awardee_or_recipient_legal) AS awardee_or_recipient_legal,
            UPPER(REGEXP_REPLACE(legal_entity_city_name, E''\\s{{2,}}'', '' '')) AS legal_entity_city_name,
            UPPER(REGEXP_REPLACE(legal_entity_state_code, E''\\s{{2,}}'', '' '')) AS legal_entity_state_code,
            UPPER(REGEXP_REPLACE(legal_entity_state_descrip, E''\\s{{2,}}'', '' '')) AS legal_entity_state_descrip,
            UPPER(REGEXP_REPLACE(legal_entity_zip4, E''\\s{{2,}}'', '' '')) AS legal_entity_zip4,
            UPPER(REGEXP_REPLACE(legal_entity_congressional, E''\\s{{2,}}'', '' '')) AS legal_entity_congressional,
            UPPER(REGEXP_REPLACE(legal_entity_address_line1, E''\\s{{2,}}'', '' '')) AS legal_entity_address_line1,
            UPPER(REGEXP_REPLACE(legal_entity_address_line2, E''\\s{{2,}}'', '' '')) AS legal_entity_address_line2,
            UPPER(REGEXP_REPLACE(legal_entity_address_line3, E''\\s{{2,}}'', '' '')) AS legal_entity_address_line3,
            UPPER(REGEXP_REPLACE(legal_entity_country_code, E''\\s{{2,}}'', '' '')) AS legal_entity_country_code,
            UPPER(REGEXP_REPLACE(legal_entity_country_name, E''\\s{{2,}}'', '' '')) AS legal_entity_country_name,
            UPPER(period_of_performance_star) AS period_of_performance_star,
            UPPER(period_of_performance_curr) AS period_of_performance_curr,
            UPPER(period_of_perf_potential_e) AS period_of_perf_potential_e,
            UPPER(ordering_period_end_date) AS ordering_period_end_date,
            UPPER(action_date) AS action_date,
            UPPER(action_type) AS action_type,
            UPPER(action_type_description) AS action_type_description,
            federal_action_obligation::numeric(23,2) AS federal_action_obligation,
            UPPER(current_total_value_award) AS current_total_value_award,
            UPPER(potential_total_value_awar) AS potential_total_value_awar,
            UPPER(funding_sub_tier_agency_co) AS funding_sub_tier_agency_co,
            UPPER(funding_sub_tier_agency_na) AS funding_sub_tier_agency_na,
            UPPER(funding_office_code) AS funding_office_code,
            UPPER(funding_office_name) AS funding_office_name,
            UPPER(awarding_office_code) AS awarding_office_code,
            UPPER(awarding_office_name) AS awarding_office_name,
            UPPER(referenced_idv_agency_iden) AS referenced_idv_agency_iden,
            UPPER(referenced_idv_agency_desc) AS referenced_idv_agency_desc,
            UPPER(funding_agency_code) AS funding_agency_code,
            UPPER(funding_agency_name) AS funding_agency_name,
            UPPER(REGEXP_REPLACE(place_of_performance_locat, E''\\s{{2,}}'', '' '')) AS place_of_performance_locat,
            UPPER(REGEXP_REPLACE(place_of_performance_state, E''\\s{{2,}}'', '' '')) AS place_of_performance_state,
            UPPER(REGEXP_REPLACE(place_of_perfor_state_desc, E''\\s{{2,}}'', '' '')) AS place_of_perfor_state_desc,
            UPPER(REGEXP_REPLACE(place_of_perform_country_c, E''\\s{{2,}}'', '' '')) AS place_of_perform_country_c,
            UPPER(REGEXP_REPLACE(place_of_perf_country_desc, E''\\s{{2,}}'', '' '')) AS place_of_perf_country_desc,
            UPPER(idv_type) AS idv_type,
            UPPER(idv_type_description) AS idv_type_description,
            UPPER(referenced_idv_type) AS referenced_idv_type,
            UPPER(referenced_idv_type_desc) AS referenced_idv_type_desc,
            UPPER(vendor_doing_as_business_n) AS vendor_doing_as_business_n,
            UPPER(vendor_phone_number) AS vendor_phone_number,
            UPPER(vendor_fax_number) AS vendor_fax_number,
            UPPER(multiple_or_single_award_i) AS multiple_or_single_award_i,
            UPPER(multiple_or_single_aw_desc) AS multiple_or_single_aw_desc,
            UPPER(referenced_mult_or_single) AS referenced_mult_or_single,
            UPPER(referenced_mult_or_si_desc) AS referenced_mult_or_si_desc,
            UPPER(type_of_idc) AS type_of_idc,
            UPPER(type_of_idc_description) AS type_of_idc_description,
            UPPER(a_76_fair_act_action) AS a_76_fair_act_action,
            UPPER(a_76_fair_act_action_desc) AS a_76_fair_act_action_desc,
            UPPER(dod_claimant_program_code) AS dod_claimant_program_code,
            UPPER(dod_claimant_prog_cod_desc) AS dod_claimant_prog_cod_desc,
            UPPER(clinger_cohen_act_planning) AS clinger_cohen_act_planning,
            UPPER(clinger_cohen_act_pla_desc) AS clinger_cohen_act_pla_desc,
            UPPER(commercial_item_acquisitio) AS commercial_item_acquisitio,
            UPPER(commercial_item_acqui_desc) AS commercial_item_acqui_desc,
            UPPER(commercial_item_test_progr) AS commercial_item_test_progr,
            UPPER(commercial_item_test_desc) AS commercial_item_test_desc,
            UPPER(consolidated_contract) AS consolidated_contract,
            UPPER(consolidated_contract_desc) AS consolidated_contract_desc,
            UPPER(contingency_humanitarian_o) AS contingency_humanitarian_o,
            UPPER(contingency_humanitar_desc) AS contingency_humanitar_desc,
            UPPER(contract_bundling) AS contract_bundling,
            UPPER(contract_bundling_descrip) AS contract_bundling_descrip,
            UPPER(contract_financing) AS contract_financing,
            UPPER(contract_financing_descrip) AS contract_financing_descrip,
            UPPER(contracting_officers_deter) AS contracting_officers_deter,
            UPPER(contracting_officers_desc) AS contracting_officers_desc,
            UPPER(cost_accounting_standards) AS cost_accounting_standards,
            UPPER(cost_accounting_stand_desc) AS cost_accounting_stand_desc,
            UPPER(cost_or_pricing_data) AS cost_or_pricing_data,
            UPPER(cost_or_pricing_data_desc) AS cost_or_pricing_data_desc,
            UPPER(country_of_product_or_serv) AS country_of_product_or_serv,
            UPPER(country_of_product_or_desc) AS country_of_product_or_desc,
            UPPER(construction_wage_rate_req) AS construction_wage_rate_req,
            UPPER(construction_wage_rat_desc) AS construction_wage_rat_desc,
            UPPER(evaluated_preference) AS evaluated_preference,
            UPPER(evaluated_preference_desc) AS evaluated_preference_desc,
            UPPER(extent_competed) AS extent_competed,
            UPPER(extent_compete_description) AS extent_compete_description,
            UPPER(fed_biz_opps) AS fed_biz_opps,
            UPPER(fed_biz_opps_description) AS fed_biz_opps_description,
            UPPER(foreign_funding) AS foreign_funding,
            UPPER(foreign_funding_desc) AS foreign_funding_desc,
            UPPER(government_furnished_prope) AS government_furnished_prope,
            UPPER(government_furnished_desc) AS government_furnished_desc,
            UPPER(information_technology_com) AS information_technology_com,
            UPPER(information_technolog_desc) AS information_technolog_desc,
            UPPER(interagency_contracting_au) AS interagency_contracting_au,
            UPPER(interagency_contract_desc) AS interagency_contract_desc,
            UPPER(local_area_set_aside) AS local_area_set_aside,
            UPPER(local_area_set_aside_desc) AS local_area_set_aside_desc,
            UPPER(major_program) AS major_program,
            UPPER(purchase_card_as_payment_m) AS purchase_card_as_payment_m,
            UPPER(purchase_card_as_paym_desc) AS purchase_card_as_paym_desc,
            UPPER(multi_year_contract) AS multi_year_contract,
            UPPER(multi_year_contract_desc) AS multi_year_contract_desc,
            UPPER(national_interest_action) AS national_interest_action,
            UPPER(national_interest_desc) AS national_interest_desc,
            UPPER(number_of_actions) AS number_of_actions,
            UPPER(number_of_offers_received) AS number_of_offers_received,
            UPPER(other_statutory_authority) AS other_statutory_authority,
            UPPER(performance_based_service) AS performance_based_service,
            UPPER(performance_based_se_desc) AS performance_based_se_desc,
            UPPER(place_of_manufacture) AS place_of_manufacture,
            UPPER(place_of_manufacture_desc) AS place_of_manufacture_desc,
            UPPER(price_evaluation_adjustmen) AS price_evaluation_adjustmen,
            UPPER(product_or_service_code) AS product_or_service_code,
            UPPER(product_or_service_co_desc) AS product_or_service_co_desc,
            UPPER(program_acronym) AS program_acronym,
            UPPER(other_than_full_and_open_c) AS other_than_full_and_open_c,
            UPPER(other_than_full_and_o_desc) AS other_than_full_and_o_desc,
            UPPER(recovered_materials_sustai) AS recovered_materials_sustai,
            UPPER(recovered_materials_s_desc) AS recovered_materials_s_desc,
            UPPER(research) AS research,
            UPPER(research_description) AS research_description,
            UPPER(sea_transportation) AS sea_transportation,
            UPPER(sea_transportation_desc) AS sea_transportation_desc,
            UPPER(labor_standards) AS labor_standards,
            UPPER(labor_standards_descrip) AS labor_standards_descrip,
            COALESCE(small_business_competitive::boolean, False) AS small_business_competitive,
            UPPER(solicitation_identifier) AS solicitation_identifier,
            UPPER(solicitation_procedures) AS solicitation_procedures,
            UPPER(solicitation_procedur_desc) AS solicitation_procedur_desc,
            UPPER(fair_opportunity_limited_s) AS fair_opportunity_limited_s,
            UPPER(fair_opportunity_limi_desc) AS fair_opportunity_limi_desc,
            UPPER(subcontracting_plan) AS subcontracting_plan,
            UPPER(subcontracting_plan_desc) AS subcontracting_plan_desc,
            UPPER(program_system_or_equipmen) AS program_system_or_equipmen,
            UPPER(program_system_or_equ_desc) AS program_system_or_equ_desc,
            UPPER(type_set_aside) AS type_set_aside,
            UPPER(type_set_aside_description) AS type_set_aside_description,
            UPPER(epa_designated_product) AS epa_designated_product,
            UPPER(epa_designated_produc_desc) AS epa_designated_produc_desc,
            UPPER(materials_supplies_article) AS materials_supplies_article,
            UPPER(materials_supplies_descrip) AS materials_supplies_descrip,
            UPPER(transaction_number) AS transaction_number,
            UPPER(sam_exception) AS sam_exception,
            UPPER(sam_exception_description) AS sam_exception_description,
            COALESCE(city_local_government::boolean, False) AS city_local_government,
            COALESCE(county_local_government::boolean, False) AS county_local_government,
            COALESCE(inter_municipal_local_gove::boolean, False) AS inter_municipal_local_gove,
            COALESCE(local_government_owned::boolean, False) AS local_government_owned,
            COALESCE(municipality_local_governm::boolean, False) AS municipality_local_governm,
            COALESCE(school_district_local_gove::boolean, False) AS school_district_local_gove,
            COALESCE(township_local_government::boolean, False) AS township_local_government,
            COALESCE(us_state_government::boolean, False) AS us_state_government,
            COALESCE(us_federal_government::boolean, False) AS us_federal_government,
            COALESCE(federal_agency::boolean, False) AS federal_agency,
            COALESCE(federally_funded_research::boolean, False) AS federally_funded_research,
            COALESCE(us_tribal_government::boolean, False) AS us_tribal_government,
            COALESCE(foreign_government::boolean, False) AS foreign_government,
            COALESCE(community_developed_corpor::boolean, False) AS community_developed_corpor,
            COALESCE(labor_surplus_area_firm::boolean, False) AS labor_surplus_area_firm,
            COALESCE(corporate_entity_not_tax_e::boolean, False) AS corporate_entity_not_tax_e,
            COALESCE(corporate_entity_tax_exemp::boolean, False) AS corporate_entity_tax_exemp,
            COALESCE(partnership_or_limited_lia::boolean, False) AS partnership_or_limited_lia,
            COALESCE(sole_proprietorship::boolean, False) AS sole_proprietorship,
            COALESCE(small_agricultural_coopera::boolean, False) AS small_agricultural_coopera,
            COALESCE(international_organization::boolean, False) AS international_organization,
            COALESCE(us_government_entity::boolean, False) AS us_government_entity,
            COALESCE(emerging_small_business::boolean, False) AS emerging_small_business,
            COALESCE(c8a_program_participant::boolean, False) AS c8a_program_participant,
            COALESCE(sba_certified_8_a_joint_ve::boolean, False) AS sba_certified_8_a_joint_ve,
            COALESCE(dot_certified_disadvantage::boolean, False) AS dot_certified_disadvantage,
            COALESCE(self_certified_small_disad::boolean, False) AS self_certified_small_disad,
            COALESCE(historically_underutilized::boolean, False) AS historically_underutilized,
            COALESCE(small_disadvantaged_busine::boolean, False) AS small_disadvantaged_busine,
            COALESCE(the_ability_one_program::boolean, False) AS the_ability_one_program,
            COALESCE(historically_black_college::boolean, False) AS historically_black_college,
            COALESCE(c1862_land_grant_college::boolean, False) AS c1862_land_grant_college,
            COALESCE(c1890_land_grant_college::boolean, False) AS c1890_land_grant_college,
            COALESCE(c1994_land_grant_college::boolean, False) AS c1994_land_grant_college,
            COALESCE(minority_institution::boolean, False) AS minority_institution,
            COALESCE(private_university_or_coll::boolean, False) AS private_university_or_coll,
            COALESCE(school_of_forestry::boolean, False) AS school_of_forestry,
            COALESCE(state_controlled_instituti::boolean, False) AS state_controlled_instituti,
            COALESCE(tribal_college::boolean, False) AS tribal_college,
            COALESCE(veterinary_college::boolean, False) AS veterinary_college,
            COALESCE(educational_institution::boolean, False) AS educational_institution,
            COALESCE(alaskan_native_servicing_i::boolean, False) AS alaskan_native_servicing_i,
            COALESCE(community_development_corp::boolean, False) AS community_development_corp,
            COALESCE(native_hawaiian_servicing::boolean, False) AS native_hawaiian_servicing,
            COALESCE(domestic_shelter::boolean, False) AS domestic_shelter,
            COALESCE(manufacturer_of_goods::boolean, False) AS manufacturer_of_goods,
            COALESCE(hospital_flag::boolean, False) AS hospital_flag,
            COALESCE(veterinary_hospital::boolean, False) AS veterinary_hospital,
            COALESCE(hispanic_servicing_institu::boolean, False) AS hispanic_servicing_institu,
            COALESCE(foundation::boolean, False) AS foundation,
            COALESCE(woman_owned_business::boolean, False) AS woman_owned_business,
            COALESCE(minority_owned_business::boolean, False) AS minority_owned_business,
            COALESCE(women_owned_small_business::boolean, False) AS women_owned_small_business,
            COALESCE(economically_disadvantaged::boolean, False) AS economically_disadvantaged,
            COALESCE(joint_venture_women_owned::boolean, False) AS joint_venture_women_owned,
            COALESCE(joint_venture_economically::boolean, False) AS joint_venture_economically,
            COALESCE(veteran_owned_business::boolean, False) AS veteran_owned_business,
            COALESCE(service_disabled_veteran_o::boolean, False) AS service_disabled_veteran_o,
            COALESCE(contracts::boolean, False) AS contracts,
            COALESCE(grants::boolean, False) AS grants,
            COALESCE(receives_contracts_and_gra::boolean, False) AS receives_contracts_and_gra,
            COALESCE(airport_authority::boolean, False) AS airport_authority,
            COALESCE(council_of_governments::boolean, False) AS council_of_governments,
            COALESCE(housing_authorities_public::boolean, False) AS housing_authorities_public,
            COALESCE(interstate_entity::boolean, False) AS interstate_entity,
            COALESCE(planning_commission::boolean, False) AS planning_commission,
            COALESCE(port_authority::boolean, False) AS port_authority,
            COALESCE(transit_authority::boolean, False) AS transit_authority,
            COALESCE(subchapter_s_corporation::boolean, False) AS subchapter_s_corporation,
            COALESCE(limited_liability_corporat::boolean, False) AS limited_liability_corporat,
            COALESCE(foreign_owned_and_located::boolean, False) AS foreign_owned_and_located,
            COALESCE(american_indian_owned_busi::boolean, False) AS american_indian_owned_busi,
            COALESCE(alaskan_native_owned_corpo::boolean, False) AS alaskan_native_owned_corpo,
            COALESCE(indian_tribe_federally_rec::boolean, False) AS indian_tribe_federally_rec,
            COALESCE(native_hawaiian_owned_busi::boolean, False) AS native_hawaiian_owned_busi,
            COALESCE(tribally_owned_business::boolean, False) AS tribally_owned_business,
            COALESCE(asian_pacific_american_own::boolean, False) AS asian_pacific_american_own,
            COALESCE(black_american_owned_busin::boolean, False) AS black_american_owned_busin,
            COALESCE(hispanic_american_owned_bu::boolean, False) AS hispanic_american_owned_bu,
            COALESCE(native_american_owned_busi::boolean, False) AS native_american_owned_busi,
            COALESCE(subcontinent_asian_asian_i::boolean, False) AS subcontinent_asian_asian_i,
            COALESCE(other_minority_owned_busin::boolean, False) AS other_minority_owned_busin,
            COALESCE(for_profit_organization::boolean, False) AS for_profit_organization,
            COALESCE(nonprofit_organization::boolean, False) AS nonprofit_organization,
            COALESCE(other_not_for_profit_organ::boolean, False) AS other_not_for_profit_organ,
            COALESCE(us_local_government::boolean, False) AS us_local_government,
            referenced_idv_modificatio AS referenced_idv_modificatio,
            UPPER(undefinitized_action) AS undefinitized_action,
            UPPER(undefinitized_action_desc) AS undefinitized_action_desc,
            UPPER(domestic_or_foreign_entity) AS domestic_or_foreign_entity,
            UPPER(domestic_or_foreign_e_desc) AS domestic_or_foreign_e_desc,
            UPPER(pulled_from) AS pulled_from,
            UPPER(last_modified) AS last_modified,
            UPPER(annual_revenue) AS annual_revenue,
            UPPER(division_name) AS division_name,
            UPPER(division_number_or_office) AS division_number_or_office,
            UPPER(number_of_employees) AS number_of_employees,
            UPPER(vendor_alternate_name) AS vendor_alternate_name,
            UPPER(vendor_alternate_site_code) AS vendor_alternate_site_code,
            UPPER(vendor_enabled) AS vendor_enabled,
            UPPER(vendor_legal_org_name) AS vendor_legal_org_name,
            UPPER(vendor_location_disabled_f) AS vendor_location_disabled_f,
            UPPER(vendor_site_code) AS vendor_site_code,
            UPPER(initial_report_date) AS initial_report_date,
            UPPER(base_and_all_options_value) AS base_and_all_options_value,
            UPPER(base_exercised_options_val) AS base_exercised_options_val,
            UPPER(total_obligated_amount) AS total_obligated_amount,
            UPPER(place_of_perform_country_n) AS place_of_perform_country_n,
            UPPER(place_of_perform_state_nam) AS place_of_perform_state_nam,
            UPPER(referenced_idv_agency_name) AS referenced_idv_agency_name,
            UPPER(award_or_idv_flag) AS award_or_idv_flag,
            UPPER(REGEXP_REPLACE(legal_entity_county_code, E''\\s{{2,}}'', '' '')) AS legal_entity_county_code,
            UPPER(REGEXP_REPLACE(legal_entity_county_name, E''\\s{{2,}}'', '' '')) AS legal_entity_county_name,
            UPPER(REGEXP_REPLACE(legal_entity_zip5, E''\\s{{2,}}'', '' '')) AS legal_entity_zip5,
            UPPER(REGEXP_REPLACE(legal_entity_zip_last4, E''\\s{{2,}}'', '' '')) AS legal_entity_zip_last4,
            UPPER(REGEXP_REPLACE(place_of_perform_county_co, E''\\s{{2,}}'', '' '')) AS place_of_perform_county_co,
            UPPER(REGEXP_REPLACE(place_of_performance_zip5, E''\\s{{2,}}'', '' '')) AS place_of_performance_zip5,
            UPPER(REGEXP_REPLACE(place_of_perform_zip_last4, E''\\s{{2,}}'', '' '')) AS place_of_perform_zip_last4,
            UPPER(cage_code) AS cage_code,
            UPPER(inherently_government_func) AS inherently_government_func,
            UPPER(organizational_type) AS organizational_type,
            UPPER(inherently_government_desc) AS inherently_government_desc,
            UPPER(unique_award_key) AS unique_award_key,
            UPPER(additional_reporting) AS additional_reporting,
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
        FROM detached_award_procurement
        WHERE detached_award_procurement_id BETWEEN {minid} AND {maxid}'
    ) AS broker (
        created_at TIMESTAMP WITHOUT time ZONE,
        updated_at TIMESTAMP WITHOUT time ZONE,
        piid text,
        detached_award_procurement_id int,
        detached_award_proc_unique text,
        agency_id text,
        awarding_sub_tier_agency_c text,
        awarding_sub_tier_agency_n text,
        awarding_agency_code text,
        awarding_agency_name text,
        parent_award_id text,
        award_modification_amendme text,
        type_of_contract_pricing text,
        type_of_contract_pric_desc text,
        contract_award_type text,
        contract_award_type_desc text,
        naics text,
        naics_description text,
        awardee_or_recipient_uniqu text,
        ultimate_parent_legal_enti text,
        ultimate_parent_unique_ide text,
        award_description text,
        place_of_performance_zip4a text,
        place_of_perform_city_name text,
        place_of_perform_county_na text,
        place_of_performance_congr text,
        awardee_or_recipient_legal text,
        legal_entity_city_name text,
        legal_entity_state_code text,
        legal_entity_state_descrip text,
        legal_entity_zip4 text,
        legal_entity_congressional text,
        legal_entity_address_line1 text,
        legal_entity_address_line2 text,
        legal_entity_address_line3 text,
        legal_entity_country_code text,
        legal_entity_country_name text,
        period_of_performance_star text,
        period_of_performance_curr text,
        period_of_perf_potential_e text,
        ordering_period_end_date text,
        action_date text,
        action_type text,
        action_type_description text,
        federal_action_obligation numeric(23,2),
        current_total_value_award text,
        potential_total_value_awar text,
        funding_sub_tier_agency_co text,
        funding_sub_tier_agency_na text,
        funding_office_code text,
        funding_office_name text,
        awarding_office_code text,
        awarding_office_name text,
        referenced_idv_agency_iden text,
        referenced_idv_agency_desc text,
        funding_agency_code text,
        funding_agency_name text,
        place_of_performance_locat text,
        place_of_performance_state text,
        place_of_perfor_state_desc text,
        place_of_perform_country_c text,
        place_of_perf_country_desc text,
        idv_type text,
        idv_type_description text,
        referenced_idv_type text,
        referenced_idv_type_desc text,
        vendor_doing_as_business_n text,
        vendor_phone_number text,
        vendor_fax_number text,
        multiple_or_single_award_i text,
        multiple_or_single_aw_desc text,
        referenced_mult_or_single text,
        referenced_mult_or_si_desc text,
        type_of_idc text,
        type_of_idc_description text,
        a_76_fair_act_action text,
        a_76_fair_act_action_desc text,
        dod_claimant_program_code text,
        dod_claimant_prog_cod_desc text,
        clinger_cohen_act_planning text,
        clinger_cohen_act_pla_desc text,
        commercial_item_acquisitio text,
        commercial_item_acqui_desc text,
        commercial_item_test_progr text,
        commercial_item_test_desc text,
        consolidated_contract text,
        consolidated_contract_desc text,
        contingency_humanitarian_o text,
        contingency_humanitar_desc text,
        contract_bundling text,
        contract_bundling_descrip text,
        contract_financing text,
        contract_financing_descrip text,
        contracting_officers_deter text,
        contracting_officers_desc text,
        cost_accounting_standards text,
        cost_accounting_stand_desc text,
        cost_or_pricing_data text,
        cost_or_pricing_data_desc text,
        country_of_product_or_serv text,
        country_of_product_or_desc text,
        construction_wage_rate_req text,
        construction_wage_rat_desc text,
        evaluated_preference text,
        evaluated_preference_desc text,
        extent_competed text,
        extent_compete_description text,
        fed_biz_opps text,
        fed_biz_opps_description text,
        foreign_funding text,
        foreign_funding_desc text,
        government_furnished_prope text,
        government_furnished_desc text,
        information_technology_com text,
        information_technolog_desc text,
        interagency_contracting_au text,
        interagency_contract_desc text,
        local_area_set_aside text,
        local_area_set_aside_desc text,
        major_program text,
        purchase_card_as_payment_m text,
        purchase_card_as_paym_desc text,
        multi_year_contract text,
        multi_year_contract_desc text,
        national_interest_action text,
        national_interest_desc text,
        number_of_actions text,
        number_of_offers_received text,
        other_statutory_authority text,
        performance_based_service text,
        performance_based_se_desc text,
        place_of_manufacture text,
        place_of_manufacture_desc text,
        price_evaluation_adjustmen text,
        product_or_service_code text,
        product_or_service_co_desc text,
        program_acronym text,
        other_than_full_and_open_c text,
        other_than_full_and_o_desc text,
        recovered_materials_sustai text,
        recovered_materials_s_desc text,
        research text,
        research_description text,
        sea_transportation text,
        sea_transportation_desc text,
        labor_standards text,
        labor_standards_descrip text,
        small_business_competitive boolean,
        solicitation_identifier text,
        solicitation_procedures text,
        solicitation_procedur_desc text,
        fair_opportunity_limited_s text,
        fair_opportunity_limi_desc text,
        subcontracting_plan text,
        subcontracting_plan_desc text,
        program_system_or_equipmen text,
        program_system_or_equ_desc text,
        type_set_aside text,
        type_set_aside_description text,
        epa_designated_product text,
        epa_designated_produc_desc text,
        materials_supplies_article text,
        materials_supplies_descrip text,
        transaction_number text,
        sam_exception text,
        sam_exception_description text,
        city_local_government boolean,
        county_local_government boolean,
        inter_municipal_local_gove boolean,
        local_government_owned boolean,
        municipality_local_governm boolean,
        school_district_local_gove boolean,
        township_local_government boolean,
        us_state_government boolean,
        us_federal_government boolean,
        federal_agency boolean,
        federally_funded_research boolean,
        us_tribal_government boolean,
        foreign_government boolean,
        community_developed_corpor boolean,
        labor_surplus_area_firm boolean,
        corporate_entity_not_tax_e boolean,
        corporate_entity_tax_exemp boolean,
        partnership_or_limited_lia boolean,
        sole_proprietorship boolean,
        small_agricultural_coopera boolean,
        international_organization boolean,
        us_government_entity boolean,
        emerging_small_business boolean,
        c8a_program_participant boolean,
        sba_certified_8_a_joint_ve boolean,
        dot_certified_disadvantage boolean,
        self_certified_small_disad boolean,
        historically_underutilized boolean,
        small_disadvantaged_busine boolean,
        the_ability_one_program boolean,
        historically_black_college boolean,
        c1862_land_grant_college boolean,
        c1890_land_grant_college boolean,
        c1994_land_grant_college boolean,
        minority_institution boolean,
        private_university_or_coll boolean,
        school_of_forestry boolean,
        state_controlled_instituti boolean,
        tribal_college boolean,
        veterinary_college boolean,
        educational_institution boolean,
        alaskan_native_servicing_i boolean,
        community_development_corp boolean,
        native_hawaiian_servicing boolean,
        domestic_shelter boolean,
        manufacturer_of_goods boolean,
        hospital_flag boolean,
        veterinary_hospital boolean,
        hispanic_servicing_institu boolean,
        foundation boolean,
        woman_owned_business boolean,
        minority_owned_business boolean,
        women_owned_small_business boolean,
        economically_disadvantaged boolean,
        joint_venture_women_owned boolean,
        joint_venture_economically boolean,
        veteran_owned_business boolean,
        service_disabled_veteran_o boolean,
        contracts boolean,
        grants boolean,
        receives_contracts_and_gra boolean,
        airport_authority boolean,
        council_of_governments boolean,
        housing_authorities_public boolean,
        interstate_entity boolean,
        planning_commission boolean,
        port_authority boolean,
        transit_authority boolean,
        subchapter_s_corporation boolean,
        limited_liability_corporat boolean,
        foreign_owned_and_located boolean,
        american_indian_owned_busi boolean,
        alaskan_native_owned_corpo boolean,
        indian_tribe_federally_rec boolean,
        native_hawaiian_owned_busi boolean,
        tribally_owned_business boolean,
        asian_pacific_american_own boolean,
        black_american_owned_busin boolean,
        hispanic_american_owned_bu boolean,
        native_american_owned_busi boolean,
        subcontinent_asian_asian_i boolean,
        other_minority_owned_busin boolean,
        for_profit_organization boolean,
        nonprofit_organization boolean,
        other_not_for_profit_organ boolean,
        us_local_government boolean,
        referenced_idv_modificatio text,
        undefinitized_action text,
        undefinitized_action_desc text,
        domestic_or_foreign_entity text,
        domestic_or_foreign_e_desc text,
        pulled_from text,
        last_modified text,
        annual_revenue text,
        division_name text,
        division_number_or_office text,
        number_of_employees text,
        vendor_alternate_name text,
        vendor_alternate_site_code text,
        vendor_enabled text,
        vendor_legal_org_name text,
        vendor_location_disabled_f text,
        vendor_site_code text,
        initial_report_date text,
        base_and_all_options_value text,
        base_exercised_options_val text,
        total_obligated_amount text,
        place_of_perform_country_n text,
        place_of_perform_state_nam text,
        referenced_idv_agency_name text,
        award_or_idv_flag text,
        legal_entity_county_code text,
        legal_entity_county_name text,
        legal_entity_zip5 text,
        legal_entity_zip_last4 text,
        place_of_perform_county_co text,
        place_of_performance_zip5 text,
        place_of_perform_zip_last4 text,
        cage_code text,
        inherently_government_func text,
        organizational_type text,
        inherently_government_desc text,
        unique_award_key text,
        additional_reporting text,
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
    (broker.detached_award_procurement_id = usaspending.detached_award_procurement_id)
    AND (
        (broker.created_at IS DISTINCT FROM usaspending.created_at::TIMESTAMP WITHOUT TIME ZONE)
        OR (broker.updated_at IS DISTINCT FROM usaspending.updated_at::TIMESTAMP WITHOUT TIME ZONE)
        OR (broker.piid IS DISTINCT FROM usaspending.piid)
        OR (broker.detached_award_proc_unique IS DISTINCT FROM usaspending.detached_award_proc_unique)
        OR (broker.agency_id IS DISTINCT FROM usaspending.agency_id)
        OR (broker.awarding_sub_tier_agency_c IS DISTINCT FROM usaspending.awarding_sub_tier_agency_c)
        OR (broker.awarding_sub_tier_agency_n IS DISTINCT FROM usaspending.awarding_sub_tier_agency_n)
        OR (broker.awarding_agency_code IS DISTINCT FROM usaspending.awarding_agency_code)
        OR (broker.awarding_agency_name IS DISTINCT FROM usaspending.awarding_agency_name)
        OR (broker.parent_award_id IS DISTINCT FROM usaspending.parent_award_id)
        OR (broker.award_modification_amendme IS DISTINCT FROM usaspending.award_modification_amendme)
        OR (broker.type_of_contract_pricing IS DISTINCT FROM usaspending.type_of_contract_pricing)
        OR (broker.type_of_contract_pric_desc IS DISTINCT FROM usaspending.type_of_contract_pric_desc)
        OR (broker.contract_award_type IS DISTINCT FROM usaspending.contract_award_type)
        OR (broker.contract_award_type_desc IS DISTINCT FROM usaspending.contract_award_type_desc)
        OR (broker.naics IS DISTINCT FROM usaspending.naics)
        OR (broker.naics_description IS DISTINCT FROM usaspending.naics_description)
        OR (broker.awardee_or_recipient_uniqu IS DISTINCT FROM usaspending.awardee_or_recipient_uniqu)
        OR (broker.ultimate_parent_legal_enti IS DISTINCT FROM usaspending.ultimate_parent_legal_enti)
        OR (broker.ultimate_parent_unique_ide IS DISTINCT FROM usaspending.ultimate_parent_unique_ide)
        OR (broker.award_description IS DISTINCT FROM usaspending.award_description)
        OR (broker.place_of_performance_zip4a IS DISTINCT FROM usaspending.place_of_performance_zip4a)
        OR (broker.place_of_perform_city_name IS DISTINCT FROM usaspending.place_of_perform_city_name)
        OR (broker.place_of_perform_county_na IS DISTINCT FROM usaspending.place_of_perform_county_na)
        OR (broker.place_of_performance_congr IS DISTINCT FROM usaspending.place_of_performance_congr)
        OR (broker.awardee_or_recipient_legal IS DISTINCT FROM usaspending.awardee_or_recipient_legal)
        OR (broker.legal_entity_city_name IS DISTINCT FROM usaspending.legal_entity_city_name)
        OR (broker.legal_entity_state_code IS DISTINCT FROM usaspending.legal_entity_state_code)
        OR (broker.legal_entity_state_descrip IS DISTINCT FROM usaspending.legal_entity_state_descrip)
        OR (broker.legal_entity_zip4 IS DISTINCT FROM usaspending.legal_entity_zip4)
        OR (broker.legal_entity_congressional IS DISTINCT FROM usaspending.legal_entity_congressional)
        OR (broker.legal_entity_address_line1 IS DISTINCT FROM usaspending.legal_entity_address_line1)
        OR (broker.legal_entity_address_line2 IS DISTINCT FROM usaspending.legal_entity_address_line2)
        OR (broker.legal_entity_address_line3 IS DISTINCT FROM usaspending.legal_entity_address_line3)
        OR (broker.legal_entity_country_code IS DISTINCT FROM usaspending.legal_entity_country_code)
        OR (broker.legal_entity_country_name IS DISTINCT FROM usaspending.legal_entity_country_name)
        OR (broker.period_of_performance_star IS DISTINCT FROM usaspending.period_of_performance_star)
        OR (broker.period_of_performance_curr IS DISTINCT FROM usaspending.period_of_performance_curr)
        OR (broker.period_of_perf_potential_e IS DISTINCT FROM usaspending.period_of_perf_potential_e)
        OR (broker.ordering_period_end_date IS DISTINCT FROM usaspending.ordering_period_end_date)
        OR (broker.action_date IS DISTINCT FROM usaspending.action_date)
        OR (broker.action_type IS DISTINCT FROM usaspending.action_type)
        OR (broker.action_type_description IS DISTINCT FROM usaspending.action_type_description)
        OR (broker.federal_action_obligation IS DISTINCT FROM usaspending.federal_action_obligation)
        OR (broker.current_total_value_award IS DISTINCT FROM usaspending.current_total_value_award)
        OR (broker.potential_total_value_awar IS DISTINCT FROM usaspending.potential_total_value_awar)
        OR (broker.funding_sub_tier_agency_co IS DISTINCT FROM usaspending.funding_sub_tier_agency_co)
        OR (broker.funding_sub_tier_agency_na IS DISTINCT FROM usaspending.funding_sub_tier_agency_na)
        OR (broker.funding_office_code IS DISTINCT FROM usaspending.funding_office_code)
        OR (broker.funding_office_name IS DISTINCT FROM usaspending.funding_office_name)
        OR (broker.awarding_office_code IS DISTINCT FROM usaspending.awarding_office_code)
        OR (broker.awarding_office_name IS DISTINCT FROM usaspending.awarding_office_name)
        OR (broker.referenced_idv_agency_iden IS DISTINCT FROM usaspending.referenced_idv_agency_iden)
        OR (broker.referenced_idv_agency_desc IS DISTINCT FROM usaspending.referenced_idv_agency_desc)
        OR (broker.funding_agency_code IS DISTINCT FROM usaspending.funding_agency_code)
        OR (broker.funding_agency_name IS DISTINCT FROM usaspending.funding_agency_name)
        OR (broker.place_of_performance_locat IS DISTINCT FROM usaspending.place_of_performance_locat)
        OR (broker.place_of_performance_state IS DISTINCT FROM usaspending.place_of_performance_state)
        OR (broker.place_of_perfor_state_desc IS DISTINCT FROM usaspending.place_of_perfor_state_desc)
        OR (broker.place_of_perform_country_c IS DISTINCT FROM usaspending.place_of_perform_country_c)
        OR (broker.place_of_perf_country_desc IS DISTINCT FROM usaspending.place_of_perf_country_desc)
        OR (broker.idv_type IS DISTINCT FROM usaspending.idv_type)
        OR (broker.idv_type_description IS DISTINCT FROM usaspending.idv_type_description)
        OR (broker.referenced_idv_type IS DISTINCT FROM usaspending.referenced_idv_type)
        OR (broker.referenced_idv_type_desc IS DISTINCT FROM usaspending.referenced_idv_type_desc)
        OR (broker.vendor_doing_as_business_n IS DISTINCT FROM usaspending.vendor_doing_as_business_n)
        OR (broker.vendor_phone_number IS DISTINCT FROM usaspending.vendor_phone_number)
        OR (broker.vendor_fax_number IS DISTINCT FROM usaspending.vendor_fax_number)
        OR (broker.multiple_or_single_award_i IS DISTINCT FROM usaspending.multiple_or_single_award_i)
        OR (broker.multiple_or_single_aw_desc IS DISTINCT FROM usaspending.multiple_or_single_aw_desc)
        OR (broker.referenced_mult_or_single IS DISTINCT FROM usaspending.referenced_mult_or_single)
        OR (broker.referenced_mult_or_si_desc IS DISTINCT FROM usaspending.referenced_mult_or_si_desc)
        OR (broker.type_of_idc IS DISTINCT FROM usaspending.type_of_idc)
        OR (broker.type_of_idc_description IS DISTINCT FROM usaspending.type_of_idc_description)
        OR (broker.a_76_fair_act_action IS DISTINCT FROM usaspending.a_76_fair_act_action)
        OR (broker.a_76_fair_act_action_desc IS DISTINCT FROM usaspending.a_76_fair_act_action_desc)
        OR (broker.dod_claimant_program_code IS DISTINCT FROM usaspending.dod_claimant_program_code)
        OR (broker.dod_claimant_prog_cod_desc IS DISTINCT FROM usaspending.dod_claimant_prog_cod_desc)
        OR (broker.clinger_cohen_act_planning IS DISTINCT FROM usaspending.clinger_cohen_act_planning)
        OR (broker.clinger_cohen_act_pla_desc IS DISTINCT FROM usaspending.clinger_cohen_act_pla_desc)
        OR (broker.commercial_item_acquisitio IS DISTINCT FROM usaspending.commercial_item_acquisitio)
        OR (broker.commercial_item_acqui_desc IS DISTINCT FROM usaspending.commercial_item_acqui_desc)
        OR (broker.commercial_item_test_progr IS DISTINCT FROM usaspending.commercial_item_test_progr)
        OR (broker.commercial_item_test_desc IS DISTINCT FROM usaspending.commercial_item_test_desc)
        OR (broker.consolidated_contract IS DISTINCT FROM usaspending.consolidated_contract)
        OR (broker.consolidated_contract_desc IS DISTINCT FROM usaspending.consolidated_contract_desc)
        OR (broker.contingency_humanitarian_o IS DISTINCT FROM usaspending.contingency_humanitarian_o)
        OR (broker.contingency_humanitar_desc IS DISTINCT FROM usaspending.contingency_humanitar_desc)
        OR (broker.contract_bundling IS DISTINCT FROM usaspending.contract_bundling)
        OR (broker.contract_bundling_descrip IS DISTINCT FROM usaspending.contract_bundling_descrip)
        OR (broker.contract_financing IS DISTINCT FROM usaspending.contract_financing)
        OR (broker.contract_financing_descrip IS DISTINCT FROM usaspending.contract_financing_descrip)
        OR (broker.contracting_officers_deter IS DISTINCT FROM usaspending.contracting_officers_deter)
        OR (broker.contracting_officers_desc IS DISTINCT FROM usaspending.contracting_officers_desc)
        OR (broker.cost_accounting_standards IS DISTINCT FROM usaspending.cost_accounting_standards)
        OR (broker.cost_accounting_stand_desc IS DISTINCT FROM usaspending.cost_accounting_stand_desc)
        OR (broker.cost_or_pricing_data IS DISTINCT FROM usaspending.cost_or_pricing_data)
        OR (broker.cost_or_pricing_data_desc IS DISTINCT FROM usaspending.cost_or_pricing_data_desc)
        OR (broker.country_of_product_or_serv IS DISTINCT FROM usaspending.country_of_product_or_serv)
        OR (broker.country_of_product_or_desc IS DISTINCT FROM usaspending.country_of_product_or_desc)
        OR (broker.construction_wage_rate_req IS DISTINCT FROM usaspending.construction_wage_rate_req)
        OR (broker.construction_wage_rat_desc IS DISTINCT FROM usaspending.construction_wage_rat_desc)
        OR (broker.evaluated_preference IS DISTINCT FROM usaspending.evaluated_preference)
        OR (broker.evaluated_preference_desc IS DISTINCT FROM usaspending.evaluated_preference_desc)
        OR (broker.extent_competed IS DISTINCT FROM usaspending.extent_competed)
        OR (broker.extent_compete_description IS DISTINCT FROM usaspending.extent_compete_description)
        OR (broker.fed_biz_opps IS DISTINCT FROM usaspending.fed_biz_opps)
        OR (broker.fed_biz_opps_description IS DISTINCT FROM usaspending.fed_biz_opps_description)
        OR (broker.foreign_funding IS DISTINCT FROM usaspending.foreign_funding)
        OR (broker.foreign_funding_desc IS DISTINCT FROM usaspending.foreign_funding_desc)
        OR (broker.government_furnished_prope IS DISTINCT FROM usaspending.government_furnished_prope)
        OR (broker.government_furnished_desc IS DISTINCT FROM usaspending.government_furnished_desc)
        OR (broker.information_technology_com IS DISTINCT FROM usaspending.information_technology_com)
        OR (broker.information_technolog_desc IS DISTINCT FROM usaspending.information_technolog_desc)
        OR (broker.interagency_contracting_au IS DISTINCT FROM usaspending.interagency_contracting_au)
        OR (broker.interagency_contract_desc IS DISTINCT FROM usaspending.interagency_contract_desc)
        OR (broker.local_area_set_aside IS DISTINCT FROM usaspending.local_area_set_aside)
        OR (broker.local_area_set_aside_desc IS DISTINCT FROM usaspending.local_area_set_aside_desc)
        OR (broker.major_program IS DISTINCT FROM usaspending.major_program)
        OR (broker.purchase_card_as_payment_m IS DISTINCT FROM usaspending.purchase_card_as_payment_m)
        OR (broker.purchase_card_as_paym_desc IS DISTINCT FROM usaspending.purchase_card_as_paym_desc)
        OR (broker.multi_year_contract IS DISTINCT FROM usaspending.multi_year_contract)
        OR (broker.multi_year_contract_desc IS DISTINCT FROM usaspending.multi_year_contract_desc)
        OR (broker.national_interest_action IS DISTINCT FROM usaspending.national_interest_action)
        OR (broker.national_interest_desc IS DISTINCT FROM usaspending.national_interest_desc)
        OR (broker.number_of_actions IS DISTINCT FROM usaspending.number_of_actions)
        OR (broker.number_of_offers_received IS DISTINCT FROM usaspending.number_of_offers_received)
        OR (broker.other_statutory_authority IS DISTINCT FROM usaspending.other_statutory_authority)
        OR (broker.performance_based_service IS DISTINCT FROM usaspending.performance_based_service)
        OR (broker.performance_based_se_desc IS DISTINCT FROM usaspending.performance_based_se_desc)
        OR (broker.place_of_manufacture IS DISTINCT FROM usaspending.place_of_manufacture)
        OR (broker.place_of_manufacture_desc IS DISTINCT FROM usaspending.place_of_manufacture_desc)
        OR (broker.price_evaluation_adjustmen IS DISTINCT FROM usaspending.price_evaluation_adjustmen)
        OR (broker.product_or_service_code IS DISTINCT FROM usaspending.product_or_service_code)
        OR (broker.product_or_service_co_desc IS DISTINCT FROM usaspending.product_or_service_co_desc)
        OR (broker.program_acronym IS DISTINCT FROM usaspending.program_acronym)
        OR (broker.other_than_full_and_open_c IS DISTINCT FROM usaspending.other_than_full_and_open_c)
        OR (broker.other_than_full_and_o_desc IS DISTINCT FROM usaspending.other_than_full_and_o_desc)
        OR (broker.recovered_materials_sustai IS DISTINCT FROM usaspending.recovered_materials_sustai)
        OR (broker.recovered_materials_s_desc IS DISTINCT FROM usaspending.recovered_materials_s_desc)
        OR (broker.research IS DISTINCT FROM usaspending.research)
        OR (broker.research_description IS DISTINCT FROM usaspending.research_description)
        OR (broker.sea_transportation IS DISTINCT FROM usaspending.sea_transportation)
        OR (broker.sea_transportation_desc IS DISTINCT FROM usaspending.sea_transportation_desc)
        OR (broker.labor_standards IS DISTINCT FROM usaspending.labor_standards)
        OR (broker.labor_standards_descrip IS DISTINCT FROM usaspending.labor_standards_descrip)
        OR (broker.small_business_competitive IS DISTINCT FROM usaspending.small_business_competitive)
        OR (broker.solicitation_identifier IS DISTINCT FROM usaspending.solicitation_identifier)
        OR (broker.solicitation_procedures IS DISTINCT FROM usaspending.solicitation_procedures)
        OR (broker.solicitation_procedur_desc IS DISTINCT FROM usaspending.solicitation_procedur_desc)
        OR (broker.fair_opportunity_limited_s IS DISTINCT FROM usaspending.fair_opportunity_limited_s)
        OR (broker.fair_opportunity_limi_desc IS DISTINCT FROM usaspending.fair_opportunity_limi_desc)
        OR (broker.subcontracting_plan IS DISTINCT FROM usaspending.subcontracting_plan)
        OR (broker.subcontracting_plan_desc IS DISTINCT FROM usaspending.subcontracting_plan_desc)
        OR (broker.program_system_or_equipmen IS DISTINCT FROM usaspending.program_system_or_equipmen)
        OR (broker.program_system_or_equ_desc IS DISTINCT FROM usaspending.program_system_or_equ_desc)
        OR (broker.type_set_aside IS DISTINCT FROM usaspending.type_set_aside)
        OR (broker.type_set_aside_description IS DISTINCT FROM usaspending.type_set_aside_description)
        OR (broker.epa_designated_product IS DISTINCT FROM usaspending.epa_designated_product)
        OR (broker.epa_designated_produc_desc IS DISTINCT FROM usaspending.epa_designated_produc_desc)
        OR (broker.materials_supplies_article IS DISTINCT FROM usaspending.materials_supplies_article)
        OR (broker.materials_supplies_descrip IS DISTINCT FROM usaspending.materials_supplies_descrip)
        OR (broker.transaction_number IS DISTINCT FROM usaspending.transaction_number)
        OR (broker.sam_exception IS DISTINCT FROM usaspending.sam_exception)
        OR (broker.sam_exception_description IS DISTINCT FROM usaspending.sam_exception_description)
        OR (broker.city_local_government IS DISTINCT FROM usaspending.city_local_government)
        OR (broker.county_local_government IS DISTINCT FROM usaspending.county_local_government)
        OR (broker.inter_municipal_local_gove IS DISTINCT FROM usaspending.inter_municipal_local_gove)
        OR (broker.local_government_owned IS DISTINCT FROM usaspending.local_government_owned)
        OR (broker.municipality_local_governm IS DISTINCT FROM usaspending.municipality_local_governm)
        OR (broker.school_district_local_gove IS DISTINCT FROM usaspending.school_district_local_gove)
        OR (broker.township_local_government IS DISTINCT FROM usaspending.township_local_government)
        OR (broker.us_state_government IS DISTINCT FROM usaspending.us_state_government)
        OR (broker.us_federal_government IS DISTINCT FROM usaspending.us_federal_government)
        OR (broker.federal_agency IS DISTINCT FROM usaspending.federal_agency)
        OR (broker.federally_funded_research IS DISTINCT FROM usaspending.federally_funded_research)
        OR (broker.us_tribal_government IS DISTINCT FROM usaspending.us_tribal_government)
        OR (broker.foreign_government IS DISTINCT FROM usaspending.foreign_government)
        OR (broker.community_developed_corpor IS DISTINCT FROM usaspending.community_developed_corpor)
        OR (broker.labor_surplus_area_firm IS DISTINCT FROM usaspending.labor_surplus_area_firm)
        OR (broker.corporate_entity_not_tax_e IS DISTINCT FROM usaspending.corporate_entity_not_tax_e)
        OR (broker.corporate_entity_tax_exemp IS DISTINCT FROM usaspending.corporate_entity_tax_exemp)
        OR (broker.partnership_or_limited_lia IS DISTINCT FROM usaspending.partnership_or_limited_lia)
        OR (broker.sole_proprietorship IS DISTINCT FROM usaspending.sole_proprietorship)
        OR (broker.small_agricultural_coopera IS DISTINCT FROM usaspending.small_agricultural_coopera)
        OR (broker.international_organization IS DISTINCT FROM usaspending.international_organization)
        OR (broker.us_government_entity IS DISTINCT FROM usaspending.us_government_entity)
        OR (broker.emerging_small_business IS DISTINCT FROM usaspending.emerging_small_business)
        OR (broker.c8a_program_participant IS DISTINCT FROM usaspending.c8a_program_participant)
        OR (broker.sba_certified_8_a_joint_ve IS DISTINCT FROM usaspending.sba_certified_8_a_joint_ve)
        OR (broker.dot_certified_disadvantage IS DISTINCT FROM usaspending.dot_certified_disadvantage)
        OR (broker.self_certified_small_disad IS DISTINCT FROM usaspending.self_certified_small_disad)
        OR (broker.historically_underutilized IS DISTINCT FROM usaspending.historically_underutilized)
        OR (broker.small_disadvantaged_busine IS DISTINCT FROM usaspending.small_disadvantaged_busine)
        OR (broker.the_ability_one_program IS DISTINCT FROM usaspending.the_ability_one_program)
        OR (broker.historically_black_college IS DISTINCT FROM usaspending.historically_black_college)
        OR (broker.c1862_land_grant_college IS DISTINCT FROM usaspending.c1862_land_grant_college)
        OR (broker.c1890_land_grant_college IS DISTINCT FROM usaspending.c1890_land_grant_college)
        OR (broker.c1994_land_grant_college IS DISTINCT FROM usaspending.c1994_land_grant_college)
        OR (broker.minority_institution IS DISTINCT FROM usaspending.minority_institution)
        OR (broker.private_university_or_coll IS DISTINCT FROM usaspending.private_university_or_coll)
        OR (broker.school_of_forestry IS DISTINCT FROM usaspending.school_of_forestry)
        OR (broker.state_controlled_instituti IS DISTINCT FROM usaspending.state_controlled_instituti)
        OR (broker.tribal_college IS DISTINCT FROM usaspending.tribal_college)
        OR (broker.veterinary_college IS DISTINCT FROM usaspending.veterinary_college)
        OR (broker.educational_institution IS DISTINCT FROM usaspending.educational_institution)
        OR (broker.alaskan_native_servicing_i IS DISTINCT FROM usaspending.alaskan_native_servicing_i)
        OR (broker.community_development_corp IS DISTINCT FROM usaspending.community_development_corp)
        OR (broker.native_hawaiian_servicing IS DISTINCT FROM usaspending.native_hawaiian_servicing)
        OR (broker.domestic_shelter IS DISTINCT FROM usaspending.domestic_shelter)
        OR (broker.manufacturer_of_goods IS DISTINCT FROM usaspending.manufacturer_of_goods)
        OR (broker.hospital_flag IS DISTINCT FROM usaspending.hospital_flag)
        OR (broker.veterinary_hospital IS DISTINCT FROM usaspending.veterinary_hospital)
        OR (broker.hispanic_servicing_institu IS DISTINCT FROM usaspending.hispanic_servicing_institu)
        OR (broker.foundation IS DISTINCT FROM usaspending.foundation)
        OR (broker.woman_owned_business IS DISTINCT FROM usaspending.woman_owned_business)
        OR (broker.minority_owned_business IS DISTINCT FROM usaspending.minority_owned_business)
        OR (broker.women_owned_small_business IS DISTINCT FROM usaspending.women_owned_small_business)
        OR (broker.economically_disadvantaged IS DISTINCT FROM usaspending.economically_disadvantaged)
        OR (broker.joint_venture_women_owned IS DISTINCT FROM usaspending.joint_venture_women_owned)
        OR (broker.joint_venture_economically IS DISTINCT FROM usaspending.joint_venture_economically)
        OR (broker.veteran_owned_business IS DISTINCT FROM usaspending.veteran_owned_business)
        OR (broker.service_disabled_veteran_o IS DISTINCT FROM usaspending.service_disabled_veteran_o)
        OR (broker.contracts IS DISTINCT FROM usaspending.contracts)
        OR (broker.grants IS DISTINCT FROM usaspending.grants)
        OR (broker.receives_contracts_and_gra IS DISTINCT FROM usaspending.receives_contracts_and_gra)
        OR (broker.airport_authority IS DISTINCT FROM usaspending.airport_authority)
        OR (broker.council_of_governments IS DISTINCT FROM usaspending.council_of_governments)
        OR (broker.housing_authorities_public IS DISTINCT FROM usaspending.housing_authorities_public)
        OR (broker.interstate_entity IS DISTINCT FROM usaspending.interstate_entity)
        OR (broker.planning_commission IS DISTINCT FROM usaspending.planning_commission)
        OR (broker.port_authority IS DISTINCT FROM usaspending.port_authority)
        OR (broker.transit_authority IS DISTINCT FROM usaspending.transit_authority)
        OR (broker.subchapter_s_corporation IS DISTINCT FROM usaspending.subchapter_s_corporation)
        OR (broker.limited_liability_corporat IS DISTINCT FROM usaspending.limited_liability_corporat)
        OR (broker.foreign_owned_and_located IS DISTINCT FROM usaspending.foreign_owned_and_located)
        OR (broker.american_indian_owned_busi IS DISTINCT FROM usaspending.american_indian_owned_busi)
        OR (broker.alaskan_native_owned_corpo IS DISTINCT FROM usaspending.alaskan_native_owned_corpo)
        OR (broker.indian_tribe_federally_rec IS DISTINCT FROM usaspending.indian_tribe_federally_rec)
        OR (broker.native_hawaiian_owned_busi IS DISTINCT FROM usaspending.native_hawaiian_owned_busi)
        OR (broker.tribally_owned_business IS DISTINCT FROM usaspending.tribally_owned_business)
        OR (broker.asian_pacific_american_own IS DISTINCT FROM usaspending.asian_pacific_american_own)
        OR (broker.black_american_owned_busin IS DISTINCT FROM usaspending.black_american_owned_busin)
        OR (broker.hispanic_american_owned_bu IS DISTINCT FROM usaspending.hispanic_american_owned_bu)
        OR (broker.native_american_owned_busi IS DISTINCT FROM usaspending.native_american_owned_busi)
        OR (broker.subcontinent_asian_asian_i IS DISTINCT FROM usaspending.subcontinent_asian_asian_i)
        OR (broker.other_minority_owned_busin IS DISTINCT FROM usaspending.other_minority_owned_busin)
        OR (broker.for_profit_organization IS DISTINCT FROM usaspending.for_profit_organization)
        OR (broker.nonprofit_organization IS DISTINCT FROM usaspending.nonprofit_organization)
        OR (broker.other_not_for_profit_organ IS DISTINCT FROM usaspending.other_not_for_profit_organ)
        OR (broker.us_local_government IS DISTINCT FROM usaspending.us_local_government)
        OR (broker.referenced_idv_modificatio IS DISTINCT FROM usaspending.referenced_idv_modificatio)
        OR (broker.undefinitized_action IS DISTINCT FROM usaspending.undefinitized_action)
        OR (broker.undefinitized_action_desc IS DISTINCT FROM usaspending.undefinitized_action_desc)
        OR (broker.domestic_or_foreign_entity IS DISTINCT FROM usaspending.domestic_or_foreign_entity)
        OR (broker.domestic_or_foreign_e_desc IS DISTINCT FROM usaspending.domestic_or_foreign_e_desc)
        OR (broker.pulled_from IS DISTINCT FROM usaspending.pulled_from)
        OR (broker.last_modified IS DISTINCT FROM usaspending.last_modified)
        OR (broker.annual_revenue IS DISTINCT FROM usaspending.annual_revenue)
        OR (broker.division_name IS DISTINCT FROM usaspending.division_name)
        OR (broker.division_number_or_office IS DISTINCT FROM usaspending.division_number_or_office)
        OR (broker.number_of_employees IS DISTINCT FROM usaspending.number_of_employees)
        OR (broker.vendor_alternate_name IS DISTINCT FROM usaspending.vendor_alternate_name)
        OR (broker.vendor_alternate_site_code IS DISTINCT FROM usaspending.vendor_alternate_site_code)
        OR (broker.vendor_enabled IS DISTINCT FROM usaspending.vendor_enabled)
        OR (broker.vendor_legal_org_name IS DISTINCT FROM usaspending.vendor_legal_org_name)
        OR (broker.vendor_location_disabled_f IS DISTINCT FROM usaspending.vendor_location_disabled_f)
        OR (broker.vendor_site_code IS DISTINCT FROM usaspending.vendor_site_code)
        OR (broker.initial_report_date IS DISTINCT FROM usaspending.initial_report_date)
        OR (broker.base_and_all_options_value IS DISTINCT FROM usaspending.base_and_all_options_value)
        OR (broker.base_exercised_options_val IS DISTINCT FROM usaspending.base_exercised_options_val)
        OR (broker.total_obligated_amount IS DISTINCT FROM usaspending.total_obligated_amount)
        OR (broker.place_of_perform_country_n IS DISTINCT FROM usaspending.place_of_perform_country_n)
        OR (broker.place_of_perform_state_nam IS DISTINCT FROM usaspending.place_of_perform_state_nam)
        OR (broker.referenced_idv_agency_name IS DISTINCT FROM usaspending.referenced_idv_agency_name)
        OR (broker.award_or_idv_flag IS DISTINCT FROM usaspending.award_or_idv_flag)
        OR (broker.legal_entity_county_code IS DISTINCT FROM usaspending.legal_entity_county_code)
        OR (broker.legal_entity_county_name IS DISTINCT FROM usaspending.legal_entity_county_name)
        OR (broker.legal_entity_zip5 IS DISTINCT FROM usaspending.legal_entity_zip5)
        OR (broker.legal_entity_zip_last4 IS DISTINCT FROM usaspending.legal_entity_zip_last4)
        OR (broker.place_of_perform_county_co IS DISTINCT FROM usaspending.place_of_perform_county_co)
        OR (broker.place_of_performance_zip5 IS DISTINCT FROM usaspending.place_of_performance_zip5)
        OR (broker.place_of_perform_zip_last4 IS DISTINCT FROM usaspending.place_of_perform_zip_last4)
        OR (broker.cage_code IS DISTINCT FROM usaspending.cage_code)
        OR (broker.inherently_government_func IS DISTINCT FROM usaspending.inherently_government_func)
        OR (broker.organizational_type IS DISTINCT FROM usaspending.organizational_type)
        OR (broker.inherently_government_desc IS DISTINCT FROM usaspending.inherently_government_desc)
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