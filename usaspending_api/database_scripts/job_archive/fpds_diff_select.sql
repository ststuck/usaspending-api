SELECT
    'fpds'::TEXT as "system",
    usaspending.transaction_id,
    usaspending.detached_award_procurement_id, AS "broker_surrogate_id",
    usaspending.detached_award_proc_unique AS "broker_derived_unique_key",
    usaspending.piid AS "piid_fain_uri",
    usaspending.unique_award_key,
    usaspending.action_date::date,
    usaspending.modified_at::date AS "record_last_modified",
    broker.created_at::timestamp with time zone AS "broker_record_create",
    broker.updated_at::timestamp with time zone AS "broker_record_update",
    transaction_normalized.create_date::timestamp with time zone AS "usaspending_record_create",
    transaction_normalized.update_date::timestamp with time zone AS "usaspending_record_update"
FROM transaction_fpds AS usaspending
INNER JOIN transaction_normalized ON usaspending.transaction_id = transaction_normalized.id
INNER JOIN
(
    SELECT * FROM dblink (
        'broker_server',
        'SELECT * FROM detached_award_procurement WHERE detached_award_procurement_id BETWEEN {minid} AND {maxid}'
    ) AS broker (
        created_at TIMESTAMP WITHOUT time ZONE,
        updated_at TIMESTAMP WITHOUT time ZONE,
        detached_award_procurement_id int,
        detached_award_proc_unique text,
        piid text,
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
        federal_action_obligation numeric,
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
        small_business_competitive text,
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
        city_local_government text,
        county_local_government text,
        inter_municipal_local_gove text,
        local_government_owned text,
        municipality_local_governm text,
        school_district_local_gove text,
        township_local_government text,
        us_state_government text,
        us_federal_government text,
        federal_agency text,
        federally_funded_research text,
        us_tribal_government text,
        foreign_government text,
        community_developed_corpor text,
        labor_surplus_area_firm text,
        corporate_entity_not_tax_e text,
        corporate_entity_tax_exemp text,
        partnership_or_limited_lia text,
        sole_proprietorship text,
        small_agricultural_coopera text,
        international_organization text,
        us_government_entity text,
        emerging_small_business text,
        c8a_program_participant text,
        sba_certified_8_a_joint_ve text,
        dot_certified_disadvantage text,
        self_certified_small_disad text,
        historically_underutilized text,
        small_disadvantaged_busine text,
        the_ability_one_program text,
        historically_black_college text,
        c1862_land_grant_college text,
        c1890_land_grant_college text,
        c1994_land_grant_college text,
        minority_institution text,
        private_university_or_coll text,
        school_of_forestry text,
        state_controlled_instituti text,
        tribal_college text,
        veterinary_college text,
        educational_institution text,
        alaskan_native_servicing_i text,
        community_development_corp text,
        native_hawaiian_servicing text,
        domestic_shelter text,
        manufacturer_of_goods text,
        hospital_flag text,
        veterinary_hospital text,
        hispanic_servicing_institu text,
        foundation text,
        woman_owned_business text,
        minority_owned_business text,
        women_owned_small_business text,
        economically_disadvantaged text,
        joint_venture_women_owned text,
        joint_venture_economically text,
        veteran_owned_business text,
        service_disabled_veteran_o text,
        contracts text,
        grants text,
        receives_contracts_and_gra text,
        airport_authority text,
        council_of_governments text,
        housing_authorities_public text,
        interstate_entity text,
        planning_commission text,
        port_authority text,
        transit_authority text,
        subchapter_s_corporation text,
        limited_liability_corporat text,
        foreign_owned_and_located text,
        american_indian_owned_busi text,
        alaskan_native_owned_corpo text,
        indian_tribe_federally_rec text,
        native_hawaiian_owned_busi text,
        tribally_owned_business text,
        asian_pacific_american_own text,
        black_american_owned_busin text,
        hispanic_american_owned_bu text,
        native_american_owned_busi text,
        subcontinent_asian_asian_i text,
        other_minority_owned_busin text,
        for_profit_organization text,
        nonprofit_organization text,
        other_not_for_profit_organ text,
        us_local_government text,
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
        business_categories text[],
        cage_code text,
        inherently_government_func text,
        organizational_type text,
        inherently_government_desc text,
        unique_award_key text,
        solicitation_date text,
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
        additional_reporting text
    )
) AS broker ON (
    (broker.detached_award_procurement_id = usaspending.detached_award_procurement_id)
    AND (
        (broker.piid IS DISTINCT FROM usaspending.piid)
        OR (broker.detached_award_proc_unique IS DISTINCT FROM usaspending.detached_award_proc_unique)
        OR (broker.agency_id IS DISTINCT FROM usaspending.agency_id)
        OR (broker.awarding_sub_tier_agency_c IS DISTINCT FROM usaspending.awarding_sub_tier_agency_c)
        OR (broker.awarding_sub_tier_agency_n IS DISTINCT FROM usaspending.awarding_sub_tier_agency_n)
        OR (broker.awarding_agency_code IS DISTINCT FROM usaspending.awarding_agency_code)
        OR (broker.awarding_agency_name IS DISTINCT FROM usaspending.awarding_agency_name)
        OR (broker.parent_award_id IS DISTINCT FROM usaspending.parent_award_id)
        OR (broker.award_modification_amendme IS DISTINCT FROM usaspending.award_modification_amendme)
        OR (broker.type_of_contract_pricing IS DISTINCT FROM usaspending.type_of_contract_pricing)
        OR (broker.type_of_contract_pric_desc IS DISTINCT FROM usaspending.type_of_contract_pricing)
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
        OR (broker.small_business_competitive IS DISTINCT FROM usaspending.small_business_competitive::text)
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
        OR (broker.city_local_government IS DISTINCT FROM usaspending.city_local_government::text)
        OR (broker.county_local_government IS DISTINCT FROM usaspending.county_local_government::text)
        OR (broker.inter_municipal_local_gove IS DISTINCT FROM usaspending.inter_municipal_local_gove::text)
        OR (broker.local_government_owned IS DISTINCT FROM usaspending.local_government_owned::text)
        OR (broker.municipality_local_governm IS DISTINCT FROM usaspending.municipality_local_governm::text)
        OR (broker.school_district_local_gove IS DISTINCT FROM usaspending.school_district_local_gove::text)
        OR (broker.township_local_government IS DISTINCT FROM usaspending.township_local_government::text)
        OR (broker.us_state_government IS DISTINCT FROM usaspending.us_state_government::text)
        OR (broker.us_federal_government IS DISTINCT FROM usaspending.us_federal_government::text)
        OR (broker.federal_agency IS DISTINCT FROM usaspending.federal_agency::text)
        OR (broker.federally_funded_research IS DISTINCT FROM usaspending.federally_funded_research::text)
        OR (broker.us_tribal_government IS DISTINCT FROM usaspending.us_tribal_government::text)
        OR (broker.foreign_government IS DISTINCT FROM usaspending.foreign_government::text)
        OR (broker.community_developed_corpor IS DISTINCT FROM usaspending.community_developed_corpor::text)
        OR (broker.labor_surplus_area_firm IS DISTINCT FROM usaspending.labor_surplus_area_firm::text)
        OR (broker.corporate_entity_not_tax_e IS DISTINCT FROM usaspending.corporate_entity_not_tax_e::text)
        OR (broker.corporate_entity_tax_exemp IS DISTINCT FROM usaspending.corporate_entity_tax_exemp::text)
        OR (broker.partnership_or_limited_lia IS DISTINCT FROM usaspending.partnership_or_limited_lia::text)
        OR (broker.sole_proprietorship IS DISTINCT FROM usaspending.sole_proprietorship::text)
        OR (broker.small_agricultural_coopera IS DISTINCT FROM usaspending.small_agricultural_coopera::text)
        OR (broker.international_organization IS DISTINCT FROM usaspending.international_organization::text)
        OR (broker.us_government_entity IS DISTINCT FROM usaspending.us_government_entity::text)
        OR (broker.emerging_small_business IS DISTINCT FROM usaspending.emerging_small_business::text)
        OR (broker.c8a_program_participant IS DISTINCT FROM usaspending.c8a_program_participant::text)
        OR (broker.sba_certified_8_a_joint_ve IS DISTINCT FROM usaspending.sba_certified_8_a_joint_ve::text)
        OR (broker.dot_certified_disadvantage IS DISTINCT FROM usaspending.dot_certified_disadvantage::text)
        OR (broker.self_certified_small_disad IS DISTINCT FROM usaspending.self_certified_small_disad::text)
        OR (broker.historically_underutilized IS DISTINCT FROM usaspending.historically_underutilized::text)
        OR (broker.small_disadvantaged_busine IS DISTINCT FROM usaspending.small_disadvantaged_busine::text)
        OR (broker.the_ability_one_program IS DISTINCT FROM usaspending.the_ability_one_program::text)
        OR (broker.historically_black_college IS DISTINCT FROM usaspending.historically_black_college::text)
        OR (broker.c1862_land_grant_college IS DISTINCT FROM usaspending.c1862_land_grant_college::text)
        OR (broker.c1890_land_grant_college IS DISTINCT FROM usaspending.c1890_land_grant_college::text)
        OR (broker.c1994_land_grant_college IS DISTINCT FROM usaspending.c1994_land_grant_college::text)
        OR (broker.minority_institution IS DISTINCT FROM usaspending.minority_institution::text)
        OR (broker.private_university_or_coll IS DISTINCT FROM usaspending.private_university_or_coll::text)
        OR (broker.school_of_forestry IS DISTINCT FROM usaspending.school_of_forestry::text)
        OR (broker.state_controlled_instituti IS DISTINCT FROM usaspending.state_controlled_instituti::text)
        OR (broker.tribal_college IS DISTINCT FROM usaspending.tribal_college::text)
        OR (broker.veterinary_college IS DISTINCT FROM usaspending.veterinary_college::text)
        OR (broker.educational_institution IS DISTINCT FROM usaspending.educational_institution::text)
        OR (broker.alaskan_native_servicing_i IS DISTINCT FROM usaspending.alaskan_native_servicing_i::text)
        OR (broker.community_development_corp IS DISTINCT FROM usaspending.community_development_corp::text)
        OR (broker.native_hawaiian_servicing IS DISTINCT FROM usaspending.native_hawaiian_servicing::text)
        OR (broker.domestic_shelter IS DISTINCT FROM usaspending.domestic_shelter::text)
        OR (broker.manufacturer_of_goods IS DISTINCT FROM usaspending.manufacturer_of_goods::text)
        OR (broker.hospital_flag IS DISTINCT FROM usaspending.hospital_flag::text)
        OR (broker.veterinary_hospital IS DISTINCT FROM usaspending.veterinary_hospital::text)
        OR (broker.hispanic_servicing_institu IS DISTINCT FROM usaspending.hispanic_servicing_institu::text)
        OR (broker.foundation IS DISTINCT FROM usaspending.foundation::text)
        OR (broker.woman_owned_business IS DISTINCT FROM usaspending.woman_owned_business::text)
        OR (broker.minority_owned_business IS DISTINCT FROM usaspending.minority_owned_business::text)
        OR (broker.women_owned_small_business IS DISTINCT FROM usaspending.women_owned_small_business::text)
        OR (broker.economically_disadvantaged IS DISTINCT FROM usaspending.economically_disadvantaged::text)
        OR (broker.joint_venture_women_owned IS DISTINCT FROM usaspending.joint_venture_women_owned::text)
        OR (broker.joint_venture_economically IS DISTINCT FROM usaspending.joint_venture_economically::text)
        OR (broker.veteran_owned_business IS DISTINCT FROM usaspending.veteran_owned_business::text)
        OR (broker.service_disabled_veteran_o IS DISTINCT FROM usaspending.service_disabled_veteran_o::text)
        OR (broker.contracts IS DISTINCT FROM usaspending.contracts::text)
        OR (broker.grants IS DISTINCT FROM usaspending.grants::text)
        OR (broker.receives_contracts_and_gra IS DISTINCT FROM usaspending.receives_contracts_and_gra::text)
        OR (broker.airport_authority IS DISTINCT FROM usaspending.airport_authority::text)
        OR (broker.council_of_governments IS DISTINCT FROM usaspending.council_of_governments::text)
        OR (broker.housing_authorities_public IS DISTINCT FROM usaspending.housing_authorities_public::text)
        OR (broker.interstate_entity IS DISTINCT FROM usaspending.interstate_entity::text)
        OR (broker.planning_commission IS DISTINCT FROM usaspending.planning_commission::text)
        OR (broker.port_authority IS DISTINCT FROM usaspending.port_authority::text)
        OR (broker.transit_authority IS DISTINCT FROM usaspending.transit_authority::text)
        OR (broker.subchapter_s_corporation IS DISTINCT FROM usaspending.subchapter_s_corporation::text)
        OR (broker.limited_liability_corporat IS DISTINCT FROM usaspending.limited_liability_corporat::text)
        OR (broker.foreign_owned_and_located IS DISTINCT FROM usaspending.foreign_owned_and_located::text)
        OR (broker.american_indian_owned_busi IS DISTINCT FROM usaspending.american_indian_owned_busi::text)
        OR (broker.alaskan_native_owned_corpo IS DISTINCT FROM usaspending.alaskan_native_owned_corpo::text)
        OR (broker.indian_tribe_federally_rec IS DISTINCT FROM usaspending.indian_tribe_federally_rec::text)
        OR (broker.native_hawaiian_owned_busi IS DISTINCT FROM usaspending.native_hawaiian_owned_busi::text)
        OR (broker.tribally_owned_business IS DISTINCT FROM usaspending.tribally_owned_business::text)
        OR (broker.asian_pacific_american_own IS DISTINCT FROM usaspending.asian_pacific_american_own::text)
        OR (broker.black_american_owned_busin IS DISTINCT FROM usaspending.black_american_owned_busin::text)
        OR (broker.hispanic_american_owned_bu IS DISTINCT FROM usaspending.hispanic_american_owned_bu::text)
        OR (broker.native_american_owned_busi IS DISTINCT FROM usaspending.native_american_owned_busi::text)
        OR (broker.subcontinent_asian_asian_i IS DISTINCT FROM usaspending.subcontinent_asian_asian_i::text)
        OR (broker.other_minority_owned_busin IS DISTINCT FROM usaspending.other_minority_owned_busin::text)
        OR (broker.for_profit_organization IS DISTINCT FROM usaspending.for_profit_organization::text)
        OR (broker.nonprofit_organization IS DISTINCT FROM usaspending.nonprofit_organization::text)
        OR (broker.other_not_for_profit_organ IS DISTINCT FROM usaspending.other_not_for_profit_organ::text)
        OR (broker.us_local_government IS DISTINCT FROM usaspending.us_local_government::text)
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
    )
)