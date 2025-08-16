def return_base_query(delivering_entity='58', max_reporting_date="2024-10-31", limit_number=10, source_system='SYS', dummy_analysis_entity='XY', dummy_delivering_entity='50', dummy_currency='EUR'):
    
    base_query = f'''
    SELECT
    DISTINCT
    '{dummy_analysis_entity}' wos_analysis_entity,
    '{dummy_delivering_entity}' wos_delivering_entity,
    wos_reporting_date,
    concat('a',substring(md5(wos_cms_contract_number),3,8),'xy',substring(md5(wos_cms_contract_number),4,9),'z') wos_cms_contract_number,
    '{source_system}' wos_source_system,
    wos_bevis_id,
    wos_net_rv_loss_amount,
    wos_loss_date,
    wos_write_off_date,
    wos_reposession_date,
    wos_remarketing_date,
    wos_remarketing_proceeds,
    wos_portfolio,
    '{dummy_currency}' wos_currency,
    wos_recovery_amount,
    wos_last_recovery_date,
    wos_net_loss_amount,
    wos_abs_code,
    wos_cal_periodnetcreditloss_amount,
    wos_drv_alm_flag,
    wos_drv_risk_flag,
    MGT_InsertTime,
    MGT_UpdateTime
    FROM
    westeurope_spire_platform_int.application_rdr_historymodel.fact_pd_writeoff
    WHERE wos_delivering_entity={delivering_entity}
    AND wos_reporting_date = '{max_reporting_date}'
    LIMIT {limit_number};
    '''

    return base_query