def return_base_query(delivering_entity='58', max_reporting_date="2024-10-31", limit_number=10, source_system='SYS', dummy_analysis_entity='XY', dummy_delivering_entity='50', dummy_currency='EUR'):
    
    base_query = f'''
    SELECT
    DISTINCT
    '{dummy_analysis_entity}' cfs_analysis_entity,
    '{dummy_delivering_entity}' cfs_delivering_entity,
    cfs_reporting_date,
    concat('a',substring(md5(cfs_cms_contract_number),3,8),'xy',substring(md5(cfs_cms_contract_number),4,9),'z') cfs_cms_contract_number,
    '{source_system}' cfs_source_system,
    cfs_cashflow_type,
    cfs_payment_date,
    cfs_cashflow,
    cfs_drv_alm_flag,
    cfs_drv_risk_flag,
    MGT_InsertTime,
    MGT_UpdateTime
    FROM
    westeurope_spire_platform_int.application_rdr_historymodel.fact_pd_cashflow
    WHERE cfs_delivering_entity={delivering_entity}
    AND cfs_reporting_date = '{max_reporting_date}'
    LIMIT {limit_number};
    '''

    return base_query