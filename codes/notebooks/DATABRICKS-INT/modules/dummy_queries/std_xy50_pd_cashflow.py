def return_base_query(table_name, limit_number=10, source_system='SYS', dummy_analysis_entity='XY', dummy_delivering_entity='50', dummy_currency='EUR'):
    
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
    cfs_drv_max_payment_date,
    cfs_drv_sum_of_redemptions,
    cfs_drv_first_payment_date,
    MGT_InsertTime,
    MGT_UpdateTime,
    MGT_ValidationStatus
    FROM
    {table_name}
    LIMIT {limit_number};
    '''

    return base_query