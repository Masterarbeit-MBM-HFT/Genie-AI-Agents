SELECT
DISTINCT
'XY' cfs_analysis_entity, --'{dummy_analysis_entity}'
'50' cfs_delivering_entity, --'{dummy_delivering_entity}'
cfs_reporting_date,
concat('a',substring(md5(cfs_cms_contract_number),3,8),'xy',substring(md5(cfs_cms_contract_number),4,9),'z') cfs_cms_contract_number,
'SYS' cfs_source_system, --'{source_system}
cfs_cashflow_type,
cfs_payment_date,
cfs_cashflow,
cfs_drv_alm_flag,
cfs_drv_risk_flag,
MGT_InsertTime,
MGT_UpdateTime
FROM
westeurope_spire_platform_int.application_rdr_historymodel.fact_pd_cashflow
where cfs_delivering_entity = 58 --{delivering_entity}
and cfs_reporting_date = '2024-10-31' --'{max_reporting_date}'
limit 10; --{limit_number}