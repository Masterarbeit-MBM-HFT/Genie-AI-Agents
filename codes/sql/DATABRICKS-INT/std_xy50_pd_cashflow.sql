SELECT
DISTINCT
'XY' cfs_analysis_entity, --'{dummy_analysis_entity}'
'50' cfs_delivering_entity, --'{dummy_delivering_entity}'
cfs_reporting_date,
concat('a',substring(md5(cfs_cms_contract_number),3,8),'xy',substring(md5(cfs_cms_contract_number),4,9),'z') cfs_cms_contract_number,
'SYS' cfs_source_system, --'{source_system}'
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
westeurope_spire_platform_int.application_rdr_standard.std_us58_pd_cashflow --{table_name}
limit 10; --{limit_number}