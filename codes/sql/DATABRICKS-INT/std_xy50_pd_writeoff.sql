SELECT
DISTINCT
'XY' wos_analysis_entity, --'{dummy_analysis_entity}'
'50' wos_delivering_entity, --'{dummy_delivering_entity}'
wos_reporting_date,
concat('a',substring(md5(wos_cms_contract_number),3,8),'xy',substring(md5(wos_cms_contract_number),4,9),'z') wos_cms_contract_number,
'SYS' wos_source_system, --'{source_system}'
wos_bevis_id,
wos_net_loss_amount,
wos_loss_date,
wos_write_off_date,
wos_reposession_date,
wos_remarketing_date,
wos_remarketing_proceeds,
wos_portfolio,
'EUR' wos_currency, --'{dummy_currency}'
wos_recovery_amount,
wos_last_recovery_date,
wos_net_rv_loss_amount,
wos_abs_code,
wos_cal_periodnetcreditloss_amount,
MGT_InsertTime,
MGT_UpdateTime,
MGT_ValidationStatus
FROM
westeurope_spire_platform_int.application_rdr_standard.std_us58_pd_writeoff --{table_name}
LIMIT 10; --{limit_number}