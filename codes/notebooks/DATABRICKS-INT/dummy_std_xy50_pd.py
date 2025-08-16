# 1. User Variables
_datasets_common_path = "./dummy_datasets/"
_source_system = "SYS"
_dummy_analysis_entity = 'XY'
_dummy_delivering_entity = '50'
_dummy_currency = 'EUR'

# 2. Import Libraries
import pandas as pd
from modules.portfolio_counts import std_portfolio_counts
from modules.dummy_queries import std_xy50_pd_contract, std_xy50_pd_cashflow, std_xy50_pd_writeoff

# 3. Functions
def return_table_names_with_limits(_file_choice):
    query = std_portfolio_counts.return_portfolio_counts(_file_choice)
    df_portfolio = spark.sql(query).toPandas()
    table_names_with_limits = df_portfolio[["table_name", "rows_to_select"]].apply(lambda x: (x[0], int(x[1])), axis=1).tolist()

    return table_names_with_limits

def get_base_query(_file_choice, table, _limit, _source_system, _dummy_analysis_entity, _dummy_delivering_entity, _dummy_currency):
    if _file_choice == 1:
        return std_xy50_pd_contract.return_base_query(table, limit_number=_limit, source_system=_source_system, dummy_analysis_entity=_dummy_analysis_entity, dummy_delivering_entity=_dummy_delivering_entity, dummy_currency=_dummy_currency)
    elif _file_choice == 2:
        return std_xy50_pd_cashflow.return_base_query(table, limit_number=_limit, source_system=_source_system, dummy_analysis_entity=_dummy_analysis_entity, dummy_delivering_entity=_dummy_delivering_entity, dummy_currency=_dummy_currency)
    elif _file_choice == 3:
        return std_xy50_pd_writeoff.return_base_query(table, limit_number=_limit, source_system=_source_system, dummy_analysis_entity=_dummy_analysis_entity, dummy_delivering_entity=_dummy_delivering_entity, dummy_currency=_dummy_currency)
    else:
        raise ValueError("Invalid file choice")

def return_dummy_df(_file_choice, _source_system, _dummy_analysis_entity, _dummy_delivering_entity, _dummy_currency):
    table_names_with_limits = return_table_names_with_limits(_file_choice)
    
    df_final = pd.DataFrame()
    for (table,_limit) in table_names_with_limits:
        current_query = get_base_query(_file_choice, table, _limit, _source_system, _dummy_analysis_entity, _dummy_delivering_entity, _dummy_currency)
        df_current = spark.sql(current_query).toPandas()
        df_final = pd.concat([df_final,df_current])

    return df_final

def prepare_dummy_datasets(_file_choice, df_std):
    _file_type = "_contracts" if _file_choice == 1 else "_cashflows" if _file_choice == 2 else "_writeoffs"
    samples_n = [100, 500, 1000, 5000]
    for n in samples_n:

        # v1: Files to be used for "Without metadata"
        df_std.sample(n).reset_index(drop=True).to_csv(_datasets_common_path + f"v1/std_{_dummy_analysis_entity}{_dummy_delivering_entity}_pd{_file_type}_{n}_v1.csv",sep=";", index=False)

        # v2: Files to be used for "With metadata"
        df_std.sample(n).reset_index(drop=True).to_csv(_datasets_common_path + f"v2/std_{_dummy_analysis_entity}{_dummy_delivering_entity}_pd{_file_type}_{n}_v2.csv",sep=";", index=False)

    print(f"Dummy datasets prepared for: {_file_choice}.")

# 4. Data Preparation

df_std_contracts = return_dummy_df(1, _source_system, _dummy_analysis_entity, _dummy_delivering_entity, _dummy_currency)
df_std_cashflows = return_dummy_df(2, _source_system, _dummy_analysis_entity, _dummy_delivering_entity, _dummy_currency)
df_std_writeoff = return_dummy_df(3, _source_system, _dummy_analysis_entity, _dummy_delivering_entity, _dummy_currency)

# 5. Save Dummy Datasets
prepare_dummy_datasets(1, df_std_contracts)
prepare_dummy_datasets(2, df_std_cashflows)
prepare_dummy_datasets(3, df_std_writeoff)