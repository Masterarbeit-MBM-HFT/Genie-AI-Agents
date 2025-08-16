# 1. Import Libaries
import pandas as pd
import os

# 2. File Handling
_local_common_path = "./dummy_datasets/"
_input_files = [file for file in os.listdir(_local_common_path + "v1/") if "clean" not in file]

# 3. Data Cleaning
for _input_file_name in _input_files:
    if "contract" in _input_file_name:
        _input_full_path = _local_common_path + f"v1/{_input_file_name}"

        _output_file_name_v1 = _input_file_name[:-7]+'_clean_v1.csv'
        _output_file_name_v2 = _input_file_name[:-7]+'_clean_v2.csv'

        df = pd.read_csv(_input_full_path, sep=";")

        if "Unnamed: 0" in df.columns:
            df = df.drop("Unnamed: 0", axis=1)
        _remove_columns = [col for col in df.columns if "_cal" in col]
        _keep_columns = df.columns.difference(_remove_columns)
        df_clean = df[_keep_columns]

        df_clean.to_csv(_local_common_path + f"v1/{_output_file_name_v1}", sep=";", index=False)
        df_clean.to_csv(_local_common_path + f"v2/{_output_file_name_v2}", sep=";", index=False)