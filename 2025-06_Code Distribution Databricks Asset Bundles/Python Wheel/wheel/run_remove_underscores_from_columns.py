from z_data_cleansing.remove_underscores_from_columns import *

# Example Usage
column_names = ["first_name", "last_name", "email_address"]
column_names = remove_underscores_from_columns(column_names)
print(column_names)



"""
# Run Wheel 1:
cd ../../../
python ./run_remove_underscores_from_columns.py

# Install wheel:
cd z_data_cleansing/dist/
py -m pip install z_data_cleansing-0.1.0-py3-none-any.whl

"""

"""
# Run Wheel 2:
cd ../../
python ./run_remove_underscores_from_columns.py

"""