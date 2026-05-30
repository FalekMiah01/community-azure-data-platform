# Python Wheel for Cleansing

------------------------------------------------------------------------------------------------------------------------------------
# Optional

- **Virtual Environment** Create/activate a python virtual environment using `venv` for testing in the `directory` of the repository:

   ```
   # Create folder for venv
   py -m venv venv-fm
   
   # Activate venv
   ./venv-fm/Scripts/Activate.ps1
   ```

------------------------------------------------------------------------------------------------------------------------------------
# Pre-requisites

- **Setup Packages** - check and uninstall existing packages using `pip`

   ```
   # List all packages
   py -m pip list

   # Show specific packages details
   py -m pip show 'z_data_cleansing'
   ```

- **Install Package** - install poetry

   ```
   py -m pip install poetry

   -- py -m pip install pytest
   ```

------------------------------------------------------------------------------------------------------------------------------------
## Develop & Build Wheel

Setup development environment using the following steps:

- **Create Wheel Project** - in the `directory` of the repository and run the following:

   ```
   cd wheel

   py -m poetry new z_data_cleansing
   ```

- **Create Wheel Logic** - create new file `remove_underscores_from_columns.py` and copy following code:

   ```    
   def remove_underscores_from_columns(data: list) -> list:
        """
        Removes underscores from column names in a list.
        
        Parameters:
                data (list): The input list of column names.
                
        Returns:
                list: A new list with modified column names.
        """
        return [col.replace('_', '') if isinstance(col, str) else col for col in data]
   ```

- **Build Wheel** - navigate to the `wheel` directory in the repository and run the following:

   ```
   cd z_data_cleansing/src/z_data_cleansing

   py -m poetry build
   ```

- **Install Wheel** - navigate to the `dist` directory in the repository, run the following: 

   ```
   cd ../../dist/

   py -m pip install z_data_cleansing-0.1.0-py3-none-any.whl
   ```

------------------------------------------------------------------------------------------------------------------------------------
## Test & Run Wheel

- **Testing Wheel Logic** - create new file `run_remove_underscores_from_columns.py` and copy following code to test wheel:

   ```
   from z_data_cleansing import remove_underscores_from_columns

   # Example Usage
   column_names = ["first_name", "last_name", "email_address"]
   column_names = remove_underscores_from_columns(column_names)
   print(column_names)
   ```

- **Run Wheel** - navigate to the `wheel` directory in the repository and run the following:

   ```
   cd ../../
   
   python .\run_remove_underscores_from_columns.py
   ```

------------------------------------------------------------------------------------------------------------------------------------
## Automated Integration Test

- **Create Test** - create new file `test_remove_underscores_from_columns.py` and copy following code to test wheel using PyTest:
   
   ```
   def test_remove_underscores_from_columns():
        assert remove_underscores_from_columns(["first_name", "last_name", "email_address"]) == ["firstname", "lastname", "emailaddress"]
        assert remove_underscores_from_columns(["_leading", "trailing_", "_both_"]) == ["leading", "trailing", "both"]
        assert remove_underscores_from_columns(["nochange"]) == ["nochange"]
        assert remove_underscores_from_columns(["under_score_here"]) == ["underscorehere"]
        assert remove_underscores_from_columns([]) == []
   ```

- **Install `PyTest` Package** - install PyTest and add to Poetry

   ```
   py -m pip install pytest

   poetry add pytest
   ```

- **Run `all` Test** - run the Integration Test to ensure everything is setup correctly, using `PyTest`.

   ```
   cd ./z_data_cleansing
        
   # Run `All` tests
   pytest -v .\tests
   ```

- **Run `Speific` Test** - run the Integration Test to ensure everything is setup correctly, using `PyTest`.

   ```
   cd ./z_data_cleansing

   # Run `Speific` Test
   pytest -v .\tests\test_remove_underscores_from_columns.py
   ```
   
------------------------------------------------------------------------------------------------------------------------------------
# Deployment
  - **Manually Deploy** - deploy the Python Wheel to Databricks
    - In `Compute` Tab
    - Select and click on the `cluster` you want to deploy wheel to
    - Click on `Libraries` tab
    - Click `Install new` option
    - Select `File Path/ADLS` option under `Library Source` 
    - Select `Python Wheel` option under `Library Type`
    - Enter the File Path or Drag File
    - Click Install


------------------------------------------------------------------------------------------------------------------------------------
# Clean Up

- **Clean up** - uninstall existing packages using `pip` and deactivate virtual environment

   ```
   # Uninstall packages
   py -m pip uninstall 'z_data_cleansing'
        
   # Deactivate venv
   deactivate venv-fm
   ```