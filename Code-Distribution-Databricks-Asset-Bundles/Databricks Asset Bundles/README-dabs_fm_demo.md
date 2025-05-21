# Databricks Asset Bundle (DAB) Package

------------------------------------------------------------------------------------------------------------------------------------
## Pre-requisites

- Azure Subscription
- Azure Databricks with Unity Catalog enabled
- Databricks CLI
- Azure DevOps

------------------------------------------------------------------------------------------------------------------------------------
## Getting started

- Install the Databricks CLI from https://docs.databricks.com/dev-tools/cli/databricks-cli.html
<br><br>

- Set location: `\Databricks Asset Bundles`
<br><br>

- Use the default bundle template to create `dabs_fm_demo_live` project
  ```
   databricks bundle init default-python
   ```
   _Default Options: `default-python`, `default-sql`, `dbt-sql`, `mlops-stacks`_
<br>

- Check what has been created
   
   ```
   cd dabs_fm_demo_live
   ls
   ```

------------------------------------------------------------------------------------------------------------------------------------
## Simple Project (Default)

- Review: key configure files:
  - `src/` - stores the **source code** (notebooks, Python modules, DLT)
  <br><br>
  - `resources/` - contains **assets** (jobs, pipelines and define tasks, cluster type (job/serverless) and dependencies )
  <br><br>
  - `databricks.yml` - contains the bundle **configuration & deployment** details. 
     - `bundle` name has been defined
     - `resources/*.yml` will contain the Databricks assets to be deployed
     - `workspace host` uses the Databricks CLI configuration
<br><br>

- Validate bundle project - Checks that the YAML syntax and schema is valid (i.e. field mappings and types)
   ```
   databricks bundle validate --profile DEFAULT
   ```

- Deploy bundle project - deployed to target workspace under the `user` folder
   ```
   databricks bundle deploy --target dev -p DEFAULT
   ```

- Run a job or pipeline - run the deployed jobs or pipeline

   ```
   databricks bundle run -t dev -p DEFAULT dabs_fm_demo_live.job
   ```

------------------------------------------------------------------------------------------------------------------------------------
## Advance Project (Lakehouse)

- Create **Notebooks** for Lakehouse workloads in `src/` folder:
  - `nb_1_landing_to_bronze`
  - `nb_2_bronze_to_silver`
  - `nb_3_silver_to_gold`
<br><br>

- Create **YML file** for the Lakehouse workloads in `resources/` folder:
  - Create a file `dabs_lakehouse_demo_job.yml` 
  - Ensure tasks & notebook_path point to the relevant `notebooks` defined
<br><br>

- Review the `databrick.yaml` file 
  - Ensure setting is pointing to desired target
  - Ensure required `resources/*.yml` are selected
<br><br>

  ### Deploy Manually

- Set location: `\Databricks Asset Bundles\dabs_lakehouse_demo`
   ```
   cd ../dabs_fm_demo
   ```
<br>

- Validate bundle project
   ```
   databricks bundle validate --profile DEFAULT
   ```
<br>

------------------------------------------------------------------------------------------------------------------------------------
## Deploy the Project

- Deploy bundle project
   ```
   databricks bundle deploy --target dev -p DEFAULT
   ```
<br>

- Run a job or pipeline - Demo
   ```
   databricks bundle run -t dev -p DEFAULT dabs_lakehouse_demo_job
   ```

- Run a job or pipeline - Clean Up - Using Serverless
   ```
   databricks bundle run -t dev -p DEFAULT dabs_lakehouse_demo_x_clean_up
   ```

------------------------------------------------------------------------------------------------------------------------------------
## Clean Up

- Destroy/delete all jobs, pipelines, and artifacts deployed, by running command from the bundle root:
  
   ```
   databricks bundle destroy -t dev -p DEFAULT
   ```