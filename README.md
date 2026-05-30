# Community Azure Data Platform

A community reference repository for Azure data platform patterns built on Databricks. Contains conference and meetup demos, slides, templates, and educational notebooks covering Databricks Asset Bundles, Delta Lake, Python Wheels, and CI/CD integration.

---

## Session & Demos

| Session | Topic | Tech Stack | README |
|---------|-------|------------|--------|
| [Terraform + DevOps: Databricks](2022-03_Terraform-DevOps-Databricks/) | Infrastructure as Code for Databricks | Terraform, Azure DevOps | — |
| [Terraform + DevOps: Synapse](2022-03_Terraform-DevOps-Synapse/) | Infrastructure as Code for Synapse | Terraform, Azure DevOps | — |
| [Intro to Azure DevOps](2023-04_Intro%20to%20the%20Wonders%20of%20Azure%20DevOps/) | Azure DevOps fundamentals for data teams | Azure DevOps, YAML Pipelines | — |
| [Spark Execution Plans](2023-06_Spark%20Execution%20Plans%20for%20Databricks/) | Physical, logical and optimised query plans | PySpark, Databricks | — |
| [Value of DevOps Release Process](2024-03_Value%20of%20DevOps%20Release%20Process/) | Release process patterns for data pipelines | Azure DevOps | — |
| [Quest to Delta Optimisation](2024-04_Quest%20to%20Delta%20Optimisation/) | Delta Lake performance tuning | Delta Lake, PySpark | — |
| [Code Distribution with DABs](2025-06_Code%20Distribution%20Databricks%20Asset%20Bundles/) | End-to-end DABs demo with CI/CD | DABs, Python Wheel, PyTest, GitHub Actions, Azure DevOps | [README](2025-06_Code%20Distribution%20Databricks%20Asset%20Bundles/Databricks%20Asset%20Bundles/dabs_fm_demo/README.md) |
| [Lakeflow Declarative Pipelines](2026-06_Databricks-Lakeflow-Declarative-Pipelines/) | Lakeflow SDP, Connect, and Jobs — DPNS 2026 | Lakeflow SDP, Lakeflow Connect, Lakeflow Jobs, DABs | — |
| [Databricks Execution Plans](Databricks-Execution-Plans/) | Blog - Standalone execution plan walkthrough | PySpark, Databricks | [README](Databricks-Execution-Plans/README.md) |
| [Delta vs Spark Cache](Databricks-Delta-Spark-Cache/) | Blog - Caching strategy comparison on NYC Taxi data | Delta Lake, PySpark | [README](Databricks-Delta-Spark-Cache/README.md) |

---

## Key Technologies

- **Databricks** — Runtime 15.4, Unity Catalog, Delta Lake
- **Databricks Asset Bundles (DABs)** — Infrastructure-as-code for Databricks workflows
- **Apache Spark / PySpark** — Distributed data processing
- **Python 3.11** — Core language; packaged as wheels using Poetry and setuptools
- **PyTest** — Unit and integration testing
- **Azure Data Lake Storage Gen2** — Cloud storage backend
- **GitHub Actions / Azure DevOps** — CI/CD pipeline templates
- **Terraform** — Infrastructure provisioning

---