# Azure Data Engineering ETL Project

## Project Overview
This project demonstrates an end-to-end ETL (Extract, Transform, Load) pipeline using Azure services. The pipeline extracts raw data from a GitHub repository, processes it using Azure Data Factory, transforms it in Azure Databricks using PySpark, and finally loads it into Azure Synapse Analytics for visualization in Power BI.

## Architecture and Workflow
1. Data Storage in Azure Blob Storage
   - Created an Azure Storage Account with a container.
   - Two types of data storage:
     - Raw Data: Extracted from GitHub.
     - Transformed Data: Processed and stored after transformations.

2. Azure Data Factory (ADF) Pipeline
   - Opened Azure Data Factory and created a pipeline.
   - Used the Move & Transform tool to copy data.
   - Configured the source as the raw data link from GitHub.
   - Configured the sink as Azure Data Lake Storage Gen2.
   - Connected all 5 tables and validated the pipeline execution.
   - Verified if tables were correctly stored in Azure Data Lake Gen2.

3. Azure Databricks for Data Transformation
   - Created a Databricks Compute cluster.
   - Connected Azure Data Lake Gen2 to Databricks:
     - Registered an application in Azure AD.
     - Generated an Application Key and Directory Key.
     - Created a Client Secret in "Certificates & Secrets".
   - Mounted Azure Data Lake Gen2 in Databricks.
   - Used PySpark for data transformation and manipulation.
   - Wrote the transformed data back to the storage container.

4. Azure Synapse Analytics for SQL Queries
   - Connected Synapse to Azure Data Lake.
   - Created a table from transformed data stored in Azure Data Lake.
   - Used SQL Scripts to perform further data manipulations.
   - Enabled visualization on SQL queries.

5. Power BI for Data Visualization
   - Connected Power BI to Azure Synapse Analytics.
   - Created an interactive dashboard to visualize insights from the transformed data.

## Technologies Used
- Azure Blob Storage: Data storage (raw and transformed data).
- Azure Data Factory: Data ingestion and ETL pipeline.
- Azure Databricks: Data transformation using PySpark.
- Azure Synapse Analytics: Data warehousing and SQL-based analysis.
- Power BI: Data visualization and reporting.
- GitHub: Raw data source and project documentation.

## Conclusion
This project successfully implements a full-fledged ETL process using Azure services, ensuring data is extracted, transformed, and loaded efficiently into a data warehouse for analytics and reporting. The integration with Power BI enables intuitive data visualizations for decision-making.

