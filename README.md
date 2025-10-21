# Welcome to the demo dbt project!

This project is full pipeline. I'm using FiveTran connector to ingest data files from Azure Blob storage to Databricks bronze layer. Then I'm using dbt core to run data transformations and load silver layer tables and aggregations at the gold layer.

### Using the starter project

Try running the following commands:
- dbt run
- dbt test


### Resources:
- Learn more about dbt [in the docs](https://docs.getdbt.com/docs/introduction)
- Check out [Discourse](https://discourse.getdbt.com/) for commonly asked questions and answers
- Join the [chat](https://community.getdbt.com/) on Slack for live discussions and support
- Find [dbt events](https://events.getdbt.com) near you
- Check out [the blog](https://blog.getdbt.com/) for the latest news on dbt's development and best practices
