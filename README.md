# OLIST-Data-Engineering-AWS
## Architecture Diagram
Findings:
```raw
    1. Costly to run Glue Jobs.
    2. Better to use pyspark locally/ using google colab or other third party services
    3. Since Redshift and Quicksight incurs some billing amount, a flask server is built to read data from the
    presentation layer and then the frontend can be built in React.JS to show the visualizations properly.
    4. For better support, use an EC2 instance that would run the flask server. In the free tier, running an
    EC2 instance is free for 750 hours a month.
```
### Cost inducing solution (Do not use AWS Glue, Athena, Redshift and Quicksight if billing is a concern)
![AWS Data Pipeline](AWS_project_architecture.png)


### Almost zero cost alternative 💯 🚀 !!!

Notes:
```raw
    1. Running the EC2 instance is also not required. One can do this on local as well.
    2. This will work pretty well for smaller datasets. For larger datasets (Enterprise level) this architecture
    might not work. For large datasets, one would ofcourse use a data warehousing solution with AWS Glue/ Databricks/
    AWS lambda (for medium sized data) as their ETL solution.
    3. Building the project this way gave me some knowledge 🧠 on building Flask Servers + React for simple
    dashboarding.
```  
![Zero Cost Pipeline](Almost_Zero_Cost_Solution_to_Dashboarding.png)
