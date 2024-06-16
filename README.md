# Fetch Rewards Coding Exercise - Analytics Engineer

## First: Review Existing Unstructured Data and Diagram a New Structured Relational Data Model
I started by reviewing the provided JSON files for receipts, users, and brands. Based on the data I went with 
the approach of having the receipts data as the fact table with the different dimensions being users, brands and
exploding the 'rewardsReceiptItemList' field to create a receipt_items table.

To make the data model production-ready, I created a data processing pipeline consisting of the following key components:
- [data_cleaner.py](data_processing%2Fdata_cleaner.py): Cleans and preprocesses raw JSON data.
- [data_reader.py](data_processing%2Fdata_reader.py): Reads JSON files into pandas DataFrames.
- [data_uploader.py](data_processing%2Fdata_uploader.py): Uploads cleaned data into PostgreSQL.
- [main.py](data_processing%2Fmain.py): Orchestrates the entire data processing workflow.

The data model is captured in the following diagram: 

![ERD.png](diagrams%2FERD.png)

- Users Table: user_id (PK)
- Brands Table: brand_id (PK)
- Receipts Table: receipt_id (PK), user_id (FK)
- Receipt_Items Table: receipt_id (PK), partner_item_id (PK), brand_code (FK)

## Second: Write queries that directly answer predetermined questions from a business stakeholder
I choose Postgres as the database. The queries can be found in the 'sql' folder.

Although I have included the queries for all the questions but all of them are not expected to give
the relevant results as the data provided is not clean and has pending issues.

The queries are as follows:
- What are the top 5 brands by receipts scanned for most recent month? 
  - [recent_month_top_5.sql](sql%2Frecent_month_top_5.sql): 
Given the issue with the brandCode field having multiple brands in the brands table, this query will not be able to 
give proper results.
- How does the ranking of the top 5 brands by receipts scanned for the recent month compare to the ranking for the previous month?
  - [recent_vs_previous_month_top_5.sql](sql%2Frecent_vs_previous_month_top_5.sql): 
Same as the previous query this query as well utilizes the brand table therefore the results will not be accurate.
- When considering average spend from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
    - [average_spend_accepted_vs_rejected.sql](sql%2Faverage_spend_accepted_vs_rejected.sql): 
This query will give the average spend for receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’ assuming 
'Finished' as the equivalent of 'Accepted'.
- When considering total number of items purchased from receipts with 'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’, which is greater?
    - [total_items_purchased_accepted_vs_rejected.sql](sql%2Ftotal_items_purchased_accepted_vs_rejected.sql): 
With the same assumption as above this query will give the total number of items purchased from receipts with 
'rewardsReceiptStatus’ of ‘Accepted’ or ‘Rejected’.
- Which brand has the most spend among users who were created within the past 6 months?
  - [brand_most_spend_6_months.sql](sql%2Fbrand_most_spend_6_months.sql):
This query utilizes the brand table and users table which both have duplication
therefore the results will not be accurate.
- Which brand has the most transactions among users who were created within the past 6 months?
  - [brand_most_transactions_6_months.sql](sql%2Fbrand_most_transactions_6_months.sql):
Same as the previous query this query as well utilizes the brand table and the users table therefore the results will not be accurate.



## Third: Evaluate Data Quality Issues in the Data Provided

Data Quality Issues:
- Users Data:
    - Multiple user_id values are duplicated and has different data for the same user_id's.
    - According to the documentation the user_role field should have a default value of 'consumer' but there are some rows where the value
contains 'fetch-staff'. These could be testing id's or some other issue that needs to be addressed.
- Brands Data:
    - The brandCode field contains a lot of null values, given the current data model as it acts as a join key for items to brand this field is important and should not have null values.
    - The brandCode field contains duplicate records for the same brand name(GOODNITES, HUGGIES), although the occurrences are only 1, on scale it could be a data quality issue.
    - The barcode field contains duplicate records, this could be an issue as the barcode should be unique for each brand.
- Receipts Data:
  - The purchaseDate field is prior to the createDate field for some receipts, this is an anomaly as the purchaseDate should ideally be greater than the createDate.

## Fourth: Communicate with Stakeholders
The communication with stakeholders is in the 'communication' folder: [stakeholder_communication](communication%2Fstakeholder_communication)

### Future Improvements:
- Indexing: For deployment, indexing should be done on the fields that are expected to be frequently used like user_id, brand_id, receipt_id, partner_item_id 
and columns used in joins and filters like brand_code.
- Data Checks: Given the unstructured data source, checks like handling missing values, correcting data types, and ensuring data consistency, deduplication, etc.
need to be implemented in the data pipeline to clean the data before loading it into the database.
- Partitioning: It can be expected that the receipts table and items table will grow significantly over time, 
therefore partitioning should be done on appropriate date field to improve query performance. For example, Fact_Receipts can be partitioned by create_date.
- Materialized Views: Materialized views can be created for the queries that are expected to be run frequently to improve accessibility while managing load on the database.
For example, a view can be created for the top brands by receipts scanned for the most recent and previous month.