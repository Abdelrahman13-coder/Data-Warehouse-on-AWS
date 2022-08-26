# Data-Warehouse-on-AWS
Building ETL pipeline that extracts data from S3, stages them in Redshift, and transform data into a set of dimensional tables for their analytics team.



### **In addition to the data files the project workspace includes four files:**

* `create_table.py` is where you'll create your fact and dimension tables for the star schema in Redshift.
* `etl.py` is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
* `sql_queries.py` is where you'll define you SQL statements, which will be imported into the two other files above.
* `README.md` is where you'll provide discussion on your process and decisions for this ETL pipeline.
