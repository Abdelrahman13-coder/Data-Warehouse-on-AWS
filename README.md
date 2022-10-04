
<img align="left" alt="codeSTACKr | songs" width="70px" src="https://user-images.githubusercontent.com/58150666/186937046-0ec8e86b-303b-4463-9a45-f4bc53880f46.png"/>

# Data-Warehouse-on-AWS

### **Project Overview**
> Building ETL pipeline that extracts data from S3, stages them in Redshift, and transform data into a set of dimensional tables for their analytics team.



### **In addition to the data files the project workspace includes four files:**

* `create_table.py` is where you'll create your fact and dimension tables for the star schema in Redshift.
* `etl.py` is where you'll load data from S3 into staging tables on Redshift and then process that data into your analytics tables on Redshift.
* `sql_queries.py` is where you'll define you SQL statements, which will be imported into the two other files above.
* `README.md` is where you'll provide discussion on your process and decisions for this ETL pipeline.

### **Staging tables**
`staging_songs`

| COLUMN | TYPE |
|--------|------|
| num_song | int |
| artist_id | varchar |
| artist_latitude | decimal |
| artist_longitude | decimal |
| artist_name | varchar |
| song_id | varchar |
| title | varchar | 
| duration | decimal |
| year | int |

`staging_events`
| COLUMN | TYPE |
|--------|------|
| artist | varchar|
| auth | varchar |
| firstName | varchar |
| gender | varchar |
| itemInSession | int |
| lastName | varchar |
| length | decimal |
| level | varchar |
| location | varchar |
| method | varchar |
| page | varchar |
| registeration | varchar |
| sessionId | int |
| song | varchar |
| status | int |
| ts | timestamp |
| userAgent | varchar |
| userId | int |

### **Star Schema**

![image](https://user-images.githubusercontent.com/58150666/193865741-237ebaa5-10fd-481d-a4dd-8ded6bf9818d.png)




