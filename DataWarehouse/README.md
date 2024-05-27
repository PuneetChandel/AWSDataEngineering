
Data Warehouse Project for Udactiy Data Engineer Nanodegree. 
I completed in my personal AWS as the load data might have exhausted the budget limit of Udacity account.
Also had to restrict the buckets to s3://udacity-dend/song-data/A to be able to load the data

**Summary**
The project loads the raw data from S3 buckets to the staging tables and then from stagungb tables to fact and dimension tables.

**Project files**
1. create_table.py is where fact and dimension tables for the star schema in Redshift are created.
2. etl.py is where data gets loaded from S3 into staging tables on Redshift and then processed into the analytics tables on Redshift.
3. sql_queries.py where SQL statements are defined, which are then used by etl.py, create_table.py and analytics.py.
4. dwh.cfg configuration files for the project 
5. validate.py is a script to validate the data in the tables
6. requirements.txt is a file with the required libraries for the project
7. CreateCluster.ipynb is a jupyter notebook to create the Redshift cluster

**Database schema**
staging_events:	staging table for event data
staging_songs:	staging table for song data

songplays:	facts table, played songs and session information
users:	dimension, user-related 
songs:	dimension, song-related information
artists: dimension,	artist information
time:	dimension, time components


To-run
cd into the project directory and run the following commands
1. python3 -m venv venv
2. source venv/bin/activate
3. pip install -r requirements.txt
4. python CreateCluster.py
4. python create_tables.py
5. python etl.py
6. python validate.py

**Note**
Run Summary



