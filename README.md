# Purpose
*Discuss the purpose of this database in the context of the startup, Sparkify, and their analytical goals.*
As we know Sparkify wants to analyze the data they've been collecting on songs and user activity on their new music streaming app. 
Inorder to analyze the data better, they want to bring the user activity log data and the song datastored as semi-structure JSON files to a relational database structure.  having their song and log data on a relational database such as postgres would help them performa better analytics and generate reports. 

# Database schema design
The database design is based on star schema concept. songplays table is the FACT table which connects with the dimension tables songs, users, artists and time.

# ETL pipeline
The ETL pipeline implemented in the etl.py will make sure the data transformaiton from JSON file to Postgres tables become easier and makes it easy to debug as well. The dimension table data is ingested first and atlast the fact table data is ingested.

# Project Files
- *data* folder contain the semi structured data for songs and user logs
- sql_queries.py - contains the SQL queries for creating the RDBMS tables and also INSERT statements
- create_tables.py - this file reads the sql_queries.py and takes care of creating the database and tables. Use can invoke it by going to the terminal and use the command 'python create_tables.py'
- etl.ipynb - sample notebook to prepare the components of the ETL pipeline
- etl.py - The ETL pipeline to transform the JSON files present in the *data* folder to the RDBMS tables. Use can invoke it by going to the terminal and use the command 'python etl.py'
- test.ipynb - sample notebook to analyze the data in all the tables, especially analyzing the songplays table.

# Example queries and results for song play analysis.
- Analyze songs of a given artist
    SELECT * FROM songplays where artist_id = 'AR5KOSW1187FB35FF4'