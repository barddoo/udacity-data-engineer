# Project: Data Warehouse

## Purpose
A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As their data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. I will be able to test the database and ETL pipeline by running queries given to me by the analytics team from Sparkify and compare the results with their expected results.

## Project Summary
In this project, I have applied what I learned on data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. This involves loading data from S3 to staging tables on Redshift and executing SQL statements that create the analytics tables from these staging tables.

## Project Datasets
Below are the S3 links used for the song and log datasets:

- Log data: `s3://dend/log_data`
- Song data: `s3://dend/song_data`

### Project files
Below are all the files.

- **create_table.py**: Contains code to create the fact and dimension tables for the star schema on Redshift.
- **etl.py**: Here, we load data from the S3 bucket into staging tables on Redshift and then process(load) it into analytics tables on Redshift.
- **dwh.cfg**: This is where we define the credentials needed for connection to the Redshift database (cluster), IAM role and S3 bucket.
- **sql_queries.py**: SQL staements defined here are imported in the files above for execution.
- **README.md**: This is the file you are reading right now and it discusses the processes and decisions for my ETL pipeline(summary of the project, how to run the Python scripts, and an explanation of the files in the repository).

## How to run the Python scripts
The scripts that need to be run on the terminal are `create_table.py` and `etl.py`.

Note!!! Replace the fields in the `dwh.cfg` file before running the commands below. 

- Run the command `python create_table.py` to create the Redshift database tables.
- Run the command `python etl.py` to load data from the S3 bucket into staging tables on Redshift, and then further load it into analytics tables on Redshift.

There are no errors reported on the terminal for either commands.

## Star Schema Design

### Fact Table
1. <b>songplays</b>: This table contains information on all the songs that have been played by at least one user in the database.

### Dimension Tables
2. <b>users</b>: This table contains information about users of the database. I used`user_id` as the primary key to ensure that each user is unique and `DISTINCT` during insertion to avoid dubplicate users.
3. <b>songs</b>: This table contains information about songs on the database that can be played by its users. I used the `song_id` column as primary key and `DISTINCT` during insertion to avoid dubplicates.
4. <b>artists</b>: This table contains information about the artists that sung the songs that are on the database. I designated the `artist_id` as the primary key and `DISTINCT` during insertion to avoid dubplicates.
5. <b>time</b>: This table contains information about times during which users of the database played songs. To aviod duplicates of the records, I introduced a `start_time` as the primary key.

### Staging Tables
These staging tables are on Redshift.
1. <b>staging_events</b>: The `staging_events` table is used to load information/data abouts events like a user playing a particular song. As seen in the code, the data is copied from S3 onto the staging table and then inserted into the final analytics tables following the relations of the different tables. During staging, I used `IGNOREHEADER 1` to ensure that the first row of the csv file in S3 is ignored since it holds the column names.
2. <b>staging_songs</b>: The `staging_songs` table is used to load data from the `songplays` fact table.I used `staging_songs.title = staging_events.song` to load data into the `songplays` analytics table as this indicates the event that a song has been played. I also used `format as json 'auto'` to indicate that the song files in the S3 bucket are in `.json` format.
