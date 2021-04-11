# Project Data Lake with Spark

## Intro

*A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data
warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity.*

We built an ETL process that gets data from s3, using Spark framework deployed on AWS' EMR service, then load data back into s#.  

From these tables we will be able to find insights in what songs their users are listening to.

## How to run

*To run this project in local mode*, create a file `dl.cfg` in the root of this project with the following data:

```
[AWS]
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_KEY
```

Create an S3 Bucket named `sparkify-dend` where output results will be stored.

Run:

`python etl.py`

## Structure

The files found at this project are the following:

- etl.py: Program that extracts songs and log data from S3, transforms it using Spark.

## Pipeline

1. Load credentials
2. Read data from S3
    - Song data: `s3://udacity-dend/song_data`
    - Log data: `s3://udacity-dend/log_data`

   The script reads song_data and load_data from S3.

3. Process data using spark

   Transforms them to create five different tables.

4. Write data back to S3

   Writes data to partitioned files in table directories on S3. Each table has its own folder within the directory.
   Songs table files are partitioned by year and then artist. Time table files are partitioned by year and month.
   Songplays table files are partitioned by year and month.
