# About the project

This repo contains third capstone project for **Udacity Data Engeneering Nanodegree** program (here is the project [frontpage](https://www.udacity.com/course/data-engineer-nanodegree--nd027)).\
The goal is to implement a DWH using `Amazon Redshift` service, and a ETL pipeline that extracts data from S3 bucket, and loads it into normalized (`3NF`) data model.

The project revolves around imaginary music streeming company - **Sparkify**.\
They store JSON logs of users activitiy in S3 bucket, along with metadata on songs played.\
We are to enable this data for easy analytical queries. This is achieved by implementing a ETL pipeline that loads the data into staging tables in a Redshift cluster, and transforms it into dimesional tables.

## Repo structure

**/functions** stores main project functions:
* `setup_aws.py` creates a instance of Redshift cluster,
* `create_tables.py` creates both staging and dimesional tables,
* `etl.py` copies the data from S3 into staging tables, and loads the data from staging into final data model.

**/confings**:
* contains the cluster and dwh specifications `aws.cfg`
* for proper execution this folder should also contain `iam.cfg` with credentials neccesary for cluster creation (see details in _Running this project_ section below).

**/samples**:
* contain samples of users activity log data `2018-11-01-events.json` and 
* song metadata `TRAAAAK128F9318786.json`
* `log_json_path.json` is the definition of log structure, allowing for proper copy from `S3`.

## Running this project

As mentioned above a IAM credentials are required for sucessful project execution. The config should contain KEY and SECRET under IAM section:
```
[IAM]
KEY = 
SECRET = 
```

The user needs policies that allow to handle Redshift cluster **AmazonRedshiftFullAccess**.\
Also a policy allowing to pass `S3` access to the cluster **AmazonS3ReadOnlyAccess** is required. Such policy can be defined as such:
```json
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Effect": "Allow",
            "Action": [
                "iam:GetRole",
                "iam:PassRole"
            ],
            "Resource": "arn:aws:iam::xxxxxxxxxxxx:role/Redshift_RO"
        }
    ]
}
```
Where `Resource` is a `arn` of your role providing `S3` access.\
More on IAM user and policies can be found [here](https://aws.amazon.com/iam/) and [here](https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html).

When `IAM.cfg` is provided, then the `test.ipynb` runs through all stages of the project:
1) first a Redshift cluster is created,
2) then a `TCP` port is opened allowing for access to the cluster endpoint,
3) finally `ETL` is run, creatning staging tables, copying the data form `S3` and populating analytical data model,
4) now analytical queries can be run,
5) when done with queries don't forget to cleanup the resources to avoid extra charges from Amazon.

# Redshift and ETL

## Architecture

## Data model

# Sample queries


### The database design

As the goal is to enable analysts easy and flexible way of quering the data a **relational database** is chosen.
Main relation will be `songplays` **facts** table.
Additional details about: 
* the song
* the artist
* the user

will be stored in seperate tables. Seperate table with time and data details will also be created.

Those tables will be arranged in **snowflake** logical architecture around `songplays` table.
This architecture will provide logical separation of different business entities, limit the data redundancy.
Each table will have a `primary key` allowing for easy and flexible `JOIN`.
Those properties make our database design achieve `3NF`.

## ETL

The `ETL` process can be separated into four stages:

1) Create empty target tables:
* `songs` 
* `artists`
* `users`
* `time`
* `songplays`


2) Populate `song` and `artist` tables with song metadata

3) Populate `user` and `time` tables with activity logs.

4) Finally populate `songplays`, basing on activity logs and with added song and user id from `song` and `user` tables.

## Running test ETL

ETL can be run with `test.ipynb` script. First a database is created `create_db.py`, then it is populated with empty tables `create_tables.py`.
Finally tables are populated with proper data `etl.py`.
SQLs for tables creation are stored in `sql_queries.py`.

## Sample queries

a) top 10 most played songs (with song title and artist name)

`
SELECT
    songs.title AS song,
    artists.name AS artist
FROM songplays 
    JOIN songs ON songplays.song_id = songs.song_id
    JOIN artists ON songplays.artist_id = artists.artist_id
GROUP BY 1, 2
ORDER BY count(*) DESC
LIMIT 10;
`

b) average number of songs played daily by all users

`
SELECT 
    min(songs_played) as min_songs_played,
    avg(songs_played)::int as average_songs_played,
    max(songs_played) as max_songs_played
FROM
(
    SELECT
        time.year,
        time.month,
        time.day,
        count(*) AS songs_played
    FROM songplays
        JOIN time ON songplays.start_time = time.start_time
    GROUP BY 1,2,3
) AS daily_plays
`


