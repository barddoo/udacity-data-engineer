### Song Play Analysis

Project to populate redshift cluster.

### Schema
- Fact Table
  1. songplays - records in event data associated with song plays i.e. records with page NextSong
    - songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

- Dimension Tables
  1. users - users in the app
    - user_id, first_name, last_name, gender, level
  2. songs - songs in music database
    - song_id, title, artist_id, year, duration
  3. artists - artists in music database
    - artist_id, name, location, lattitude, longitude
  4. time - timestamps of records in songplays broken down into specific units
    - start_time, hour, day, week, month, year, weekday

### Run
`python create_tables.py && python etl.py`
