import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
    Function for processing out song files
    
    - Reads the file into a dataframe
    
    - Converts the dataframe values to a list for the song table
    
    - Performs an insert into the database for the song table
    
    - Converts the artist data values to a list for the artist table
    
    - Performs an insert into the table for the artist data.
    '''
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id', 'title', 'artist_id',
                         'year', 'duration']].values[0])
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = list(df[['artist_id', 'artist_name', 'artist_location', 
                           'artist_latitude', 'artist_longitude']].values[0])
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
    Primary function for processing log data
    
    - Reads the JSON file into pandas given the relevant file
    
    - Filters our data frame to only "NextSong" actions
    
    - Converts our ts to a datetime object
    
    - Generates a relevant list of time data, their columns, and pushes them into a dataframe
    
    - Inserts the time data
    
    - Gets relevant user information from dataframe, and pushes into user table
    
    - Queries the database for required information 
      for songplay table, then combines this with data 
      already in dataframe, and inserts it.
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df['page'] == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    
    # insert time data records
    time_data =[list(t.values), list(t.dt.hour.values), 
                list(t.dt.day.values), list(t.dt.weekofyear.values), 
                list(t.dt.month.values), list(t.dt.year.values), 
                list(t.dt.weekday.values)]
    column_labels = ['timestamp', 'hour', 'day', 'week of year', 'month', 'year', 'weekday']
    time_dict = {column_labels[i]: time_data[i] for i in range(len(column_labels))}
    time_df = pd.DataFrame(time_dict)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId', 'firstName', 'lastName', 'gender', 'level']]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit='ms'), 
                         row.userId, row.level, 
                         songid, artistid, row.sessionId,
                         row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
    Our primary method for processing data
    
    
    - Walks through our relevant directories, 
      looking for any files that are a JSON.  
      This will generate a list of files
    
    - Loops over the files, calling the relevant function 
      for the data type, and performing whatever actions there specified.
    '''
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
    The main function to handle ETL of the pipeline
    
    - Connects to our database source
    
    - Calls our process data function to target song data
    
    - Calls our process data to target log files
    
    - Closes our connection
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()