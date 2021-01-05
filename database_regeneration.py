import create_tables
from etl import process_data, process_song_file, process_log_file
import psycopg2

def recreate_database():
    '''
    Handles running the code normally managed by create tables.
    
    - Will Drop then create the database
    
    - Will then drop any of our tables that may exist.
    
    - Will then create our tables.
    '''
    cur, conn = create_tables.create_database()
    create_tables.drop_tables(cur, conn)
    create_tables.create_tables(cur, conn)
    conn.close()

def reprocess_data():
    '''
    Handles processing the data normally managed by ETL
    
    - Will get connection to our database
    
    - Will then find all relevant files for the song data, 
    load them into a pandas dataframe, 
    and push the data into our database
    
    - Will then find all relevant files for the log data,
    load each of the files into a pandas dataframe,
    and then push our log data into our database
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()

def get_results_from_test_query(cur, query, **kwargs):
    '''
    Given a cursor, a query, and any kwargs(likely vars for a variable query)
    
    - Will execute the query
    
    - Will fetch the results
    
    - Will return the results
    '''
    cur.execute(query, **kwargs)
    results = cur.fetchall()
    return results

def run_test_query_and_print_first_result(cur, query, table_name, **kwargs):
    '''
    Primary function for running test queries.
    
    Given a cursor, query, table name, and any kwargs,
    
    - Will utilize our get results function
    
    - Will grab the first result
    
    - Will print the result with the table name
    '''
    results = get_results_from_test_query(cur, query, **kwargs)
    first_result = results[0]
    print(f'Results from table {table_name} : {first_result}')
    
def check_creation():
    '''
    A function for checking the creation succeeded.
    
    This function is intended to be ran after the database is created and populated
    
    - Will connect to the database
    
    - Perform a series of queries against the database, utilizing the run test query and print
    
    - Expectation is, the user would monitor the prints to ensure data is returned.
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    run_test_query_and_print_first_result(cur,
                             """select * from artists WHERE artists.name=%s""", 
                             'artists', 
                             vars=('Casual',))
    run_test_query_and_print_first_result(cur, """select * from users""", 'users')
    run_test_query_and_print_first_result(cur, """select * from songs""", 'songs')
    run_test_query_and_print_first_result(cur, 'select * from artists', 'artists')
    run_test_query_and_print_first_result(cur, 'select * from time', 'time')
    run_test_query_and_print_first_result(cur, 'select * from songplays', 'songplays')
    conn.close()
    print('Database Regeneration Complete')

def _main():
    '''
    Main function to handle running the regeneration
    '''
    recreate_database()
    reprocess_data()
    check_creation()

if __name__ == '__main__':
    _main()