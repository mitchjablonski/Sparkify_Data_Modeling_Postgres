import create_tables
from etl import process_data, process_song_file, process_log_file
import psycopg2

def recreate_database():
    cur, conn = create_tables.create_database()
    create_tables.drop_tables(cur, conn)
    create_tables.create_tables(cur, conn)
    conn.close()

def reprocess_data():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)
    conn.close()

def get_results_from_test_query(cur, query, **kwargs):
    cur.execute(query, **kwargs)
    results = cur.fetchall()
    return results

def run_test_query_and_print_first_result(cur, query, table_name, **kwargs):
    results = get_results_from_test_query(cur, query, **kwargs)
    first_result = results[0]
    print(f'Results from table {table_name} : {first_result}')
    
def check_creation():
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
    recreate_database()
    reprocess_data()
    check_creation()

if __name__ == '__main__':
    _main()