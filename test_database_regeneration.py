from database_regeneration import recreate_database, reprocess_data, get_results_from_test_query

import psycopg2
import pytest

@pytest.fixture
def cur():
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    yield conn.cursor()
    conn.close()
        
class TestRegeneration:
    @classmethod
    def setup_class(self):
        recreate_database()
        reprocess_data()
        
        
    def check_len_greater_zero(self, results):
        assert(len(results) >  0)

    def test_artists_with_vars(self, cur):
        self.check_len_greater_zero(
            get_results_from_test_query(cur,
                                        """select * from artists WHERE artists.name=%s""",
                                        vars=('Casual',)))

    def test_users_table(self, cur):
        self.check_len_greater_zero(
            get_results_from_test_query(cur,
                                        """select * from users"""))

    def test_songs_table(self, cur):
        self.check_len_greater_zero(
            get_results_from_test_query(cur,
                                        """select * from songs"""))

    def test_artists_table(self, cur):
        self.check_len_greater_zero(
            get_results_from_test_query(cur,
                                        'select * from artists'))

    def test_time_table(self, cur):
        self.check_len_greater_zero(
            get_results_from_test_query(cur,
                                        'select * from time'))

    def test_songplays_table(self, cur):
        self.check_len_greater_zero(
            get_results_from_test_query(cur,
                                        'select * from songplays'))