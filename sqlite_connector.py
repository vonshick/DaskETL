from sqlite3 import connect
from datetime import datetime

class SQLiteConnector:
    def __init__(self):
        self._connection = connect('tracks.db')
        self._connection.execute(
            '''CREATE TABLE IF NOT EXISTS unique_tracks (
                performance_id text
                , track_id text
                , artist text
                , track text)''')
        self._connection.execute(
            '''CREATE TABLE IF NOT EXISTS triplets_sample (
                user_id text
                , track_id text
                , play_date text)''')
        self._connection.execute(
            '''CREATE TABLE IF NOT EXISTS error_logs (
                error_date text
                , table_issued text
                , error_message text)'''
        )

    def insert_data_to_triplets_sample(self, data_tupple):
        self.insert_data_to_db('triplets_sample', 3, data_tupple)

    def insert_data_to_unique_tracks(self, data_tupple):
        self.insert_data_to_db('unique_tracks', 4, data_tupple)

    def insert_data_to_db(self, table_name, expected_columns_number, data_tupple):
        if(len(data_tupple) != expected_columns_number):
            try:
                error_message = f'''Insert into {table_name} failed. 
                    {expected_columns_number} elements in the tupple 
                    expected, value found: {data_tupple}'''
                error_triple = (str(datetime.now), 'triplets_sample', error_message)
                error_query = 'INSERT INTO error_logs VALUES (?,?,?)'
                print(error_message)
                self._connection.execute(error_query, error_triple)
                self._connection.commit()
            except Exception as e:
                print(f'Writing logs to database failed. Error message:\n{e}')
        else:
            param_string = ','.join('?' * expected_columns_number)
            query = f'INSERT INTO {table_name} VALUES ({param_string})'
            try:
                self._connection.execute(query, data_tupple)
                self._connection.commit()
            except Exception as e:
                print(f'Writing to database failed. Error message:\n{e}')