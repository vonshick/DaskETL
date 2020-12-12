from sqlite_connector import SQLiteConnector
from time import time

def process_triplets_sample(db_connector, insert_data_to_db):
    file_name = "triplets_sample_20p.txt"
    with open(file_name, 'r') as f:
        for _ in f:
            data_tupple = f.readline().replace('\n', '').split("<SEP>")
            if(insert_data_to_db):
                db_connector.insert_data_to_triplets_sample(data_tupple)

def process_unique_tracks(db_connector, insert_data_to_db):
    file_name = "unique_tracks.txt"
    with open(file_name, 'r', encoding='iso-8859-1') as f:
        for _ in f:
            data_tupple = f.readline().replace('\n', '').split("<SEP>")
            if(insert_data_to_db):
                db_connector.insert_data_to_unique_tracks(data_tupple)

def main():
    insert_data_to_db = True

    processing_start_time = time()
    db_connector = SQLiteConnector()
    process_triplets_sample(db_connector, insert_data_to_db)
    process_unique_tracks(db_connector, insert_data_to_db)

    finished_inserting = time()
    print(f'Inserting to database time: {finished_inserting - processing_start_time} seconds')

    db_connector.get_info()
    print(f'Processing time: {time() - finished_inserting} seconds')

if __name__=='__main__':
    main()