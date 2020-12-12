from sqlite_connector import SQLiteConnector
from time import time


def print_desired_summaries(track_plays_summary, tracks_summary):
    track_plays_summary_ordered = iter(
        dict(sorted(track_plays_summary.items(), key=lambda item: -item[1])))

    print('Five the most popular tracks:\n')
    counter = 0
    while(counter < 5):
        track_id = next(track_plays_summary_ordered)
        if track_id in tracks_summary:
            print(f'{tracks_summary[track_id]} - plays {track_plays_summary[track_id]}')
            counter += 1
        else:
            print(f'Track ID {track_id} was not found in unique_tracks.txt!')

    plays_per_artist = dict()

    for track_id in iter(tracks_summary):
        if(track_id in track_plays_summary):
            track_plays = track_plays_summary[track_id]
            artist = tracks_summary[track_id][0]
            if artist in plays_per_artist:
                plays_per_artist[artist] += track_plays
            else:
                plays_per_artist[artist] = track_plays

    plays_per_artist_ordered = iter(dict(sorted(plays_per_artist.items(), key=lambda item: -item[1])))
    most_popular_artist = next(plays_per_artist_ordered)
    print(f'The most popular artist:\n {most_popular_artist}')


def process_triplets_sample(db_connector, insert_data_to_db):
    track_plays_summary = dict()

    with open("triplets_sample_20p.txt", 'r') as f:
        for line in f:
            data_tupple = line.replace('\n', '').split("<SEP>")
            if len(data_tupple) > 1:
                track_id = data_tupple[1]

                if track_id in track_plays_summary:
                    track_plays_summary[track_id] += 1
                else:
                    track_plays_summary[track_id] = 1

                if(insert_data_to_db):
                    db_connector.insert_data_to_triplets_sample(data_tupple)

    return track_plays_summary


def process_unique_tracks(db_connector, insert_data_to_db):
    tracks_summary = dict()

    with open('unique_tracks.txt', 'r', encoding='iso-8859-1') as f:
        for line in f:
            data_tupple = line.replace('\n', '').split("<SEP>")
            if len(data_tupple) > 3:
                track_id = data_tupple[1]
                artist_and_title = data_tupple[2:]
                if track_id not in tracks_summary:
                    tracks_summary[track_id] = artist_and_title
                if(insert_data_to_db):
                    db_connector.insert_data_to_unique_tracks(data_tupple)
            else:
                print(data_tupple)

    return tracks_summary


def main():
    insert_data_to_db = False
    db_connector = SQLiteConnector()
    processing_start_time = time()

    track_plays_summary = process_triplets_sample(db_connector, insert_data_to_db)
    track_plays_time = time()
    print(f'track_plays_time: {track_plays_time - processing_start_time} seconds')

    tracks_summary = process_unique_tracks(db_connector, insert_data_to_db)
    track_summary_time = time()
    print(f'track_summary_time: {track_summary_time - track_plays_time} seconds')

    print_desired_summaries(track_plays_summary, tracks_summary)

    print(f'Processing time: {time() - processing_start_time} seconds')


if __name__ == '__main__':
    main()
