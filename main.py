from dask.dataframe import read_csv
from time import time


def print_desired_summaries(triplets_sample, tracks_summary):
    track_plays_summary = triplets_sample \
        .groupby('track_id') \
        .agg({'track_id': 'count'}) \
        .rename(columns={'track_id': 'plays'}) \
        .reset_index() \
        .merge(tracks_summary, how='left', on='track_id') \

    most_popular_tracks = track_plays_summary.nlargest(5, 'plays').compute()
    print(f"Five the most popular tracks:\n{most_popular_tracks[['title', 'plays']]}\n")

    most_popular_artist = track_plays_summary \
        .groupby('artist') \
        .agg({'plays': 'sum'}) \
        .reset_index() \
        .nlargest(1, 'plays') \
        .compute()
    print(f'The most popular artist:\n{most_popular_artist}')


def load_file_to_data_frame(file_name, column_names, encoding='utf-8'):
    data_frame = read_csv(file_name, sep=r'<SEP>',
                          encoding=encoding, engine='python')
    data_frame = data_frame.rename(columns=dict(
        zip(data_frame.columns, column_names)))
    print(f'{file_name}:\n{data_frame.head(5)}\n')
    return data_frame


def main():
    processing_start_time = time()

    triplets_sample = load_file_to_data_frame(
        "triplets_sample_20p.txt", ['user_id', 'track_id', 'date'])

    unique_tracks = load_file_to_data_frame(
        "unique_tracks.txt", ['version_id', 'track_id', 'artist', 'title'], 'iso-8859-1')

    tracks_summary = unique_tracks[['track_id', 'artist', 'title']] \
        .drop_duplicates() \
        .compute()

    print_desired_summaries(triplets_sample, tracks_summary)

    insert_dataframe_into_db(tracks_summary)
    print(f"Procesing time: {time()-processing_start_time} seconds")


if __name__ == '__main__':
    main()
