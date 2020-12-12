import dask.dataframe as dd
from time import time

def print_desired_summaries(triplets_sample, tracks_summary):
    track_plays_summary = triplets_sample \
        .groupby('track_id') \
        .agg({'track_id':'count'}) \
        .rename(columns={'track_id':'plays'}) \
        .reset_index() \
        .merge(tracks_summary, how='left', on='track_id') \
        
    most_popular_tracks = track_plays_summary.nlargest(5, 'plays').compute()
    print(f"Five the most popular tracks:\n{most_popular_tracks[['title', 'artist', 'plays']]}\n")

    most_popular_artist = track_plays_summary \
        .groupby('artist') \
        .agg({'plays':'sum'}) \
        .reset_index() \
        .nlargest(1, 'plays') \
        .compute()

    # most_popular_artist = triplets_sample \
    #     .merge(tracks_summary, how='left', on='track_id') \
    #     .groupby('artist') \
    #     .agg({'artist':'count'}) \
    #     .rename(columns={'artist':'plays'}) \
    #     .reset_index() \
    #     .nlargest(1, 'plays') \
    #     .compute()

    print(f'The most popular artist:\n{most_popular_artist}')

def load_unique_tracks():
    unique_tracks = dd.read_csv('unique_tracks.txt', sep=r'<SEP>', encoding='iso-8859-1', engine='python')
    new_columns = ['version_id', 'track_id', 'artist', 'title']
    unique_tracks = unique_tracks.rename(columns=dict(zip(unique_tracks.columns, new_columns)))
    print(f'unique_tracks:\n{unique_tracks.head(5)}\n')
    return unique_tracks

def load_triplets_sample():
    triplets_sample = dd.read_csv('triplets_sample_20p.txt', sep=r'<SEP>', engine='python')
    new_columns = ['user_id', 'track_id', 'date']
    triplets_sample = triplets_sample.rename(columns=dict(zip(triplets_sample.columns, new_columns)))
    print(f'triplets_sample:\n{triplets_sample.head(5)}\n')
    return triplets_sample

def main():
    processing_start_time = time()
    triplets_sample = load_triplets_sample()
    unique_tracks = load_unique_tracks()
    tracks_summary = unique_tracks[['track_id', 'artist', 'title']].drop_duplicates().compute()
    print_desired_summaries(triplets_sample, tracks_summary)

    print(f"Procesing time: {time()-processing_start_time} seconds")

if __name__ == '__main__':
    main()