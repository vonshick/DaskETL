The data files (*triplets_sample_20p.txt* and *unique_tracks.txt*) need to be placed in the project root directory.<br>
To start the data processing and print desired information run the command below:

```{shell}
python main.py
```

File *dask_sandbox.py* contains the solution made with *dask* framework. 
Although *dask* was made for processing large files performance of that script was quite poor.
Data processing took about 350 seconds (above 11 times longer than with *main.py* script).