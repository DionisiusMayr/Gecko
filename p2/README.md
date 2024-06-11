# Preparing the data for movies
To get the data from movies you need to cd into `p2`

``` sh
pip install tmdbsimple pyarrow fastparquet
python raw_movies.py
```

# Preparing the data for anime
No additional steps required
# Preparing the data for boardgames
The notebook `p2/spark.ipynb` makes the parsing from the original XML files.
