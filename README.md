# Gecko
Generic Universal Recommender System

# File Structure
- anime: has the scraper for anime
- movie: has the scraper for movie
- boardgame: has the scraper for boardgame
- videogame: has the scraper for videogame
- demo: has all the information related to demo
  - Inside it you will find multiple folders related to the Docker image persisted volumes

# Requirements
All the requirements to reproduce this project can either be found inside `requirements.txt` or the Docker images get them automatically when built.

# Running the demo
You just need to run the scrapers and compile the information inside `demo`.
Build the Docker image for spark (e.g. `docker build --network host -t spark_custom:0.1 ./demo/docker/spark_custom/`)
Boot up the images with `docker compose up` inside `demo` folder.
Then either follow the `demo/v_spark/spark.ipynb` notebook for an interactive and exploratory environment or check the equivalent production script `python demo/v_spark/spark.py`.

For the interface, something as simple as `streamlit run ./demo/interface.py` should suffice.
