# To run the files
Run the scrape file to your hearts content. The longest it runs the better. It will generate around 5GB of data per day.

``` sh
python src/scrape.py
```

Then transform your data using parquet with

``` sh
python src/consolidate.py
```

# To copy the files into the S3 docker container

You can copy the files to S3 docker mock using the script provided

``` sh
sh stage_parquets.sh
```
You can check for the files you uploaded with the `ls` command in AWS.
``` sh
aws s3 ls gecko/anime/ --endpoint-url=http://localhost:9090
```
