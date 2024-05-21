# To execute the script

```sh
python src/get_data.py
```

It will populate the directory `raw_data`.

# To copy the files into the S3 docker container

(It is necessary to have the local container running before executing this. Refer to `../README.md` for more information.)

You can copy the files to S3 docker mock using

``` sh
bash src/stage_raw_data.sh
```

To check that everything was uploaded:

``` sh
aws s3 ls gecko/boardgame/ --endpoint-url=http://localhost:9090
```
