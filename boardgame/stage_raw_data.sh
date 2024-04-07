aws s3api create-bucket --bucket gecko --endpoint-url=http://localhost:9090
aws s3 cp raw_data/collection/ s3://gecko/boardgame --endpoint-url=http://localhost:9090 --recursive
