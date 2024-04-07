aws s3api create-bucket --bucket gecko --endpoint-url=http://localhost:9090
aws s3 cp parquets/ s3://gecko/anime --endpoint-url=http://localhost:9090 --recursive
