# Gecko
Generic Universal Recommender System

# Loading to S3 
Since we are only mocking the connection to S3, we are not using AWS cloud. Instead we use a docker to mock the behaviour. We use adobe implemention of the mock. More information can be found here: https://github.com/adobe/S3Mock
To run the docker container, run in the root folder of the project 

```sh
docker compose up -d
```
This will initialize the docker container for S3.

To interact with S3 you need to have the AWS CLI. This can be downloaded and installed following the steps in https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html 

The details on the usage of this tool are explained in the README inside `/anime/` and `/boardgames/`
