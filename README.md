# Gecko
Generic Universal Recommender System

# Setting up S3

Since we are only mocking the connection to S3, we are not using AWS cloud. Instead we use a docker to mock the behaviour. We use adobe implemention of the mock. More information can be found here: https://github.com/adobe/S3Mock
To run the docker container, run in the root folder of the project:

```sh
docker compose up -d
```
This will initialize the docker container for S3.

To interact with S3 you need to have the AWS CLI. This can be downloaded and installed following the steps in https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html

The details on the usage of this tool are explained in the README inside `/anime/` and `/boardgames/`

# HDFS

We considered using HDFS in this project and did a proof of concept of it.

In order to set it up, we are using the docker image provide by Gaffer, found [here](https://github.com/gchq/gaffer-docker/tree/develop/docker/hdfs).
Besides, it is also necessary to set up the volume accordingly in order to transfer data between the local machine and the HDFS container.

This can be done by adding an extra volume (like `- ${HOME}/bdma/upc/big_data_management/project/Gecko/hdfs/:/home/workspace/`) to `docker-compose.yaml`'s services.

Once this is configured, one can start the containers using `docker compose up` and access the HDFS interface through http://localhost:9870 or through the terminal.

## Interacting with HDFS

These commands can be used as a reference to interact with HDFS in this setup.

To get a `bash` instance of the container:
```sh
docker exec -it hdfs-namenode bash
```

(Now, from inside the container:)

```sh
hadoop fs -ls
hadoop fs -put ./1733.xml /
hadoop fs -rm /1733.xml
```

Here, `1733.xml` is an example of a file that is being put and removed from HDFS.
