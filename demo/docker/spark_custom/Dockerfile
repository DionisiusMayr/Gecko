FROM jupyter/pyspark-notebook:spark-3.4.0

RUN pip install --no-cache-dir boto3
RUN pip install --no-cache-dir pyspark
RUN pip install --no-cache-dir yake

USER root
RUN apt update
RUN apt install -y curl

RUN curl -o /usr/local/spark-3.4.0-bin-hadoop3/jars/hadoop-aws-3.4.0.jar  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.4.0/hadoop-aws-3.4.0.jar
RUN curl -o /usr/local/spark-3.4.0-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.599.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.599/aws-java-sdk-bundle-1.12.599.jar

RUN echo -e '\nspark.hadoop.fs.s3a.access.key=test_key_id' >> /usr/local/spark-3.4.0-bin-hadoop3/python/test_coverage/conf/spark-defaults.conf
RUN echo -e '\nspark.hadoop.fs.s3a.secret.key=test_access_key' >> /usr/local/spark-3.4.0-bin-hadoop3/python/test_coverage/conf/spark-defaults.conf

RUN $SPARK_HOME/bin/pyspark --packages org.neo4j:neo4j-connector-apache-spark_2.12:5.3.0_for_spark_3

USER jovyan
RUN pip install --no-cache-dir graphdatascience
CMD ["jupyter", "lab", "--NotebookApp.token=''"]
