FROM jupyter/pyspark-notebook

RUN pip install --no-cache-dir boto3
RUN pip install --no-cache-dir pyspark
RUN pip install --no-cache-dir yake

USER root

RUN curl -o /usr/local/spark-3.5.0-bin-hadoop3/jars/hadoop-aws-3.2.0.jar  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.2.0/hadoop-aws-3.2.0.jar
RUN curl -o /usr/local/spark-3.5.0-bin-hadoop3/jars/aws-java-sdk-bundle-1.11.375.jar https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.375/aws-java-sdk-bundle-1.11.375.jar



RUN echo -e '\nspark.hadoop.fs.s3a.access.key=test_key_id' >> /usr/local/spark-3.5.0-bin-hadoop3/python/test_coverage/conf/spark-defaults.conf
RUN echo -e '\nspark.hadoop.fs.s3a.secret.key=test_access_key' >> /usr/local/spark-3.5.0-bin-hadoop3/python/test_coverage/conf/spark-defaults.conf


USER jovyan

CMD ["jupyter", "lab", "--NotebookApp.token=''"]
