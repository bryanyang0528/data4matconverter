FROM gettyimages/spark:2.3.0-hadoop-2.8

RUN apt update && apt install -y zip

ENV SPARK_VERSION=2.3.0
ENV SPARK_HOME=/usr/spark-${SPARK_VERSION}

RUN cd ${SPARK_HOME}/jars/ &&\
    curl -O https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-latest-hadoop2.jar

COPY conf/ ${SPARK_HOME}/conf/
COPY dist/ /app
WORKDIR /app

CMD ["/bin/bash"]
