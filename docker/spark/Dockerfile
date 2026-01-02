FROM eclipse-temurin:11-jre

# Versions
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# System Tools
RUN apt-get update && apt-get install -y \
    wget curl python3 python3-pip vim unzip \
    && rm -rf /var/lib/apt/lists/*

# Install Spark (Hadoop3 compatible build)
RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    && tar zxvf spark-${SPARK_VERSION}-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark \
    && rm spark-${SPARK_VERSION}-bin-hadoop3.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

# Install S3A support (Hadoop AWS + AWS SDK bundle)
RUN wget -q -P /opt/spark/jars \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
 && wget -q -P /opt/spark/jars \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.557/aws-java-sdk-bundle-1.12.557.jar

# Install PostgreSQL JDBC driver
RUN wget -q -P /opt/spark/jars \
    https://jdbc.postgresql.org/download/postgresql-42.6.0.jar

# install python + pyspark
RUN apt-get update && apt-get install -y python3-full
RUN python3 -m venv /opt/venv
RUN /opt/venv/bin/pip install --upgrade pip
RUN /opt/venv/bin/pip install pyspark==3.5.1
ENV PATH="/opt/venv/bin:$PATH"


# Workspace
WORKDIR /app
COPY . /app/

CMD ["sleep", "infinity"]