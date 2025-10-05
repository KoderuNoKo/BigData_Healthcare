#!/usr/bin/env bash
set -e

echo "🔧 Updating packages..."
apt update -y && apt install -y wget openjdk-11-jdk

SPARK_VERSION="3.5.2"
SPARK_DIR="/opt/spark-${SPARK_VERSION}-bin-hadoop3"
SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop3.tgz"
SPARK_LINK="/opt/spark"

echo "📦 Checking Spark..."
if [ ! -d "$SPARK_DIR" ]; then
  echo "➡️  Installing Spark ${SPARK_VERSION}..."
  wget -nc https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}
  tar -xvf ${SPARK_TGZ} -C /opt/
  ln -sf ${SPARK_DIR} ${SPARK_LINK}
else
  echo "✅ Spark already installed."
fi

echo "⚙️  Installing Kafka dependencies..."
cd /opt/spark/jars

declare -A JARS=(
  ["spark-sql-kafka-0-10_2.12-3.5.2.jar"]="https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.2/spark-sql-kafka-0-10_2.12-3.5.2.jar"
  ["spark-token-provider-kafka-0-10_2.12-3.5.2.jar"]="https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.2/spark-token-provider-kafka-0-10_2.12-3.5.2.jar"
  ["kafka-clients-3.5.2.jar"]="https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.2/kafka-clients-3.5.2.jar"
  ["commons-pool2-2.11.1.jar"]="https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"
)

for jar in "${!JARS[@]}"; do
  if [ ! -f "$jar" ]; then
    echo "➡️  Downloading $jar..."
    wget -nc "${JARS[$jar]}"
  else
    echo "✅ $jar already exists."
  fi
done

echo "✨ Spark + Kafka setup complete!"