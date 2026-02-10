# We start from the Docker image of Spark (the one packaged by Bitnami, including Python3), so to have access to spark-submit
FROM bitnamilegacy/spark:3.5.1

# Create /opt/bitnami/spark/.ivy2 (used to store extra JARs) and make it a volume writable by everyone, so --packages will work
# USER root
# RUN mkdir /opt/bitnami/spark/.ivy2 && \
#     chmod a+rwx /opt/bitnami/spark/.ivy2
VOLUME /opt/bitnami/spark/.ivy2

# Install dependencies (done first to reuse Docker cache), then copy Python source files
WORKDIR /app
COPY requirements.txt .
RUN pip install -U pip && \
    pip install -r requirements.txt


# We submit the Spark application on a configurable Spark cluster
# The cluster can be created using this very same image (as it is based on the Bitnami Spark one).
# The default property values are set assuming the container is run with --network=host, so all required
# services are available on localhost at usual ports
USER 1001
COPY . .
CMD spark-submit \
    --master ${PROCESSOR_MASTER:-spark://localhost:7077} \
    # --jars /opt/spark-extra-jars/spark-sql-kafka-0-10_2.12-3.5.1.jar,/opt/spark-extra-jars/spark-token-provider-kafka-0-10_2.12-3.5.1.jar,/opt/spark-extra-jars/kafka-clients-3.4.1.jar,/opt/spark-extra-jars/postgresql-42.7.3.jar \
    --conf spark.driver.extraJavaOptions="-Divy.home=/tmp -Divy.cache.dir=/tmp" \
    --packages ${PROCESSOR_PACKAGES:-org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.postgresql:postgresql:42.7.3} \
    --py-files `echo *.py | tr ' ' ','` \
    ${PROCESSOR_IMPLEMENTATION:-processor.py} \
    # /app/processor.py \
    $PROCESSOR_ARGS
    