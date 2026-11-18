# Flink image for the SQL jobs: Flink 2.1 plus the Kafka connector, the
# Iceberg runtime with its AWS bundle (Glue catalog, S3 FileIO) and the
# Hadoop client classes Iceberg's Flink module still references.
ARG FLINK=2.1.2-scala_2.12-java17

# Stage 1: compile the SQL runner against Flink's own jars.
FROM flink:${FLINK} AS flink
FROM eclipse-temurin:17-jdk AS build
COPY --from=flink /opt/flink/lib /flink-lib
COPY src/SqlRunner.java /src/
RUN javac --release 17 -cp "/flink-lib/*" -d /classes /src/SqlRunner.java \
 && jar --create --file /sql-runner.jar --main-class SqlRunner -C /classes .

# Stage 2: the runtime image.
FROM flink:${FLINK}
ARG MVN=https://repo1.maven.org/maven2
ADD --chown=flink:flink ${MVN}/org/apache/flink/flink-sql-connector-kafka/5.0.0-2.1/flink-sql-connector-kafka-5.0.0-2.1.jar /opt/flink/lib/
ADD --chown=flink:flink ${MVN}/org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar /opt/flink/lib/
ADD --chown=flink:flink ${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/flink/lib/
ADD --chown=flink:flink ${MVN}/org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar /opt/flink/lib/
ADD --chown=flink:flink ${MVN}/org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar /opt/flink/lib/
COPY --from=build --chown=flink:flink /sql-runner.jar /opt/flink/usrlib/sql-runner.jar
COPY --chown=flink:flink sql/ /opt/flink/usrlib/sql/
