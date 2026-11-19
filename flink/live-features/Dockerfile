# Live features job: Flink 2.1 plus the Iceberg runtime, its AWS bundle
# (Glue catalog, S3 FileIO) and the Hadoop client classes Iceberg's Flink
# module references (same jars as the raw writer), and one job jar with the
# Kafka connector, Redis client and JSON parser packed inside.
ARG FLINK=2.1.2-scala_2.12-java17

# Stage 1: build the jar with Maven (no Java toolchain needed on the laptop).
# Runs on the build machine natively (the jar is the same on any CPU).
FROM --platform=$BUILDPLATFORM maven:3.9-eclipse-temurin-17 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -q -B dependency:go-offline
COPY src/ src/
RUN mvn -q -B package -DskipTests

# Stage 2: the runtime image.
FROM flink:${FLINK}
ARG MVN=https://repo1.maven.org/maven2
ADD --chown=flink:flink ${MVN}/org/apache/iceberg/iceberg-flink-runtime-2.1/1.11.0/iceberg-flink-runtime-2.1-1.11.0.jar /opt/flink/lib/
ADD --chown=flink:flink ${MVN}/org/apache/iceberg/iceberg-aws-bundle/1.11.0/iceberg-aws-bundle-1.11.0.jar /opt/flink/lib/
ADD --chown=flink:flink ${MVN}/org/apache/hadoop/hadoop-client-api/3.4.1/hadoop-client-api-3.4.1.jar /opt/flink/lib/
ADD --chown=flink:flink ${MVN}/org/apache/hadoop/hadoop-client-runtime/3.4.1/hadoop-client-runtime-3.4.1.jar /opt/flink/lib/
COPY --from=build --chown=flink:flink /src/target/live-features-1.0.jar /opt/flink/usrlib/live-features.jar
