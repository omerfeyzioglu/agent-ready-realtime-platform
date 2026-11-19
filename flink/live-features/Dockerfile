# Live features job: plain Flink 2.1 image plus one job jar (Kafka
# connector, Redis client and JSON parser packed inside the jar).
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
COPY --from=build --chown=flink:flink /src/target/live-features-1.0.jar /opt/flink/usrlib/live-features.jar
