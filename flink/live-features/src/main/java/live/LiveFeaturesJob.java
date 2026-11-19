package live;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * Live zone features: Kafka trip-events -> per-zone, per-minute values.
 * Definitions: features/live.yaml.
 *
 *   trip-events --(1) match each trip's events by trip_id--> signals
 *               --(2) aggregate per zone|provider each minute--> ZoneMinute
 *               --(3a) latest value per key--> Redis (serving)
 *               --(3b) every minute, appended--> Iceberg (history)
 *
 * All time is event time (the replay clock), so the values are the same at
 * any replay speed.
 */
public class LiveFeaturesJob {

    public static void main(String[] args) throws Exception {
        String brokers = env("KAFKA_BROKERS", "events-kafka-bootstrap.kafka:9092");
        String topic = env("KAFKA_TOPIC", "trip-events");
        String redisHost = env("REDIS_HOST", "redis.redis");
        int redisPort = Integer.parseInt(env("REDIS_PORT", "6379"));
        String warehouse = env("ICEBERG_WAREHOUSE", "s3://nyc-taxi-raw-969788248726/lake");
        String tableLocation = env("ICEBERG_TABLE_LOCATION",
                "s3://nyc-taxi-raw-969788248726/lake/features/zone_minute_live");

        StreamExecutionEnvironment flink = StreamExecutionEnvironment.getExecutionEnvironment();

        // Without saved state the job rebuilds from the start of the topic:
        // open requests and recent waits are only correct with the history.
        // With state (restart, upgrade) it continues from its checkpoint.
        KafkaSource<TripEvent> source = KafkaSource.<TripEvent>builder()
                .setBootstrapServers(brokers)
                .setTopics(topic)
                .setGroupId("live-features")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new TripEventSchema())
                .build();

        // Watermarks are tracked per Kafka partition inside the source and the
        // slowest partition sets the clock, so a partition read ahead of the
        // others (catching up on history) does not make the rest look late.
        // Within a partition events come in event-time order; 5 s of slack.
        // A partition with no data for a minute does not hold the clock back.
        WatermarkStrategy<TripEvent> watermarks = WatermarkStrategy
                .<TripEvent>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((e, ts) -> e.eventTimeMs)
                .withIdleness(Duration.ofMinutes(1));

        // Fixed uids: checkpointed state is matched to operators by uid, so a
        // later change to the job graph (a new sink, a new step) can still
        // restore from the last checkpoint. Without them Flink derives ids
        // from the graph's shape and a changed graph cannot restore.
        DataStream<TripEvent> events = flink.fromSource(source, watermarks, "trip-events")
                .uid("trip-events-source");

        DataStream<ZoneMinute> minutes = events
                .keyBy(e -> e.tripId)
                .process(new TripMatcher()).uid("trip-matcher").name("match trip events")
                .keyBy(Signal::zoneKey)
                .process(new ZoneAggregator()).uid("zone-aggregator").name("zone minute values");

        // One computation, two sinks reading the same records.
        minutes.sinkTo(new RedisSink(redisHost, redisPort)).uid("redis-sink").name("redis");

        var catalog = MinuteLog.catalog(warehouse);
        MinuteLog.ensureTable(catalog, tableLocation);
        MinuteLog.sinkTo(minutes, catalog);

        flink.execute("live-features");
    }

    private static String env(String name, String fallback) {
        String v = System.getenv(name);
        return v == null || v.isEmpty() ? fallback : v;
    }
}
