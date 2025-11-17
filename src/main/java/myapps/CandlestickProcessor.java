package myapps;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.state.WindowStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class CandlestickProcessor {

    private static final Logger logger = LoggerFactory.getLogger(CandlestickProcessor.class);

    // Topic names
    private static final String INPUT_TOPIC = getEnv("INPUT_TOPIC", "streams-candlestick-open-input2");
    private static final String OUTPUT_TOPIC = getEnv("OUTPUT_TOPIC", "streams-candlestick-open-output2");
    private static final String DLQ_TOPIC = getEnv("DLQ_TOPIC", "streams-candlestick-dlq");

    // Application configuration
    private static final String APPLICATION_ID = getEnv("APPLICATION_ID", "candlestick-processor");
    private static final String BOOTSTRAP_SERVERS = getEnv("BOOTSTRAP_SERVERS", "localhost:9092");
    private static final String STATE_DIR = getEnv("STATE_DIR", "/tmp/kafka-streams");

    public static void main(String[] args) {
        logger.info("Starting Candlestick Processor application");
        logger.info("Input topic: {}", INPUT_TOPIC);
        logger.info("Output topic: {}", OUTPUT_TOPIC);
        logger.info("Dead letter queue topic: {}", DLQ_TOPIC);

        Properties props = buildStreamProperties();
        logger.info("Stream will be built with custom props {}", props);

        StreamsBuilder builder = new StreamsBuilder();
        buildTopology(builder);

        KafkaStreams streams = new KafkaStreams(builder.build(), props);


        // Configure state listener for monitoring
        streams.setStateListener((newState, oldState) -> {
            logger.info("State transition from {} to {}", oldState, newState);
            if (newState == KafkaStreams.State.ERROR) {
                logger.error("Application entered ERROR state");
            }
        });

        // Configure exception handler for resilience
        streams.setUncaughtExceptionHandler((throwable) -> {
            logger.error("Uncaught exception in stream thread {}: {}",
                    throwable.getMessage(), throwable);
            // SHUTDOWN_APPLICATION ensures exactly-once semantics are preserved
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
        });

        // Setup graceful shutdown
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Received shutdown signal, closing streams gracefully");
            streams.close(Duration.ofSeconds(30));
            shutdownLatch.countDown();
            logger.info("Streams closed successfully");
        }));

        try {
            logger.info("Starting Kafka Streams topology");
            streams.start();
            shutdownLatch.await();
        } catch (InterruptedException e) {
            logger.error("Application interrupted", e);
            Thread.currentThread().interrupt();
        } finally {
            logger.info("Application shutdown complete");
        }
    }

    private static Properties buildStreamProperties() {
        Properties props = new Properties();

        // Basic configuration
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, APPLICATION_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.STATE_DIR_CONFIG, STATE_DIR);

        // Exactly-once semantics (EOS v2)
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);

        // Serialization
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        // Performance tuning
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG,
                getEnv("NUM_STREAM_THREADS", "2")); // Multiple threads per instance
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000"); // Commit every 30s
        // Transaction timeout must be higher than commit interval for EOS
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "60000"); // 60 seconds
        // deprecated
        //props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "10485760"); // 10MB cache

        // Rebalancing and fault tolerance:
        //props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,
        //        getEnv("REPLICATION_FACTOR", "3")); // Replicate state stores
        //props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "1"); // Hot standby for faster recovery
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG,
                getEnv("REPLICATION_FACTOR", "1")); // test with one node
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "0"); // test with one node


        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "10000"); // Accept 10k records lag

        // Consumer configuration
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"); // Process all data
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "1000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "300000"); // 5 minutes

        // Producer configuration for exactly-once
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);

        // Topology optimization
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);

        logger.info("Stream properties configured for production with exactly-once semantics");
        return props;
    }

    private static void buildTopology(StreamsBuilder builder) {
        // Main processing stream
        KStream<String, String> inputStream = builder.stream(INPUT_TOPIC,
                Consumed.with(Serdes.String(), Serdes.String())
                        .withTimestampExtractor(new FailOnInvalidTimestamp())); // Fail on invalid timestamps

        // Branch for valid and invalid records
        KStream<String, String> validRecords =
                inputStream.filter((key, value) -> value != null && !value.trim().isEmpty());

        KStream<String, String> invalidRecords =
                inputStream.filterNot((key, value) -> value != null && !value.trim().isEmpty());

        // Send invalid records to DLQ
        invalidRecords
                .peek((key, value) -> logger.warn("Invalid record sent to DLQ - key: {}, value: {}", key, value))
                .to(DLQ_TOPIC);

        // retention time config
        Map<String, String> changelogConfig = new HashMap<>();
        changelogConfig.put("retention.ms", "86400000"); // 24 hours
        // Process valid records
        validRecords
                .mapValues((key, value) -> {
                    try {
                        Double price = Double.parseDouble(value);
                        if (price <= 0) {
                            logger.warn("Non-positive price value: {}, sending to DLQ", price);
                            return null;
                        }
                        logger.debug("Parsed price: {}", price);
                        return price;
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid price format: {}, sending to DLQ", value);
                        return null;
                    }
                })
                .filter((key, value) -> value != null)

                // Group all prices together (or use key for per-symbol processing)
                .groupBy((key, value) -> "all",
                        Grouped.with(Serdes.String(), Serdes.Double()))

                // Create 1-minute tumbling windows with no grace period
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))

                // Aggregate into candlestick with named state store
                .aggregate(
                        Candlestick::new,
                        (key, price, candlestick) -> {
                            if (candlestick.open == null) {
                                logger.debug("Initializing candlestick with price: {}", price);
                                candlestick.open = price;
                                candlestick.high = price;
                                candlestick.low = price;
                                candlestick.close = price;
                            } else {
                                if (price > candlestick.high) {
                                    logger.debug("New high: {}", price);
                                    candlestick.high = price;
                                }
                                if (price < candlestick.low) {
                                    logger.debug("New low: {}", price);
                                    candlestick.low = price;
                                }
                                candlestick.close = price;
                                logger.debug("Updated candlestick - O:{} H:{} L:{} C:{}",
                                        candlestick.open, candlestick.high, candlestick.low, candlestick.close);
                            }
                            return candlestick;
                        },
                        Materialized.<String, Candlestick, WindowStore<Bytes, byte[]>>as("candlestick-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(new CandlestickSerde())
                                .withRetention(Duration.ofHours(24)) // Retain windows for 24 hours
                                .withLoggingEnabled(changelogConfig) // Enable changelog for fault tolerance
                )

                // Suppress intermediate results, emit only on window close
                .suppress(Suppressed.untilWindowCloses(Suppressed.BufferConfig.unbounded()
                        .shutDownWhenFull()))

                // Convert to output format
                .toStream()
                .map((windowedKey, candlestick) -> {
                    long windowStartMillis = windowedKey.window().start();
                    Instant windowStartInstant = Instant.ofEpochMilli(windowStartMillis);
                    String windowStart = windowStartInstant.atZone(ZoneOffset.UTC)
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"));
                    String csvValue = String.format("%.6f,%.6f,%.6f,%.6f",
                            candlestick.open,
                            candlestick.high,
                            candlestick.low,
                            candlestick.close
                    );
                    logger.info("Emitting candlestick for window {}: {}", windowStart, csvValue);
                    return KeyValue.pair(windowStart, csvValue);
                })
                .to(OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.String()));

        logger.info("Topology built successfully");
    }

    // Helper method to get environment variables with defaults
    private static String getEnv(String key, String defaultValue) {
        String value = System.getenv(key);
        return value != null ? value : defaultValue;
    }

    // Candlestick data class
    static class Candlestick {
        Double open;
        Double high;
        Double low;
        Double close;

        public Candlestick() {}
    }

    // Custom Serde for Candlestick
    static class CandlestickSerde extends Serdes.WrapperSerde<Candlestick> {
        public CandlestickSerde() {
            super(new CandlestickSerializer(), new CandlestickDeserializer());
        }
    }

    static class CandlestickSerializer implements org.apache.kafka.common.serialization.Serializer<Candlestick> {
        @Override
        public byte[] serialize(String topic, Candlestick data) {
            if (data == null) return null;
            String str = String.format("%.6f,%.6f,%.6f,%.6f",
                    data.open, data.high, data.low, data.close);
            return str.getBytes();
        }
    }

    static class CandlestickDeserializer implements org.apache.kafka.common.serialization.Deserializer<Candlestick> {
        @Override
        public Candlestick deserialize(String topic, byte[] data) {
            if (data == null) return null;
            try {
                String str = new String(data);
                String[] parts = str.split(",");
                if (parts.length != 4) {
                    logger.warn("Invalid candlestick format: {}", str);
                    return null;
                }
                Candlestick candlestick = new Candlestick();
                candlestick.open = Double.parseDouble(parts[0]);
                candlestick.high = Double.parseDouble(parts[1]);
                candlestick.low = Double.parseDouble(parts[2]);
                candlestick.close = Double.parseDouble(parts[3]);
                return candlestick;
            } catch (Exception e) {
                logger.error("Error deserializing candlestick", e);
                return null;
            }
        }
    }
}