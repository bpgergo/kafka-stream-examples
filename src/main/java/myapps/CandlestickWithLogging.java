package myapps;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.kstream.Materialized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class CandlestickWithLogging {

    private static final Logger logger = LoggerFactory.getLogger(CandlestickWithLogging.class);
    private static final DateTimeFormatter FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm").withZone(ZoneOffset.UTC);

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "candlestick-processor");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        logger.info("Starting Candlestick Processor application");

        KStream<String, String> inputStream = builder.stream("streams-candlestick-input");

        inputStream
                // Convert string price to double
                .mapValues(value -> {
                    try {
                        Double price = Double.parseDouble(value);
                        logger.debug("Parsed price: {}", price);
                        return price;
                    } catch (NumberFormatException e) {
                        logger.warn("Invalid price format: {}", value);
                        return null; // Filter out invalid values
                    }
                })
                .filter((key, value) -> value != null)

                // Group by a constant key to aggregate all prices together
                // If you want to group by stock symbol, use the message key instead
                .groupBy((key, value) -> "all", Grouped.with(Serdes.String(), Serdes.Double()))

                // Create 1-minute tumbling windows
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))

                // Aggregate into candlestick data
                .aggregate(
                        // Initializer
                        Candlestick::new,
                        // Aggregator
                        (key, price, candlestick) -> {
                            if (candlestick.open == null) {
                                // First price in the window
                                logger.debug("Initializing candlestick with price: {}", price);
                                candlestick.open = price;
                                candlestick.high = price;
                                candlestick.low = price;
                                candlestick.close = price;
                            } else {
                                // Update high and low
                                if (price > candlestick.high) {
                                    logger.debug("New high: {}", price);
                                    candlestick.high = price;
                                }
                                if (price < candlestick.low) {
                                    logger.debug("New low: {}", price);
                                    candlestick.low = price;
                                }
                                // Always update close with the latest price
                                candlestick.close = price;
                                logger.debug("Updated candlestick - O:{} H:{} L:{} C:{}",
                                        candlestick.open, candlestick.high, candlestick.low, candlestick.close);
                            }
                            return candlestick;
                        },

                        // State store materialization
                        Materialized.with(Serdes.String(), new CandlestickSerde())
                )

                // Convert windowed key to ISO datetime format and candlestick to CSV format
                .toStream()
                .map((windowedKey, candlestick) -> {
                    long windowStartMillis = windowedKey.window().start();
                    Instant windowStartInstant = Instant.ofEpochMilli(windowStartMillis);
                    String windowStart = windowStartInstant.atZone(ZoneOffset.UTC)
                            .format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm"));
                    String csvValue = String.format("%f,%f,%f,%f",
                            candlestick.open,
                            candlestick.high,
                            candlestick.low,
                            candlestick.close
                    );
                    logger.info("Emitting candlestick for window {}: {}", windowStart, csvValue);
                    return KeyValue.pair(windowStart, csvValue);
                })

                // Write to output topic
                .to("streams-candlestick-output");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // Add state change listener for logging
        streams.setStateListener((newState, oldState) -> {
            logger.info("State transition from {} to {}", oldState, newState);
        });

        // Add uncaught exception handler
        streams.setUncaughtExceptionHandler((throwable) -> {
            logger.error("Uncaught exception in thread {}: {}", throwable.getMessage(), throwable);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });

        // Add shutdown hook for graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutting down Candlestick Processor");
            streams.close();
        }));

        logger.info("Starting Kafka Streams");
        streams.start();
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
            String str = String.format("%f,%f,%f,%f", data.open, data.high, data.low, data.close);
            return str.getBytes();
        }
    }

    static class CandlestickDeserializer implements org.apache.kafka.common.serialization.Deserializer<Candlestick> {
        @Override
        public Candlestick deserialize(String topic, byte[] data) {
            if (data == null) return null;
            String str = new String(data);
            String[] parts = str.split(",");
            Candlestick candlestick = new Candlestick();
            candlestick.open = Double.parseDouble(parts[0]);
            candlestick.high = Double.parseDouble(parts[1]);
            candlestick.low = Double.parseDouble(parts[2]);
            candlestick.close = Double.parseDouble(parts[3]);
            return candlestick;
        }
    }
}