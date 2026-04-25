package com.kafka.replication.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * CLI application that generates JSON events to the primary Kafka cluster's commit-log topic.
 *
 * Usage: java -jar commit-log-producer.jar --count N [--bootstrap-servers host:port] [--topic name]
 */
public class CommitLogProducer {

    private static final Logger log = LoggerFactory.getLogger(CommitLogProducer.class);

    private static final String DEFAULT_BOOTSTRAP_SERVERS = "primary-kafka:9092";
    private static final String DEFAULT_TOPIC = "commit-log";

    public static void main(String[] args) {
        int count = -1;
        String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
        String topic = DEFAULT_TOPIC;

        // Parse CLI arguments
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--count":
                    if (i + 1 < args.length) {
                        count = Integer.parseInt(args[++i]);
                    } else {
                        printUsageAndExit("--count requires a value");
                    }
                    break;
                case "--bootstrap-servers":
                    if (i + 1 < args.length) {
                        bootstrapServers = args[++i];
                    } else {
                        printUsageAndExit("--bootstrap-servers requires a value");
                    }
                    break;
                case "--topic":
                    if (i + 1 < args.length) {
                        topic = args[++i];
                    } else {
                        printUsageAndExit("--topic requires a value");
                    }
                    break;
                case "--help":
                    printUsageAndExit(null);
                    break;
                default:
                    printUsageAndExit("Unknown argument: " + args[i]);
            }
        }

        if (count < 0) {
            printUsageAndExit("--count is required");
        }

        log.info("Starting Commit Log Producer: count={}, bootstrapServers={}, topic={}",
                count, bootstrapServers, topic);

        Properties props = buildProducerProperties(bootstrapServers);

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            EventGenerator generator = new EventGenerator();
            int successCount = 0;

            for (int i = 0; i < count; i++) {
                String eventJson = generator.generateEvent();
                String eventKey = generator.getLastEventKey();

                ProducerRecord<String, String> record = new ProducerRecord<>(topic, eventKey, eventJson);

                try {
                    RecordMetadata metadata = producer.send(record).get();
                    successCount++;

                    if (successCount % 100 == 0 || successCount == count) {
                        log.info("Produced {}/{} messages. Latest offset: {}, partition: {}",
                                successCount, count, metadata.offset(), metadata.partition());
                    }
                } catch (ExecutionException e) {
                    log.error("Failed to produce message {}/{}: {}", i + 1, count, e.getMessage(), e);
                    throw new RuntimeException("Producer failed at message " + (i + 1), e);
                }
            }

            producer.flush();
            log.info("Successfully produced all {} messages to topic '{}'", successCount, topic);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Producer interrupted", e);
            System.exit(1);
        }
    }

    private static Properties buildProducerProperties(String bootstrapServers) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, 1000);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);
        return props;
    }

    private static void printUsageAndExit(String error) {
        if (error != null) {
            System.err.println("Error: " + error);
        }
        System.err.println("Usage: commit-log-producer --count N [--bootstrap-servers host:port] [--topic name]");
        System.err.println();
        System.err.println("Options:");
        System.err.println("  --count N                Number of messages to produce (required)");
        System.err.println("  --bootstrap-servers      Kafka bootstrap servers (default: primary-kafka:9092)");
        System.err.println("  --topic                  Target topic name (default: commit-log)");
        System.err.println("  --help                   Show this help message");
        System.exit(error != null ? 1 : 0);
    }
}
