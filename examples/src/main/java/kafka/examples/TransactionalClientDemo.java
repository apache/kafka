package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.AbstractMap;


import static java.time.Duration.ofSeconds;
import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class TransactionalClientDemo {

    private static final String CONSUMER_GROUP_ID = "my-group-id";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";
    private static KafkaConsumer<String, String> consumer;
    private static KafkaProducer<String, String> producer;

    public static void main(String[] args) {
        initializeApplication();

        boolean isRunning = true;
        // Continuously poll for records
        while(isRunning) {
            try {
                try {
                    // Poll records from Kafka for a timeout of 60 seconds
                    ConsumerRecords<String, String> records = consumer.poll(ofSeconds(60));

                    // Process records to generate word count map
                    Map<String, Integer> wordCountMap = records.records(new TopicPartition(INPUT_TOPIC, 0))
                            .stream()
                            .flatMap(record -> Stream.of(record.value().split(" ")))
                            .map(word -> new AbstractMap.SimpleEntry<>(word, 1))
                            .collect(Collectors.toMap(
                                    AbstractMap.SimpleEntry::getKey,
                                    AbstractMap.SimpleEntry::getValue,
                                    (v1, v2) -> v1 + v2
                            ));

                    // Begin transaction
                    producer.beginTransaction();

                    // Produce word count results to output topic
                    wordCountMap.forEach((key, value) ->
                            producer.send(new ProducerRecord<>(OUTPUT_TOPIC, key, value.toString())));

                    // Determine offsets to commit
                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
                        long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
                        offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
                    }

                    // Send offsets to transaction for atomic commit
                    producer.sendOffsetsToTransaction(offsetsToCommit, CONSUMER_GROUP_ID);

                    // Commit transaction
                    producer.commitTransaction();
                } catch (AbortableTransactionException e) {
                    // Abortable Exception: Handle Kafka exception by aborting transaction. Abortable Exception should never be thrown.
                    producer.abortTransaction();
                    resetToLastCommittedPositions(consumer);
                }
            } catch (InvalidConfiguationTransactionException e) {
                //  Fatal Error: The error is bubbled up to the application layer. The application can decide what to do
                closeAll();
                throw InvalidConfiguationTransactionException;
            } catch (KafkaException e) {
                // Application Recoverable: The application must restart
                closeAll();
                initializeApplication();
            }
        }

    }

    public static void initializeApplication() {
        // Create Kafka consumer and producer
        consumer = createKafkaConsumer();
        producer = createKafkaProducer();

        // Initialize producer with transactions
        producer.initTransactions();
    }
    private static KafkaConsumer<String, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(GROUP_ID_CONFIG, CONSUMER_GROUP_ID);
        props.put(ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(singleton(INPUT_TOPIC));
        return consumer;
    }

    private static KafkaProducer<String, String> createKafkaProducer() {

        Properties props = new Properties();
        props.put(BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ENABLE_IDEMPOTENCE_CONFIG, "true");
        props.put(TRANSACTIONAL_ID_CONFIG, "prod-1");
        props.put(KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        return new KafkaProducer(props);

    }

    private static void resetToLastCommittedPositions(KafkaConsumer<String, String> consumer) {
        final Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
        consumer.assignment().forEach(tp -> {
            OffsetAndMetadata offsetAndMetadata = committed.get(tp);
            if (offsetAndMetadata != null)
                consumer.seek(tp, offsetAndMetadata.offset());
            else
                consumer.seekToBeginning(singleton(tp));
        });
    }

    private static void closeAll() {
        // Close Kafka consumer and producer
        consumer.close();
        producer.close();
    }

}