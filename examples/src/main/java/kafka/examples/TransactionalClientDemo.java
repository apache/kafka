package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;

import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;


import static java.time.Duration.ofSeconds;
import static java.util.Collections.singleton;


/**
 * This class demonstrates a transactional Kafka client application that consumes messages from an input topic,
 * processes them to generate word count statistics, and produces the results to an output topic.
 * It utilizes Kafka's transactional capabilities to ensure exactly-once processing semantics.
 *
 * The application continuously polls for records from the input topic, processes them, and commits the offsets
 * in a transactional manner. In case of exceptions or errors, it handles them appropriately, either aborting the
 * transaction and resetting to the last committed positions, or restarting the application.
 *
 */
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
        while (isRunning) {
            try {
                try {
                    // Poll records from Kafka for a timeout of 60 seconds
                    ConsumerRecords<String, String> records = consumer.poll(ofSeconds(60));

                    // Process records to generate word count map
                    Map<String, Integer> wordCountMap = new HashMap<>();

                    for (ConsumerRecord<String, String> record : records) {
                        String[] words = record.value().split(" ");
                        for (String word : words) {
                            wordCountMap.merge(word, 1, Integer::sum);
                        }
                    }

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
                    // Abortable Exception: Handle Kafka exception by aborting transaction. producer.abortTransaction() should not throw abortable exception.
                    producer.abortTransaction();
                    resetToLastCommittedPositions(consumer);
                }
            } catch (InvalidConfiguationTransactionException e) {
                //  Fatal Error: The error is bubbled up to the application layer. The application can decide what to do
                closeAll();
                throw e;
            } catch (KafkaException | ApplicationRecoverableTransactionException e) {
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