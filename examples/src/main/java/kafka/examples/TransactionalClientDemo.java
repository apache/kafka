/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.ApplicationRecoverableException;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.TransactionAbortableException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static java.time.Duration.ofSeconds;
import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.ISOLATION_LEVEL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.TRANSACTIONAL_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;


/**
 * This class demonstrates a transactional Kafka client application that consumes messages from an input topic,
 * processes them to generate word count statistics, and produces the results to an output topic.
 * It utilizes Kafka's transactional capabilities to ensure exactly-once processing semantics.
 *
 * The application continuously polls for records from the input topic, processes them, and commits the offsets
 * in a transactional manner. In case of exceptions or errors, it handles them appropriately, either aborting the
 * transaction and resetting to the last committed positions, or restarting the application.
 * 
 * Follows KIP-1050 guidelines for consistent error handling in transactions. 
 * @see <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-1050">KIP-1050</a>
 */
public class TransactionalClientDemo {

    private static final String CONSUMER_GROUP_ID = "my-group-id";
    private static final String OUTPUT_TOPIC = "output";
    private static final String INPUT_TOPIC = "input";

    private static KafkaConsumer<String, String> consumer;
    private static KafkaProducer<String, String> producer;
    private static volatile boolean isRunning = true;

    public static void main(String[] args) {
        Utils.printOut("Starting TransactionalClientDemo");

        registerShutdownHook();

        initializeApplication();

        while (isRunning) {
            try {
                try {
                    Utils.printOut("Polling records from Kafka");
                    ConsumerRecords<String, String> records = consumer.poll(ofSeconds(60));

                    Utils.printOut("Polled %d records from input topic '%s'", records.count(), INPUT_TOPIC);
                    for (ConsumerRecord<String, String> record : records) {
                        Utils.printOut("Record: key='%s', value='%s', partition=%d, offset=%d", record.key(), record.value(), record.partition(), record.offset());
                    }

                    // Process records to generate word count map
                    Map<String, Integer> wordCountMap = new HashMap<>();
                    for (ConsumerRecord<String, String> record : records) {
                        String[] words = record.value().split(" ");
                        for (String word : words) {
                            wordCountMap.merge(word, 1, Integer::sum);
                        }
                    }

                    Utils.printOut("Word count map to be produced: %s", wordCountMap);

                    Utils.printOut("Beginning transaction");
                    producer.beginTransaction();

                    wordCountMap.forEach((key, value) ->
                        producer.send(new ProducerRecord<>(OUTPUT_TOPIC, key, value.toString())));
                    Utils.printOut("Produced %d word count records to output topic '%s'", wordCountMap.size(), OUTPUT_TOPIC);

                    Map<TopicPartition, OffsetAndMetadata> offsetsToCommit = new HashMap<>();
                    for (TopicPartition partition : records.partitions()) {
                        List<ConsumerRecord<String, String>> partitionedRecords = records.records(partition);
                        long offset = partitionedRecords.get(partitionedRecords.size() - 1).offset();
                        offsetsToCommit.put(partition, new OffsetAndMetadata(offset + 1));
                    }

                    producer.sendOffsetsToTransaction(offsetsToCommit, consumer.groupMetadata());
                    Utils.printOut("Sent offsets to transaction for commit");

                    producer.commitTransaction();
                    Utils.printOut("Transaction committed successfully");
                } catch (TransactionAbortableException e) {
                    // Abortable Exception: Handle Kafka exception by aborting transaction. producer.abortTransaction() should not throw abortable exception.
                    Utils.printErr("TransactionAbortableException: %s. Aborting transaction.", e.getMessage());
                    producer.abortTransaction();
                    Utils.printOut("Transaction aborted. Resetting consumer to last committed positions.");
                    resetToLastCommittedPositions(consumer);
                }
            } catch (InvalidConfigurationException e) {
                // Fatal Error: The error is bubbled up to the application layer. The application can decide what to do 
                Utils.printErr("InvalidConfigurationException: %s. Shutting down.", e.getMessage());
                closeAll();
                throw e;
            } catch (ApplicationRecoverableException e) {
                // Application Recoverable: The application must restart
                Utils.printErr("ApplicationRecoverableException: %s. Restarting application.", e.getMessage());
                closeAll();
                initializeApplication();
            } catch (KafkaException e) {
                // KafkaException should be treated as Application Recoverable. Applications can make custom changes to handle generic exceptions
                Utils.printErr("KafkaException: %s. Restarting application.", e.getMessage());
                closeAll();
                initializeApplication();
            } catch (Exception e) {
                Utils.printErr("Unhandled exception: %s", e.getMessage());
                closeAll();
                throw e;
            }
        }
    }

    public static void initializeApplication() {
        Utils.printOut("Initializing Kafka consumer and producer");
        consumer = createKafkaConsumer();
        producer = createKafkaProducer();
        
        producer.initTransactions();
        Utils.printOut("Producer initialized with transactions");
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

        return new KafkaProducer<>(props);
    }

    private static void resetToLastCommittedPositions(KafkaConsumer<String, String> consumer) {
        Utils.printOut("Resetting consumer to last committed positions");
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
        if (consumer != null) {
            consumer.close();
        }
        if (producer != null) {
            producer.close();
        }
        Utils.printOut("All resources closed");
    }

    private static void registerShutdownHook() {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            Utils.printOut("Shutdown signal received. Exiting...");
            isRunning = false;
            closeAll();
        }));
    }

}