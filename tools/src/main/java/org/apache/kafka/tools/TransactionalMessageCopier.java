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
package org.apache.kafka.tools;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singleton;
import static net.sourceforge.argparse4j.impl.Arguments.store;
import static net.sourceforge.argparse4j.impl.Arguments.storeTrue;

/**
 * This class is primarily meant for use with system tests. It copies messages from an input partition to an output
 * topic transactionally, committing the offsets and messages together.
 */
public class TransactionalMessageCopier {

    /** Get the command-line argument parser. */
    private static ArgumentParser argParser() {
        ArgumentParser parser = ArgumentParsers
                .newArgumentParser("transactional-message-copier")
                .defaultHelp(true)
                .description("This tool copies messages transactionally from an input partition to an output topic, " +
                        "committing the consumed offsets along with the output messages");

        parser.addArgument("--input-topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("INPUT-TOPIC")
                .dest("inputTopic")
                .help("Consume messages from this topic");

        parser.addArgument("--input-partition")
                .action(store())
                .required(true)
                .type(Integer.class)
                .metavar("INPUT-PARTITION")
                .dest("inputPartition")
                .help("Consume messages from this partition of the input topic.");

        parser.addArgument("--output-topic")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("OUTPUT-TOPIC")
                .dest("outputTopic")
                .help("Produce messages to this topic");

        parser.addArgument("--broker-list")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help("Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

        parser.addArgument("--max-messages")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(Integer.class)
                .metavar("MAX-MESSAGES")
                .dest("maxMessages")
                .help("Process these many messages upto the end offset at the time this program was launched. If set to -1 " +
                        "we will just read to the end offset of the input partition (as of the time the program was launched).");

        parser.addArgument("--consumer-group")
                .action(store())
                .required(false)
                .setDefault(-1)
                .type(String.class)
                .metavar("CONSUMER-GROUP")
                .dest("consumerGroup")
                .help("The consumer group id to use for storing the consumer offsets.");

        parser.addArgument("--transaction-size")
                .action(store())
                .required(false)
                .setDefault(200)
                .type(Integer.class)
                .metavar("TRANSACTION-SIZE")
                .dest("messagesPerTransaction")
                .help("The number of messages to put in each transaction. Default is 200.");

        parser.addArgument("--transactional-id")
                .action(store())
                .required(true)
                .type(String.class)
                .metavar("TRANSACTIONAL-ID")
                .dest("transactionalId")
                .help("The transactionalId to assign to the producer");

        parser.addArgument("--enable-random-aborts")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("ENABLE-RANDOM-ABORTS")
                .dest("enableRandomAborts")
                .help("Whether or not to enable random transaction aborts (for system testing)");

        return parser;
    }

    private static KafkaProducer<String, String> createProducer(Namespace parsedArgs) {
        String transactionalId = parsedArgs.getString("transactionalId");
        String brokerList = parsedArgs.getString("brokerList");

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // We set a small batch size to ensure that we have multiple inflight requests per transaction.
        // If it is left at the default, each transaction will have only one batch per partition, hence not testing
        // the case with multiple inflights.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "512");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        // Multiple inflights means that when there are rolling bounces and other cluster instability, there is an
        // increased likelihood of having previously tried batch expire in the accumulator. This is a fatal error
        // for a transaction, causing the copier to exit. To work around this, we bump the request timeout.
        // We can get rid of this when KIP-91 is merged.
        props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "60000");

        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> createConsumer(Namespace parsedArgs) {
        String consumerGroup = parsedArgs.getString("consumerGroup");
        String brokerList = parsedArgs.getString("brokerList");
        Integer numMessagesPerTransaction = parsedArgs.getInt("messagesPerTransaction");

        Properties props = new Properties();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, numMessagesPerTransaction.toString());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props);
    }

    private static ProducerRecord<String, String> producerRecordFromConsumerRecord(String topic, ConsumerRecord<String, String> record) {
        return new ProducerRecord<>(topic, record.key(), record.value());
    }

    private static Map<TopicPartition, OffsetAndMetadata> consumerPositions(KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            positions.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
        }
        return positions;
    }

    private static void resetToLastCommittedPositions(KafkaConsumer<String, String> consumer) {
        for (TopicPartition topicPartition : consumer.assignment()) {
            OffsetAndMetadata offsetAndMetadata = consumer.committed(topicPartition);
            if (offsetAndMetadata != null)
                consumer.seek(topicPartition, offsetAndMetadata.offset());
            else
                consumer.seekToBeginning(singleton(topicPartition));
        }
    }

    private static long messagesRemaining(KafkaConsumer<String, String> consumer, TopicPartition partition) {
        long currentPosition = consumer.position(partition);
        Map<TopicPartition, Long> endOffsets = consumer.endOffsets(singleton(partition));
        if (endOffsets.containsKey(partition)) {
            return endOffsets.get(partition) - currentPosition;
        }
        return 0;
    }

    private static String toJsonString(Map<String, Object> data) {
        String json;
        try {
            ObjectMapper mapper = new ObjectMapper();
            json = mapper.writeValueAsString(data);
        } catch (JsonProcessingException e) {
            json = "Bad data can't be written as json: " + e.getMessage();
        }
        return json;
    }

    private static String statusAsJson(long consumed, long remaining, String transactionalId) {
        Map<String, Object> statusData = new HashMap<>();
        statusData.put("progress", transactionalId);
        statusData.put("consumed", consumed);
        statusData.put("remaining", remaining);
        return toJsonString(statusData);
    }

    private static String shutDownString(long consumed, long remaining, String transactionalId) {
        Map<String, Object> shutdownData = new HashMap<>();
        shutdownData.put("remaining", remaining);
        shutdownData.put("consumed", consumed);
        shutdownData.put("shutdown_complete", transactionalId);
        return toJsonString(shutdownData);
    }

    public static void main(String[] args) throws IOException {
        Namespace parsedArgs = argParser().parseArgsOrFail(args);
        Integer numMessagesPerTransaction = parsedArgs.getInt("messagesPerTransaction");
        final String transactionalId = parsedArgs.getString("transactionalId");
        final String outputTopic = parsedArgs.getString("outputTopic");

        String consumerGroup = parsedArgs.getString("consumerGroup");
        TopicPartition inputPartition = new TopicPartition(parsedArgs.getString("inputTopic"), parsedArgs.getInt("inputPartition"));

        final KafkaProducer<String, String> producer = createProducer(parsedArgs);
        final KafkaConsumer<String, String> consumer = createConsumer(parsedArgs);

        consumer.assign(singleton(inputPartition));

        long maxMessages = parsedArgs.getInt("maxMessages") == -1 ? Long.MAX_VALUE : parsedArgs.getInt("maxMessages");
        maxMessages = Math.min(messagesRemaining(consumer, inputPartition), maxMessages);
        final boolean enableRandomAborts = parsedArgs.getBoolean("enableRandomAborts");

        producer.initTransactions();

        final AtomicBoolean isShuttingDown = new AtomicBoolean(false);
        final AtomicLong remainingMessages = new AtomicLong(maxMessages);
        final AtomicLong numMessagesProcessed = new AtomicLong(0);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                isShuttingDown.set(true);
                // Flush any remaining messages
                producer.close();
                synchronized (consumer) {
                    consumer.close();
                }
                System.out.println(shutDownString(numMessagesProcessed.get(), remainingMessages.get(), transactionalId));
            }
        });

        try {
            Random random = new Random();
            while (0 < remainingMessages.get()) {
                System.out.println(statusAsJson(numMessagesProcessed.get(), remainingMessages.get(), transactionalId));
                if (isShuttingDown.get())
                    break;
                int messagesInCurrentTransaction = 0;
                long numMessagesForNextTransaction = Math.min(numMessagesPerTransaction, remainingMessages.get());

                try {
                    producer.beginTransaction();
                    while (messagesInCurrentTransaction < numMessagesForNextTransaction) {
                        ConsumerRecords<String, String> records = consumer.poll(200L);
                        for (ConsumerRecord<String, String> record : records) {
                            producer.send(producerRecordFromConsumerRecord(outputTopic, record));
                            messagesInCurrentTransaction++;
                        }
                    }
                    producer.sendOffsetsToTransaction(consumerPositions(consumer), consumerGroup);

                    if (enableRandomAborts && random.nextInt() % 3 == 0) {
                        throw new KafkaException("Aborting transaction");
                    } else {
                        producer.commitTransaction();
                        remainingMessages.set(maxMessages - numMessagesProcessed.addAndGet(messagesInCurrentTransaction));
                    }
                } catch (ProducerFencedException | OutOfOrderSequenceException e) {
                    // We cannot recover from these errors, so just rethrow them and let the process fail
                    throw e;
                } catch (KafkaException e) {
                    producer.abortTransaction();
                    resetToLastCommittedPositions(consumer);
                }
            }
        } finally {
            producer.close();
            synchronized (consumer) {
                consumer.close();
            }
        }
        System.exit(0);
    }
}
