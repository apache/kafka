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
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.MutuallyExclusiveGroup;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
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
import org.apache.kafka.common.utils.Exit;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
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

    private static final DateFormat FORMAT = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss:SSS");

    public static class TransactionalMessageCopierOptions {
        public final String inputTopic;
        public final Integer inputPartition;
        public final String outputTopic;
        public final String brokerList;
        public final Long maxMessages;
        public final String consumerGroup;
        public final Integer messagesPerTransaction;
        public final Integer transactionTimeout;
        public final String transactionalId;
        public final Boolean enableRandomAborts;
        public final Boolean groupMode;
        public final Boolean useGroupMetadata;

        private TransactionalMessageCopierOptions(String inputTopic, Integer inputPartition, String outputTopic,
                                                  String brokerList, Long maxMessages, String consumerGroup,
                                                  Integer messagesPerTransaction, Integer transactionTimeout, String transactionalId,
                                                  Boolean enableRandomAborts, Boolean groupMode, Boolean useGroupMetadata) {
            this.inputTopic = inputTopic;
            this.inputPartition = inputPartition;
            this.outputTopic = outputTopic;
            this.brokerList = brokerList;
            this.maxMessages = maxMessages;
            this.consumerGroup = consumerGroup;
            this.messagesPerTransaction = messagesPerTransaction;
            this.transactionTimeout = transactionTimeout;
            this.transactionalId = transactionalId;
            this.enableRandomAborts = enableRandomAborts;
            this.groupMode = groupMode;
            this.useGroupMetadata = useGroupMetadata;
        }

        public static TransactionalMessageCopierOptions parse(String[] args) {
            try {
                Namespace parsedArgs = argParser().parseArgs(args);
                return new TransactionalMessageCopierOptions(
                    parsedArgs.getString("inputTopic"), parsedArgs.getInt("inputPartition"),
                    parsedArgs.getString("outputTopic"),
                    parsedArgs.get("bootstrapServer") != null ? parsedArgs.getString("bootstrapServer") : parsedArgs.getString("brokerList"),
                    parsedArgs.getLong("maxMessages") == -1 ? Long.MAX_VALUE : parsedArgs.getLong("maxMessages"),
                    parsedArgs.getString("consumerGroup"), parsedArgs.getInt("messagesPerTransaction"),
                    parsedArgs.getInt("transactionTimeout"), parsedArgs.getString("transactionalId"),
                    parsedArgs.getBoolean("enableRandomAborts"), parsedArgs.getBoolean("groupMode"),
                    parsedArgs.getBoolean("useGroupMetadata")
                );
            } catch (ArgumentParserException ape) {
                Exit.exit(1, ape.getMessage());
                throw new IllegalStateException();
            }
        }

        /** Get the command-line argument parser. */
        private static ArgumentParser argParser() {
            ArgumentParser parser = ArgumentParsers
                .newArgumentParser("transactional-message-copier")
                .defaultHelp(true)
                .description("This tool copies messages transactionally from an input partition to an output topic, " +
                    "committing the consumed offsets along with the output messages");

            MutuallyExclusiveGroup connectionGroup = parser.addMutuallyExclusiveGroup("Connection Group")
                .description("Group of arguments for connection to brokers")
                .required(true);
            connectionGroup.addArgument("--bootstrap-server")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("bootstrapServer")
                .help("REQUIRED unless --broker-list(deprecated) is specified. The server(s) to connect to. Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");
            connectionGroup.addArgument("--broker-list")
                .action(store())
                .required(false)
                .type(String.class)
                .metavar("HOST1:PORT1[,HOST2:PORT2[...]]")
                .dest("brokerList")
                .help("DEPRECATED, use --bootstrap-server instead; Comma-separated list of Kafka brokers in the form HOST1:PORT1,HOST2:PORT2,...");

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

            parser.addArgument("--max-messages")
                .action(store())
                .required(false)
                .setDefault(-1L)
                .type(Long.class)
                .metavar("MAX-MESSAGES")
                .dest("maxMessages")
                .help("Process these many messages up to the end offset at the time this program was launched. If set to -1 " +
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

            parser.addArgument("--transaction-timeout")
                .action(store())
                .required(false)
                .setDefault(60000)
                .type(Integer.class)
                .metavar("TRANSACTION-TIMEOUT")
                .dest("transactionTimeout")
                .help("The transaction timeout in milliseconds. Default is 60000(1 minute).");

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

            parser.addArgument("--group-mode")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("GROUP-MODE")
                .dest("groupMode")
                .help("Whether to let consumer subscribe to the input topic or do manual assign. If we do" +
                    " subscription based consumption, the input partition shall be ignored");

            parser.addArgument("--use-group-metadata")
                .action(storeTrue())
                .type(Boolean.class)
                .metavar("USE-GROUP-METADATA")
                .dest("useGroupMetadata")
                .help("Whether to use the new transactional commit API with group metadata");

            return parser;
        }
    }

    private static KafkaProducer<String, String> createProducer(TransactionalMessageCopierOptions options) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, options.brokerList);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, options.transactionalId);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringSerializer");
        // We set a small batch size to ensure that we have multiple inflight requests per transaction.
        // If it is left at the default, each transaction will have only one batch per partition, hence not testing
        // the case with multiple inflights.
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "512");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, options.transactionTimeout);

        return new KafkaProducer<>(props);
    }

    private static KafkaConsumer<String, String> createConsumer(TransactionalMessageCopierOptions options) {
        String consumerGroup = options.consumerGroup;
        String brokerList = options.brokerList;
        Integer numMessagesPerTransaction = options.messagesPerTransaction;

        Properties props = new Properties();

        props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, numMessagesPerTransaction.toString());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "180000");
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "3000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");

        return new KafkaConsumer<>(props);
    }

    private static ProducerRecord<String, String> producerRecordFromConsumerRecord(String topic, ConsumerRecord<String, String> record) {
        return new ProducerRecord<>(topic, record.partition(), record.key(), record.value());
    }

    private static Map<TopicPartition, OffsetAndMetadata> consumerPositions(KafkaConsumer<String, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            positions.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
        }
        return positions;
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

    private static synchronized String statusAsJson(long totalProcessed, long consumedSinceLastRebalanced, long remaining, String transactionalId, String stage) {
        Map<String, Object> statusData = new HashMap<>();
        statusData.put("progress", transactionalId);
        statusData.put("totalProcessed", totalProcessed);
        statusData.put("consumed", consumedSinceLastRebalanced);
        statusData.put("remaining", remaining);
        statusData.put("time", FORMAT.format(new Date()));
        statusData.put("stage", stage);
        return toJsonString(statusData);
    }

    private static synchronized String shutDownString(long totalProcessed, long consumedSinceLastRebalanced, long remaining, String transactionalId) {
        Map<String, Object> shutdownData = new HashMap<>();
        shutdownData.put("shutdown_complete", transactionalId);
        shutdownData.put("totalProcessed", totalProcessed);
        shutdownData.put("consumed", consumedSinceLastRebalanced);
        shutdownData.put("remaining", remaining);
        shutdownData.put("time", FORMAT.format(new Date()));
        return toJsonString(shutdownData);
    }

    public static void main(String[] args) {
        TransactionalMessageCopierOptions options = TransactionalMessageCopierOptions.parse(args);
        final String transactionalId = options.transactionalId;
        final String outputTopic = options.outputTopic;

        String consumerGroup = options.consumerGroup;

        final KafkaProducer<String, String> producer = createProducer(options);
        final KafkaConsumer<String, String> consumer = createConsumer(options);

        final AtomicLong remainingMessages = new AtomicLong(options.maxMessages);

        boolean groupMode = options.groupMode;
        String topicName = options.inputTopic;
        final AtomicLong numMessagesProcessedSinceLastRebalance = new AtomicLong(0);
        final AtomicLong totalMessageProcessed = new AtomicLong(0);
        if (groupMode) {
            consumer.subscribe(Collections.singleton(topicName), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    remainingMessages.set(partitions.stream()
                        .mapToLong(partition -> messagesRemaining(consumer, partition)).sum());
                    numMessagesProcessedSinceLastRebalance.set(0);
                    // We use message cap for remaining here as the remainingMessages are not set yet.
                    System.out.println(statusAsJson(totalMessageProcessed.get(),
                        numMessagesProcessedSinceLastRebalance.get(), remainingMessages.get(), transactionalId, "RebalanceComplete"));
                }
            });
        } else {
            TopicPartition inputPartition = new TopicPartition(topicName, options.inputPartition);
            consumer.assign(singleton(inputPartition));
            remainingMessages.set(Math.min(messagesRemaining(consumer, inputPartition), remainingMessages.get()));
        }

        final boolean enableRandomAborts = options.enableRandomAborts;

        producer.initTransactions();

        final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

        Exit.addShutdownHook("transactional-message-copier-shutdown-hook", () -> {
            isShuttingDown.set(true);
            // Flush any remaining messages
            producer.close();
            synchronized (consumer) {
                consumer.close();
            }
            System.out.println(shutDownString(totalMessageProcessed.get(),
                numMessagesProcessedSinceLastRebalance.get(), remainingMessages.get(), transactionalId));
        });

        final boolean useGroupMetadata = options.useGroupMetadata;
        try {
            Random random = new Random();
            while (remainingMessages.get() > 0) {
                System.out.println(statusAsJson(totalMessageProcessed.get(),
                    numMessagesProcessedSinceLastRebalance.get(), remainingMessages.get(), transactionalId, "ProcessLoop"));
                if (isShuttingDown.get())
                    break;

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.count() > 0) {
                    try {
                        producer.beginTransaction();

                        for (ConsumerRecord<String, String> record : records) {
                            producer.send(producerRecordFromConsumerRecord(outputTopic, record));
                        }

                        long messagesSentWithinCurrentTxn = records.count();

                        if (useGroupMetadata) {
                            producer.sendOffsetsToTransaction(consumerPositions(consumer), consumer.groupMetadata());
                        } else {
                            producer.sendOffsetsToTransaction(consumerPositions(consumer), consumerGroup);
                        }

                        if (enableRandomAborts && random.nextInt() % 3 == 0) {
                            throw new KafkaException("Aborting transaction");
                        } else {
                            producer.commitTransaction();
                            remainingMessages.getAndAdd(-messagesSentWithinCurrentTxn);
                            numMessagesProcessedSinceLastRebalance.getAndAdd(messagesSentWithinCurrentTxn);
                            totalMessageProcessed.getAndAdd(messagesSentWithinCurrentTxn);
                        }
                    } catch (ProducerFencedException | OutOfOrderSequenceException e) {
                        // We cannot recover from these errors, so just rethrow them and let the process fail
                        throw e;
                    } catch (KafkaException e) {
                        producer.abortTransaction();
                        resetToLastCommittedPositions(consumer);
                    }
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
