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

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import static java.time.Duration.ofMillis;
import static java.util.Collections.emptyList;
import static java.util.Collections.singleton;

/**
 * This class implements a read-process-write application.
 */
public class ExactlyOnceMessageProcessor extends Thread implements ConsumerRebalanceListener, AutoCloseable {
    private static final int MAX_RETRIES = 5;
    
    private final String bootstrapServers;
    private final String inputTopic;
    private final String outputTopic;
    private final String groupInstanceId;
    private final CountDownLatch latch;
    private final String transactionalId;
    private volatile boolean closed;

    private final KafkaProducer<Integer, String> producer;
    private final KafkaConsumer<Integer, String> consumer;

    public ExactlyOnceMessageProcessor(String threadName,
                                       String bootstrapServers,
                                       String inputTopic,
                                       String outputTopic,
                                       CountDownLatch latch) {
        super(threadName);
        this.bootstrapServers = bootstrapServers;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.transactionalId = "tid-" + threadName;
        // It is recommended to have a relatively short txn timeout in order to clear pending offsets faster.
        int transactionTimeoutMs = 10_000;
        // A unique transactional.id must be provided in order to properly use EOS.
        producer = new Producer(
                "processor-producer",
                KafkaProperties.BOOTSTRAP_SERVERS,
                outputTopic,
                true,
                transactionalId,
                true,
                -1,
                transactionTimeoutMs,
                null)
                .createKafkaProducer();
        // Consumer must be in read_committed mode, which means it won't be able to read uncommitted data.
        // Consumer could optionally configure groupInstanceId to avoid unnecessary rebalances.
        this.groupInstanceId = "giid-" + threadName;
        boolean readCommitted = true;
        consumer = new Consumer(
                "processor-consumer",
                KafkaProperties.BOOTSTRAP_SERVERS,
                inputTopic,
                "processor-group",
                Optional.of(groupInstanceId),
                readCommitted,
                -1,
                null)
                .createKafkaConsumer();
        this.latch = latch;
    }

    @Override
    public void run() {
        int retries = 0;
        int processedRecords = 0;
        long remainingRecords = Long.MAX_VALUE;

        // it is recommended to have a relatively short txn timeout in order to clear pending offsets faster
        int transactionTimeoutMs = 10_000;
        // consumer must be in read_committed mode, which means it won't be able to read uncommitted data
        boolean readCommitted = true;

        try (KafkaProducer<Integer, String> producer = new Producer("processor-producer", bootstrapServers, outputTopic,
                true, transactionalId, true, -1, transactionTimeoutMs, null).createKafkaProducer();
             KafkaConsumer<Integer, String> consumer = new Consumer("processor-consumer", bootstrapServers, inputTopic,
                 "processor-group", Optional.of(groupInstanceId), readCommitted, -1, null).createKafkaConsumer()) {
            // called first and once to fence zombies and abort any pending transaction
            producer.initTransactions();
            consumer.subscribe(singleton(inputTopic), this);

            Utils.printOut("Processing new records");
            while (!closed && remainingRecords > 0) {
                try {
                    ConsumerRecords<Integer, String> records = consumer.poll(ofMillis(200));
                    if (!records.isEmpty()) {
                        // begin a new transaction session
                        producer.beginTransaction();

                        for (ConsumerRecord<Integer, String> record : records) {
                            // process the record and send downstream
                            ProducerRecord<Integer, String> newRecord =
                                new ProducerRecord<>(outputTopic, record.key(), record.value() + "-ok");
                            producer.send(newRecord);
                        }

                        // checkpoint the progress by sending offsets to group coordinator broker
                        // note that this API is only available for broker >= 2.5
                        producer.sendOffsetsToTransaction(getOffsetsToCommit(consumer), consumer.groupMetadata());

                        // commit the transaction including offsets
                        producer.commitTransaction();
                        processedRecords += records.count();
                        retries = 0;
                    }
                } catch (AuthorizationException | UnsupportedVersionException | ProducerFencedException
                         | FencedInstanceIdException | OutOfOrderSequenceException | SerializationException e) {
                    // we can't recover from these exceptions
                    Utils.printErr(e.getMessage());
                    shutdown();
                } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                    // invalid or no offset found without auto.reset.policy
                    Utils.printOut("Invalid or no offset found, using latest");
                    consumer.seekToEnd(emptyList());
                    consumer.commitSync();
                    retries = 0;
                } catch (KafkaException e) {
                    // abort the transaction
                    Utils.printOut("Aborting transaction: %s", e.getMessage());
                    producer.abortTransaction();
                    retries = maybeRetry(retries, consumer);
                }

                remainingRecords = getRemainingRecords(consumer);
                if (remainingRecords != Long.MAX_VALUE) {
                    Utils.printOut("Remaining records: %d", remainingRecords);
                }
            }
        } catch (Throwable e) {
            Utils.printErr("Unhandled exception");
            e.printStackTrace();
        }
        Utils.printOut("Processed %d records", processedRecords);
        shutdown();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Utils.printOut("Revoked partitions: %s", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Utils.printOut("Assigned partitions: %s", partitions);
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        Utils.printOut("Lost partitions: %s", partitions);
    }

    public void shutdown() {
        if (!closed) {
            closed = true;
            latch.countDown();
        }
    }

    private Map<TopicPartition, OffsetAndMetadata> getOffsetsToCommit(KafkaConsumer<Integer, String> consumer) {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            offsets.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
        }
        return offsets;
    }

    private long getRemainingRecords(KafkaConsumer<Integer, String> consumer) {
        final Map<TopicPartition, Long> fullEndOffsets = consumer.endOffsets(new ArrayList<>(consumer.assignment()));
        // if we can't detect any end offset, that means we are still not able to fetch offsets
        if (fullEndOffsets.isEmpty()) {
            return Long.MAX_VALUE;
        }
        return consumer.assignment().stream().mapToLong(partition -> {
            long currentPosition = consumer.position(partition);
            if (fullEndOffsets.containsKey(partition)) {
                return fullEndOffsets.get(partition) - currentPosition;
            } else {
                return 0;
            }
        }).sum();
    }

    /**
     * When we get a generic {@code KafkaException} while processing records, we retry up to {@code MAX_RETRIES} times.
     * If we exceed this threshold, we log an error and move on to the next batch of records.
     * In a real world application you may want to to send these records to a dead letter topic (DLT) for further processing.
     * 
     * @param retries Current number of retries
     * @param consumer Consumer instance
     * @return Updated number of retries
     */
    private int maybeRetry(int retries, KafkaConsumer<Integer, String> consumer) {
        if (retries < 0) {
            Utils.printErr("The number of retries must be greater than zero");
            shutdown();
        }
        
        if (retries < MAX_RETRIES) {
            // retry: reset fetch offset
            // the consumer fetch position needs to be restored to the committed offset before the transaction started
            Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
            consumer.assignment().forEach(tp -> {
                OffsetAndMetadata offsetAndMetadata = committed.get(tp);
                if (offsetAndMetadata != null) {
                    consumer.seek(tp, offsetAndMetadata.offset());
                } else {
                    consumer.seekToBeginning(Collections.singleton(tp));
                }
            });
            retries++;
        } else {
            // continue: skip records
            // the consumer fetch position needs to be committed as if records were processed successfully
            Utils.printErr("Skipping records after %d retries", MAX_RETRIES);
            consumer.commitSync();
            retries = 0;
        }
        return retries;
    }

    @Override
    public void close() throws Exception {
        if (producer != null) {
            producer.close();
        }

        if (consumer != null) {
            consumer.close();
        }
    }
}
