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
package org.apache.kafka.examples;

import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.FencedInstanceIdException;
import org.apache.kafka.common.errors.ProducerFencedException;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import static java.lang.String.format;
import static java.util.Collections.singleton;

/**
 * This class implements a read-process-write application.
 */
public class MessageProcessor extends Thread {
    private final String inputTopic;
    private final String outputTopic;
    private final String transactionalId;
    private final String groupInstanceId;

    private final KafkaProducer<Integer, String> producer;
    private final KafkaConsumer<Integer, String> consumer;

    private final CountDownLatch latch;

    public MessageProcessor(String inputTopic,
                            String outputTopic,
                            int instanceIdx,
                            CountDownLatch latch) {
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.transactionalId = "processor" + instanceIdx;
        // it is recommended to have a relatively short txn timeout in order to clear pending offsets faster
        final int transactionTimeoutMs = 10_000;
        // a unique transactional.id must be provided in order to properly use EOS
        this.producer = new Producer(outputTopic, true, transactionalId, true, -1, transactionTimeoutMs, null).get();
        // consumer must be in read_committed mode, which means it won't be able to read uncommitted data
        // consumer could optionally configure groupInstanceId to avoid unnecessary rebalances
        this.groupInstanceId = "processor-consumer" + instanceIdx;
        this.consumer = new Consumer(inputTopic, "processor-group", Optional.of(groupInstanceId), true, -1, null).get();
        this.latch = latch;
    }

    @Override
    public void run() {
        // init transactions call should always happen first in order to clear zombie transactions from previous generation
        producer.initTransactions();

        AtomicLong remainingMessages = new AtomicLong(Long.MAX_VALUE);

        consumer.subscribe(singleton(inputTopic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                printWithTxnId(format("Revoking partitions: %s", partitions));
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                printWithTxnId(format("Assigning partitions: %s", partitions));
                remainingMessages.set(computeRemainingMessages(consumer));
            }
        });

        int processedMessages = 0;
        while (remainingMessages.get() > 0) {
            try {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
                if (records.count() > 0) {
                    // begin a new transaction session
                    producer.beginTransaction();
                    printWithTxnId(format("Processing new messages from %s", inputTopic));
                    for (ConsumerRecord<Integer, String> record : records) {
                        // process the record and send downstream
                        ProducerRecord<Integer, String> newRecord =
                            new ProducerRecord<>(outputTopic, record.key() / 2, record.value() + "ok");
                        producer.send(newRecord);
                    }

                    Map<TopicPartition, OffsetAndMetadata> offsets = consumerOffsets();

                    // checkpoint the progress by sending offsets to group coordinator broker
                    // note that this API is only available for broker >= 2.5
                    producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());

                    // finish the transaction (all sent records should be visible for consumption now)
                    producer.commitTransaction();
                    processedMessages += records.count();
                }
            } catch (ProducerFencedException e) {
                throw new KafkaException(format("The transactional.id %s has been claimed by another process", transactionalId));
            } catch (FencedInstanceIdException e) {
                throw new KafkaException(format("The group.instance.id %s has been claimed by another process", groupInstanceId));
            } catch (KafkaException e) {
                // if we have not been fenced, try to abort the transaction and continue
                // this will raise immediately if the producer has hit a fatal error
                producer.abortTransaction();
                // the consumer fetch position needs to be restored to the committed offset before the transaction started
                resetToLastCommittedPositions(consumer);
            }
            remainingMessages.set(computeRemainingMessages(consumer));
            //printWithTxnId(format("Remaining messages: %d", remainingMessages.get()));
        }

        printWithTxnId(format("Done processing %d messages from %s", processedMessages, inputTopic));
        latch.countDown();
    }

    private Map<TopicPartition, OffsetAndMetadata> consumerOffsets() {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (TopicPartition topicPartition : consumer.assignment()) {
            offsets.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
        }
        return offsets;
    }

    private void printWithTxnId(String message) {
        System.out.printf("%s: %s%n", transactionalId, message);
    }

    private long computeRemainingMessages(KafkaConsumer<Integer, String> consumer) {
        final Map<TopicPartition, Long> fullEndOffsets = consumer.endOffsets(new ArrayList<>(consumer.assignment()));
        // if we can't detect any end offset, that means we are still not able to fetch offsets
        if (fullEndOffsets.isEmpty()) {
            return Long.MAX_VALUE;
        }

        return consumer.assignment().stream().mapToLong(partition -> {
            long currentPosition = consumer.position(partition);
            printWithTxnId(format("Processing partition %s with full offsets: %s", partition, fullEndOffsets));
            if (fullEndOffsets.containsKey(partition)) {
                return fullEndOffsets.get(partition) - currentPosition;
            }
            return 0;
        }).sum();
    }

    private static void resetToLastCommittedPositions(KafkaConsumer<Integer, String> consumer) {
        final Map<TopicPartition, OffsetAndMetadata> committed = consumer.committed(consumer.assignment());
        consumer.assignment().forEach(tp -> {
            OffsetAndMetadata offsetAndMetadata = committed.get(tp);
            if (offsetAndMetadata != null) {
                consumer.seek(tp, offsetAndMetadata.offset());
            } else {
                consumer.seekToBeginning(singleton(tp));
            }
        });
    }
}
