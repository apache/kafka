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

import org.apache.kafka.clients.consumer.CommitFailedException;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A demo class for how to write a customized EOS app. It takes a consume-process-produce loop.
 * Important configurations and APIs are commented.
 */
public class ExactlyOnceMessageProcessor extends Thread {

    private static final boolean READ_COMMITTED = true;

    private final String mode;
    private final String inputTopic;
    private final String outputTopic;
    private final String consumerGroupId;
    private final int numPartitions;
    private final int numInstances;
    private final int instanceIdx;
    private final String transactionalId;

    private final KafkaProducer<Integer, String> producer;
    private final KafkaConsumer<Integer, String> consumer;

    private final CountDownLatch latch;

    public ExactlyOnceMessageProcessor(final String mode,
                                       final String inputTopic,
                                       final String outputTopic,
                                       final int numPartitions,
                                       final int numInstances,
                                       final int instanceIdx,
                                       final CountDownLatch latch) {
        this.mode = mode;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.consumerGroupId = "Eos-consumer";
        this.numPartitions = numPartitions;
        this.numInstances = numInstances;
        this.instanceIdx = instanceIdx;
        this.transactionalId = "Processor-" + instanceIdx;
        // If we are using the group mode, it is recommended to have a relatively short txn timeout
        // in order to clear pending offsets faster.
        final int transactionTimeoutMs = this.mode.equals("groupMode") ? 10000 : -1;
        // A unique transactional.id must be provided in order to properly use EOS.
        producer = new Producer(outputTopic, true, transactionalId, true, -1, transactionTimeoutMs, null).get();
        // Consumer must be in read_committed mode, which means it won't be able to read uncommitted data.
        consumer = new Consumer(inputTopic, consumerGroupId, READ_COMMITTED, -1, null).get();
        this.latch = latch;
    }

    @Override
    public void run() {
        // Init transactions call should always happen first in order to clear zombie transactions from previous generation.
        producer.initTransactions();

        final AtomicLong messageRemaining = new AtomicLong(Long.MAX_VALUE);

        // Under group mode, topic based subscription is sufficient as EOS apps are safe to cooperate transactionally after 2.5.
        // Under standalone mode, user needs to manually assign the topic partitions and make sure the assignment is unique
        // across the consumer group instances.
        if (this.mode.equals("groupMode")) {
            consumer.subscribe(Collections.singleton(inputTopic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    printWithTxnId("Revoked partition assignment to kick-off rebalancing: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    printWithTxnId("Received partition assignment after rebalancing: " + partitions);
                    messageRemaining.set(messagesRemaining(consumer));
                }
            });
        } else {
            // Do a range assignment of topic partitions.
            List<TopicPartition> topicPartitions = new ArrayList<>();
            int rangeSize = numPartitions / numInstances;
            int startPartition = rangeSize * instanceIdx;
            int endPartition = Math.min(numPartitions - 1, startPartition + rangeSize - 1);
            for (int partition = startPartition; partition <= endPartition; partition++) {
                topicPartitions.add(new TopicPartition(inputTopic, partition));
            }

            consumer.assign(topicPartitions);
            printWithTxnId("Manually assign partitions: " + topicPartitions);
        }

        int messageProcessed = 0;
        boolean abortPreviousTransaction = false;
        while (messageRemaining.get() > 0) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
            if (records.count() > 0) {
                try {
                    // Abort previous transaction if instructed.
                    if (abortPreviousTransaction) {
                        producer.abortTransaction();
                        // The consumer fetch position also needs to be reset.
                        resetToLastCommittedPositions(consumer);
                        abortPreviousTransaction = false;
                    }
                    // Begin a new transaction session.
                    producer.beginTransaction();
                    for (ConsumerRecord<Integer, String> record : records) {
                        // Process the record and send to downstream.
                        ProducerRecord<Integer, String> customizedRecord = transform(record);
                        producer.send(customizedRecord);
                    }
                    Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
                    for (TopicPartition topicPartition : consumer.assignment()) {
                        positions.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
                    }
                    // Checkpoint the progress by sending offsets to group coordinator broker.
                    // Under group mode, we must apply consumer group metadata for proper fencing.
                    if (this.mode.equals("groupMode")) {
                        producer.sendOffsetsToTransaction(positions, consumer.groupMetadata());
                    } else {
                        producer.sendOffsetsToTransaction(positions, consumerGroupId);
                    }

                    // Finish the transaction. All sent records should be visible for consumption now.
                    producer.commitTransaction();
                    messageProcessed += records.count();
                } catch (CommitFailedException e) {
                    // In case of a retriable exception, suggest aborting the ongoing transaction for correctness.
                    abortPreviousTransaction = true;
                } catch (ProducerFencedException | FencedInstanceIdException e) {
                    throw new KafkaException("Encountered fatal error during processing: " + e.getMessage());
                }
            }
            messageRemaining.set(messagesRemaining(consumer));
            printWithTxnId("Message remaining: " + messageRemaining);
        }

        printWithTxnId("Finished processing " + messageProcessed + " records");
        latch.countDown();
    }

    private void printWithTxnId(final String message) {
        System.out.println(transactionalId + ": " + message);
    }

    private ProducerRecord<Integer, String> transform(final ConsumerRecord<Integer, String> record) {
        printWithTxnId("Transformed record (" + record.key() + "," + record.value() + ")");
        return new ProducerRecord<>(outputTopic, record.key() / 2, "Transformed_" + record.value());
    }

    private long messagesRemaining(final KafkaConsumer<Integer, String> consumer) {
        final Map<TopicPartition, Long> fullEndOffsets = consumer.endOffsets(new ArrayList<>(consumer.assignment()));
        // If we couldn't detect any end offset, that means we are still not able to fetch offsets.
        if (fullEndOffsets.isEmpty()) {
            return Long.MAX_VALUE;
        }

        return consumer.assignment().stream().mapToLong(partition -> {
            long currentPosition = consumer.position(partition);
            printWithTxnId("Processing partition " + partition + " with full offsets " + fullEndOffsets);
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
            if (offsetAndMetadata != null)
                consumer.seek(tp, offsetAndMetadata.offset());
            else
                consumer.seekToBeginning(Collections.singleton(tp));
        });
    }
}
