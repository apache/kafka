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
import java.util.concurrent.atomic.AtomicLong;

import static java.util.Collections.singleton;

/**
 * A demo class for how to write a customized EOS app. It takes a consume-process-produce loop
 * The things to pay attention to beyond a general consumer + producer app are:
 * 1. Define a unique transactional.id for your app
 * 2. Turn on read_committed isolation level
 */
public class ExactlyOnceMessageProcessor extends Thread {

    private static final boolean READ_COMMITTED = true;

    private final String mode;
    private final String inputTopic;
    private final String outputTopic;
    private final int numPartitions;
    private final int numInstances;
    private final int instanceIdx;
    private final String transactionalId;

    private final KafkaProducer<Integer, String> producer;
    private final KafkaConsumer<Integer, String> consumer;

    public ExactlyOnceMessageProcessor(final String mode,
                                       final String inputTopic,
                                       final String outputTopic,
                                       final int numPartitions,
                                       final int numInstances,
                                       final int instanceIdx) {
        this.mode = mode;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.numPartitions = numPartitions;
        this.numInstances = numInstances;
        this.instanceIdx = instanceIdx;
        this.transactionalId = "Processor-" + instanceIdx;
        producer = new Producer(outputTopic, true, transactionalId, -1).get();
        consumer = new Consumer(inputTopic, READ_COMMITTED).get();
    }

    @Override
    public void run() {
        // Init transactions call should always happen first in order to clear zombie transactions.
        producer.initTransactions();

        final AtomicLong messageRemaining = new AtomicLong(Long.MAX_VALUE);

        // Under group mode, topic based subscription is sufficient as Consumers are safe to work transactionally after 2.5.
        // Under standalone mode, user needs to manually assign the topic partitions and make sure the assignment is unique
        // across the consumer group.
        if (this.mode.equals("groupMode")) {
            consumer.subscribe(Collections.singleton(KafkaProperties.TOPIC), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                    printWithPrefix("Revoked partition assignment to kick-off rebalancing: " + partitions);
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    printWithPrefix("Received partition assignment after rebalancing: " + partitions);
                    messageRemaining.set(messagesRemaining(consumer));
                }
            });
        } else {
            List<TopicPartition> topicPartitions = new ArrayList<>();
            int rangeSize = numPartitions / numInstances;
            int startPartition = rangeSize * instanceIdx;
            int endPartition = Math.min(numPartitions - 1, startPartition + rangeSize - 1);
            for (int partition = startPartition; partition <= endPartition; partition++) {
                topicPartitions.add(new TopicPartition(inputTopic, partition));
            }

            consumer.assign(topicPartitions);
            printWithPrefix("Manually assign partitions: " + topicPartitions);
        }

        int messageProcessed = 0;
        while (messageRemaining.get() > 0) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofMillis(200));
            if (records.count() > 0) {
                try {
                    // Begin a new transaction session.
                    producer.beginTransaction();
                    for (ConsumerRecord<Integer, String> record : records) {
                        ProducerRecord<Integer, String> customizedRecord = transform(record);
                        // Send records to the downstream.
                        producer.send(customizedRecord);
                    }
                    Map<TopicPartition, OffsetAndMetadata> positions = new HashMap<>();
                    for (TopicPartition topicPartition : consumer.assignment()) {
                        positions.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition), null));
                    }
                    // Checkpoint the progress by sending offsets to group coordinator broker.
                    producer.sendOffsetsToTransaction(positions, consumer.groupMetadata());
                    // Finish the transaction.
                    producer.commitTransaction();
                    messageProcessed += records.count();
                } catch (CommitFailedException e) {
                    producer.abortTransaction();
                } catch (ProducerFencedException | FencedInstanceIdException e) {
                    throw new KafkaException("Encountered fatal error during processing: " + e.getMessage());
                }
            }
            messageRemaining.set(messagesRemaining(consumer));
            printWithPrefix("Message remaining: " + messageRemaining);
        }

        printWithPrefix("Finished processing " + messageProcessed + " records");
    }

    private void printWithPrefix(String message) {
        System.out.println(transactionalId + ": " + message);
    }

    private ProducerRecord<Integer, String> transform(ConsumerRecord<Integer, String> record) {
        // Customized business logic here
        return new ProducerRecord<>(outputTopic, record.key() / 2, record.value());
    }

    private static long messagesRemaining(KafkaConsumer<Integer, String> consumer) {
        return consumer.assignment().stream().mapToLong(partition -> {
            long currentPosition = consumer.position(partition);
            Map<TopicPartition, Long> endOffsets = consumer.endOffsets(singleton(partition));
            if (endOffsets.containsKey(partition)) {
                return endOffsets.get(partition) - currentPosition;
            }
            return 0;
        }).sum();
    }
}
