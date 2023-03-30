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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singleton;

/**
 * A simple consumer thread that subscribes to a topic, fetches new records and prints them.
 * The thread does not stop until all records are completed or an exception is raised.
 */
public class Consumer extends Thread implements ConsumerRebalanceListener {
    private final String bootstrapServers;
    private final String topic;
    private final String groupId;
    private final Optional<String> instanceId;
    private final boolean readCommitted;
    private final int numRecords;
    private final CountDownLatch latch;
    private volatile boolean closed;
    private int remainingRecords;

    public Consumer(String threadName,
                    String bootstrapServers,
                    String topic,
                    String groupId,
                    Optional<String> instanceId,
                    boolean readCommitted,
                    int numRecords,
                    CountDownLatch latch) {
        super(threadName);
        this.bootstrapServers = bootstrapServers;
        this.topic = topic;
        this.groupId = groupId;
        this.instanceId = instanceId;
        this.readCommitted = readCommitted;
        this.numRecords = numRecords;
        this.remainingRecords = numRecords;
        this.latch = latch;
    }

    @Override
    public void run() {
        // the consumer instance is NOT thread safe
        try (KafkaConsumer<Integer, String> consumer = createKafkaConsumer()) {
            consumer.subscribe(singleton(topic), this);
            Utils.printOut("Subscribed to %s", topic);
            while (!closed && remainingRecords > 0) {
                try {
                    // next poll must be called within session.timeout.ms to avoid rebalance
                    ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<Integer, String> record : records) {
                        Utils.maybePrintRecord(numRecords, record);
                    }
                    remainingRecords -= records.count();
                } catch (Throwable e) {
                    // add your application retry strategy here
                    Utils.printErr(e.getMessage());
                    break;
                }
            }
        } catch (Throwable e) {
            Utils.printOut("Fatal error");
            e.printStackTrace();
        }
        Utils.printOut("Fetched %d records", numRecords - remainingRecords);
        shutdown();
    }

    public void shutdown() {
        if (!closed) {
            closed = true;
            latch.countDown();
        }
    }

    public KafkaConsumer<Integer, String> createKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "client-" + UUID.randomUUID());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, readCommitted ? "false" : "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        if (readCommitted) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Utils.printOut("Revoked partitions: %s", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Utils.printOut("Assigned partitions: %s", partitions);
    }
}
