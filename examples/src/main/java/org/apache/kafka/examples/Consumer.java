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
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singleton;

/**
 * A simple consumer thread that subscribes to a topic, fetches new messages and prints them.
 * The thread does not stop until all messages are completed or an exception is raised.
 */
public class Consumer extends Thread implements ConsumerRebalanceListener {
    private final KafkaConsumer<Integer, String> consumer;
    private final String topic;
    private final int numRecords;
    private final CountDownLatch latch;
    private int remainingRecords;
    private volatile boolean closed;

    public Consumer(String bootstrapServers,
                    String topic,
                    String groupId,
                    Optional<String> instanceId,
                    boolean readCommitted,
                    int numRecords,
                    CountDownLatch latch) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, Utils.createClientId());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        if (readCommitted) {
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        this.consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.numRecords = numRecords;
        this.remainingRecords = numRecords;
        this.latch = latch;
    }

    public KafkaConsumer<Integer, String> get() {
        return consumer;
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(singleton(topic), this);
            System.out.printf("Subscribed to %s%n", topic);
            while (!closed && remainingRecords > 0) {
                ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<Integer, String> record : records) {
                    // we only print 10 messages when there are 20 or more to send
                    if (record.key() % Math.max(1, numRecords / 10) == 0) {
                        System.out.printf("sample: message(%d, %s) received from partition %d with offset %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                    }
                }
                remainingRecords -= records.count();
            }
        } catch (WakeupException e) {
            // swallow the wakeup
        } catch (Throwable e) {
            e.printStackTrace();
        }
        System.out.printf("Done: %d messages received from %s%n",
            numRecords - remainingRecords, topic);
        shutdown();
    }

    public void shutdown() {
        closed = true;
        consumer.close(Duration.ofMillis(5_000));
        latch.countDown();
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        System.out.printf("Revoked partitions: %s%n", partitions);
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        System.out.printf("Assigned partitions: %s%n", partitions);
    }
}
