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

package org.apache.kafka.tools.consumer.group;

import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.RangeAssignor;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_ID_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.common.GroupType.CONSUMER;

public class ConsumerGroupExecutor implements AutoCloseable {
    private final ExecutorService executor;
    private final List<ConsumerRunnable> consumers = new ArrayList<>();

    private ConsumerGroupExecutor(
            String brokerAddress,
            int numberOfConsumers,
            String groupId,
            String groupProtocol,
            String topic,
            String assignmentStrategy,
            Optional<String> remoteAssignor,
            Optional<Properties> customConfigs,
            boolean syncCommit
    ) {
        this.executor = Executors.newFixedThreadPool(numberOfConsumers);
        IntStream.rangeClosed(1, numberOfConsumers).forEach(i -> {
            ConsumerRunnable consumerThread = new ConsumerRunnable(
                    brokerAddress,
                    groupId,
                    groupProtocol,
                    topic,
                    assignmentStrategy,
                    remoteAssignor,
                    customConfigs,
                    syncCommit
            );
            submit(consumerThread);
        });
    }

    public static ConsumerGroupExecutor buildConsumerGroup(String brokerAddress,
                                                           int numConsumers,
                                                           String groupId,
                                                           String topic,
                                                           String groupProtocol,
                                                           Optional<String> remoteAssignor,
                                                           Optional<Properties> customConfigs,
                                                           boolean syncCommit) {
        return new ConsumerGroupExecutor(
                brokerAddress,
                numConsumers,
                groupId,
                groupProtocol,
                topic,
                RangeAssignor.class.getName(),
                remoteAssignor,
                customConfigs,
                syncCommit
        );
    }

    public static ConsumerGroupExecutor buildClassicGroup(String brokerAddress,
                                                          int numConsumers,
                                                          String groupId,
                                                          String topic,
                                                          String assignmentStrategy,
                                                          Optional<Properties> customConfigs,
                                                          boolean syncCommit) {
        return new ConsumerGroupExecutor(
                brokerAddress,
                numConsumers,
                groupId,
                GroupProtocol.CLASSIC.name,
                topic,
                assignmentStrategy,
                Optional.empty(),
                customConfigs,
                syncCommit
        );
    }

    void submit(ConsumerRunnable consumerThread) {
        consumers.add(consumerThread);
        executor.execute(consumerThread);
    }

    @Override
    public void close() throws Exception {
        consumers.forEach(ConsumerRunnable::shutdown);
        executor.shutdown();
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    static class ConsumerRunnable implements Runnable {
        private final String brokerAddress;
        private final String groupId;
        private final Properties customConfigs;
        private final boolean syncCommit;
        private final String topic;
        private final String groupProtocol;
        private final String assignmentStrategy;
        private final Optional<String> remoteAssignor;
        private final Properties props = new Properties();
        private KafkaConsumer<String, String> consumer;
        private boolean configured = false;
        private volatile boolean isShutdown = false;

        public ConsumerRunnable(String brokerAddress,
                                String groupId,
                                String groupProtocol,
                                String topic,
                                String assignmentStrategy,
                                Optional<String> remoteAssignor,
                                Optional<Properties> customConfigs,
                                boolean syncCommit) {
            this.brokerAddress = brokerAddress;
            this.groupId = groupId;
            this.customConfigs = customConfigs.orElse(new Properties());
            this.syncCommit = syncCommit;
            this.topic = topic;
            this.groupProtocol = groupProtocol;
            this.assignmentStrategy = assignmentStrategy;
            this.remoteAssignor = remoteAssignor;

            this.configure();
        }

        private void configure() {
            configured = true;
            configure(props);
            props.putAll(customConfigs);
            consumer = new KafkaConsumer<>(props);
        }

        private void configure(Properties props) {
            props.put(BOOTSTRAP_SERVERS_CONFIG, brokerAddress);
            props.put(GROUP_ID_CONFIG, groupId);
            props.put(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            props.put(GROUP_PROTOCOL_CONFIG, groupProtocol);
            if (Objects.equals(groupProtocol, CONSUMER.toString())) {
                remoteAssignor.ifPresent(assignor -> props.put(GROUP_REMOTE_ASSIGNOR_CONFIG, assignor));
            } else {
                props.put(PARTITION_ASSIGNMENT_STRATEGY_CONFIG, assignmentStrategy);
            }
        }

        @Override
        public void run() {
            assert configured : "Must call configure before use";
            try {
                consumer.subscribe(singleton(topic));
                while (!isShutdown) {
                    consumer.poll(Duration.ofMillis(Long.MAX_VALUE));
                    if (syncCommit)
                        consumer.commitSync();
                }
            } catch (WakeupException e) {
                // OK
            } finally {
                consumer.close();
            }
        }

        void shutdown() {
            isShutdown = true;
            consumer.wakeup();
        }
    }
}
