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

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static java.util.Collections.singleton;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_PROTOCOL_CONFIG;
import static org.apache.kafka.clients.consumer.ConsumerConfig.GROUP_REMOTE_ASSIGNOR_CONFIG;
import static org.apache.kafka.common.GroupType.CONSUMER;

public class ConsumerRunnable implements Runnable {
    final String broker;
    final String groupId;
    final Optional<Properties> customPropsOpt;
    final boolean syncCommit;
    final String topic;
    final String groupProtocol;
    final String strategy;
    final Optional<String> remoteAssignor;
    volatile boolean isShutdown = false;

    final Properties props = new Properties();
    KafkaConsumer<String, String> consumer;

    boolean configured = false;

    public ConsumerRunnable(String broker,
                            String groupId,
                            String groupProtocol,
                            String topic,
                            String strategy,
                            Optional<String> remoteAssignor,
                            Optional<Properties> customPropsOpt,
                            boolean syncCommit) {
        this.broker = broker;
        this.groupId = groupId;
        this.customPropsOpt = customPropsOpt;
        this.syncCommit = syncCommit;

        this.topic = topic;
        this.groupProtocol = groupProtocol;
        this.strategy = strategy;
        this.remoteAssignor = remoteAssignor;
    }

    void configure() {
        configured = true;
        configure(props);
        customPropsOpt.ifPresent(props::putAll);
        consumer = new KafkaConsumer<>(props);
    }

    void configure(Properties props) {
        props.put("bootstrap.servers", broker);
        props.put("group.id", groupId);
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put(GROUP_PROTOCOL_CONFIG, groupProtocol);
        if (Objects.equals(groupProtocol, CONSUMER.toString())) {
            remoteAssignor.ifPresent(assignor -> props.put(GROUP_REMOTE_ASSIGNOR_CONFIG, assignor));
        } else {
            props.put("partition.assignment.strategy", strategy);
        }
    }

    void subscribe() {
        consumer.subscribe(singleton(topic));
    }

    @Override
    public void run() {
        assert configured : "Must call configure before use";
        try {
            subscribe();
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

