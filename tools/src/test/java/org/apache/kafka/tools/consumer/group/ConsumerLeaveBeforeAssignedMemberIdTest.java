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


import kafka.test.ClusterInstance;
import kafka.test.annotation.ClusterConfigProperty;
import kafka.test.annotation.ClusterTest;
import kafka.test.annotation.ClusterTestDefaults;
import kafka.test.annotation.Type;
import kafka.test.junit.ClusterTestExtensions;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;


@ExtendWith(value = ClusterTestExtensions.class)
@ClusterTestDefaults(types = {Type.KRAFT})
public class ConsumerLeaveBeforeAssignedMemberIdTest {
    private static final String GROUP_ID = "group-id";
    public static final String TOPIC = "topic";

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = GroupCoordinatorConfig.NEW_GROUP_COORDINATOR_ENABLE_CONFIG, value = "true"),
        @ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic, consumer"),
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "num.partitions", value = "2"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1")
    })
    public void reproduce(ClusterInstance cluster) throws ExecutionException, InterruptedException {
        Admin adminClient = cluster.createAdminClient();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean running = new AtomicBoolean(true);

        Set<String> topics = Collections.singleton(TOPIC);
        Future<KafkaConsumer<String, String>> consumerFuture = executor.submit(() -> {
            KafkaConsumer<String, String> consumer = newConsumer(cluster);
            consumer.subscribe(topics);
            while (running.get()) {
                consumer.poll(Duration.ofMillis(150));
            }
            return consumer;
        });

        KafkaConsumer<String, String> consumer = newConsumer(cluster);
        consumer.subscribe(topics);
        consumer.poll(Duration.ofMillis(130));
        consumer.close();

        TestUtils.waitForCondition(() -> {
            while (!getGroupState(adminClient).equals(ConsumerGroupState.STABLE)) {
            }
            return true;
        }, "Wait until group state is STABLE");
        running.set(false);
        KafkaConsumer<String, String> consumerInBackground = consumerFuture.get();
        Assertions.assertEquals(2, consumerInBackground.assignment().size());
        consumerInBackground.close();
    }

    private static ConsumerGroupState getGroupState(Admin adminClient) throws InterruptedException, ExecutionException {
        return adminClient.describeConsumerGroups(Collections.singleton(GROUP_ID))
            .describedGroups()
            .get(GROUP_ID)
            .get()
            .state();
    }

    private KafkaConsumer<String, String> newConsumer(ClusterInstance cluster) {
        final Properties configs = new Properties();
        configs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        configs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configs.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        configs.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        return new KafkaConsumer<>(configs);
    }
}
