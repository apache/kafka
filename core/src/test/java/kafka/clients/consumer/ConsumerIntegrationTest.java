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
package kafka.clients.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.AbstractHeartbeatRequestManager;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.test.TestUtils;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.ClusterTests;

import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ClusterTestExtensions.class)
public class ConsumerIntegrationTest {

    @ClusterTests({
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "group.coordinator.new.enable", value = "false")
        }),
        @ClusterTest(serverProperties = {
            @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
            @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
            @ClusterConfigProperty(key = "group.coordinator.rebalance.protocols", value = "classic")
        })
    })
    public void testAsyncConsumerWithOldGroupCoordinator(ClusterInstance clusterInstance) throws Exception {
        String topic = "test-topic";
        clusterInstance.createTopic(topic, 1, (short) 1);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(Map.of(
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers(),
            ConsumerConfig.GROUP_ID_CONFIG, "test-group",
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName(),
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.CONSUMER.name()))) {
            consumer.subscribe(Collections.singletonList(topic));
            TestUtils.waitForCondition(() -> {
                try {
                    consumer.poll(Duration.ofMillis(1000));
                    return false;
                } catch (UnsupportedVersionException e) {
                    return e.getMessage().equals(AbstractHeartbeatRequestManager.CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG);
                }
            }, "Should get UnsupportedVersionException and how to revert to classic protocol");
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testFetchPartitionsAfterFailedListenerWithGroupProtocolClassic(ClusterInstance clusterInstance)
            throws InterruptedException {
        testFetchPartitionsAfterFailedListener(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testFetchPartitionsAfterFailedListenerWithGroupProtocolConsumer(ClusterInstance clusterInstance)
            throws InterruptedException {
        testFetchPartitionsAfterFailedListener(clusterInstance, GroupProtocol.CONSUMER);
    }

    private static void testFetchPartitionsAfterFailedListener(ClusterInstance clusterInstance, GroupProtocol groupProtocol)
            throws InterruptedException {
        var topic = "topic";
        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes()));
        }

        try (var consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name()))) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                private int count = 0;
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    count++;
                    if (count == 1) throw new IllegalArgumentException("temporary error");
                }
            });

            TestUtils.waitForCondition(() -> consumer.poll(Duration.ofSeconds(1)).count() == 1,
                    5000,
                    "failed to poll data");
        }
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolClassic(ClusterInstance clusterInstance)
            throws InterruptedException {
        testFetchPartitionsWithAlwaysFailedListener(clusterInstance, GroupProtocol.CLASSIC);
    }

    @ClusterTest(serverProperties = {
        @ClusterConfigProperty(key = "offsets.topic.num.partitions", value = "1"),
        @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
    })
    public void testFetchPartitionsWithAlwaysFailedListenerWithGroupProtocolConsumer(ClusterInstance clusterInstance)
            throws InterruptedException {
        testFetchPartitionsWithAlwaysFailedListener(clusterInstance, GroupProtocol.CONSUMER);
    }

    private static void testFetchPartitionsWithAlwaysFailedListener(ClusterInstance clusterInstance, GroupProtocol groupProtocol)
            throws InterruptedException {
        var topic = "topic";
        try (var producer = clusterInstance.producer(Map.of(
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class))) {
            producer.send(new ProducerRecord<>(topic, "key".getBytes(), "value".getBytes()));
        }

        try (var consumer = clusterInstance.consumer(Map.of(
                ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name()))) {
            consumer.subscribe(List.of(topic), new ConsumerRebalanceListener() {
                @Override
                public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
                }

                @Override
                public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
                    throw new IllegalArgumentException("always failed");
                }
            });

            long startTimeMillis = System.currentTimeMillis();
            long currentTimeMillis = System.currentTimeMillis();
            while (currentTimeMillis < startTimeMillis + 3000) {
                currentTimeMillis = System.currentTimeMillis();
                try {
                    // In the async consumer, there is a possibility that the ConsumerRebalanceListenerCallbackCompletedEvent
                    // has not yet reached the application thread. And a poll operation might still succeed, but it
                    // should not return any records since none of the assigned topic partitions are marked as fetchable.
                    assertEquals(0, consumer.poll(Duration.ofSeconds(1)).count());
                } catch (KafkaException ex) {
                    assertEquals("User rebalance callback throws an error", ex.getMessage());
                }
                Thread.sleep(300);
            }
        }
    }
}
