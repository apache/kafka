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
package org.apache.kafka.api;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.GroupProtocol;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ClusterResource;
import org.apache.kafka.common.ClusterResourceListener;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.test.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestDefaults;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.apache.kafka.test.MockDeserializer;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProducerInterceptor;
import org.apache.kafka.test.MockSerializer;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.test.TestUtils.isValidClusterId;

@ClusterTestDefaults(serverProperties = {
    @ClusterConfigProperty(key = MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, value = "org.apache.kafka.api.EndToEndClusterIdTest$MockBrokerMetricsReporter"),
})
public class EndToEndClusterIdTest {

    private static final String TOPIC = "e2etopic";
    private static final int PARTITION = 0;
    private static final int NUM_RECORDS = 1;
    private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
    private final ClusterInstance clusterInstance;

    EndToEndClusterIdTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        this.clusterInstance.createTopic(TOPIC, 2, (short) 1);
        MockDeserializer.resetStaticVariables();
    }

    public static class MockConsumerMetricsReporter extends MockMetricsReporter implements ClusterResourceListener {
        public static final AtomicReference<ClusterResource> CLUSTER_META = new AtomicReference<>();

        @Override
        public void onUpdate(ClusterResource clusterMetadata) {
            CLUSTER_META.set(clusterMetadata);
        }
    }

    public static class MockProducerMetricsReporter extends MockMetricsReporter implements ClusterResourceListener {
        public static final AtomicReference<ClusterResource> CLUSTER_META = new AtomicReference<>();

        @Override
        public void onUpdate(ClusterResource clusterMetadata) {
            CLUSTER_META.set(clusterMetadata);
        }
    }

    public static class MockBrokerMetricsReporter extends MockMetricsReporter implements ClusterResourceListener {
        public static final AtomicReference<ClusterResource> CLUSTER_META = new AtomicReference<>();

        @Override
        public void onUpdate(ClusterResource clusterMetadata) {
            CLUSTER_META.set(clusterMetadata);
        }
    }

    @ClusterTest
    public void testEndToEndWithClassicProtocol(ClusterInstance clusterInstance) throws Exception {
        testEndToEnd(GroupProtocol.CONSUMER, clusterInstance);
    }

    @ClusterTest
    public void testEndToEndWithConsumerProtocol(ClusterInstance clusterInstance) throws Exception {
        testEndToEnd(GroupProtocol.CONSUMER, clusterInstance);
    }

    public void testEndToEnd(GroupProtocol groupProtocol, ClusterInstance clusterInstance) throws Exception {
        MockConsumerInterceptor.resetCounters();
        MockProducerInterceptor.resetCounters();

        // Should have cluster id at broker metrics reporter after startup
        Assertions.assertNotNull(MockBrokerMetricsReporter.CLUSTER_META);
        isValidClusterId(MockBrokerMetricsReporter.CLUSTER_META.get().clusterId());


        Map<String, Object> producerConfig = Map.of(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName(),
            "mock.interceptor.append", "mock",
            ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockProducerMetricsReporter.class.getName(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MockSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MockSerializer.class.getName());
        try (var producer = clusterInstance.<byte[], byte[]>producer(producerConfig)) {
            // Send one record and make sure clusterId is set after send and before onAcknowledgement
            sendRecords(producer);
        }

        Assertions.assertNotEquals(MockProducerInterceptor.CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT.get(), MockProducerInterceptor.NO_CLUSTER_ID);
        Assertions.assertNotNull(MockProducerInterceptor.CLUSTER_META);
        Assertions.assertEquals(
            MockProducerInterceptor.CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT.get().clusterId(),
            MockProducerInterceptor.CLUSTER_META.get().clusterId()
        );
        isValidClusterId(MockProducerInterceptor.CLUSTER_META.get().clusterId());

        // Ensure the serializer sees Cluster ID before serialization
        Assertions.assertNotEquals(MockSerializer.CLUSTER_ID_BEFORE_SERIALIZE.get(), MockSerializer.NO_CLUSTER_ID);
        Assertions.assertNotNull(MockSerializer.CLUSTER_META);
        isValidClusterId(MockSerializer.CLUSTER_META.get().clusterId());

        // Producer metric reporter receives cluster id
        Assertions.assertNotNull(MockProducerMetricsReporter.CLUSTER_META);
        isValidClusterId(MockProducerMetricsReporter.CLUSTER_META.get().clusterId());

        Map<String, Object> consumerConfig = Map.of(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName(),
            ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockConsumerMetricsReporter.class.getName(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MockDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MockDeserializer.class.getName(),
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name());
        try (var consumer = clusterInstance.<byte[], byte[]>consumer(consumerConfig)) {
            consumer.assign(Collections.singletonList(TP));
            consumer.seek(TP, 0);
            // Consume one record
            consumeRecords(consumer);
        }

        // Cluster ID in consumer interceptor before onConsume
        Assertions.assertNotEquals(MockConsumerInterceptor.CLUSTER_ID_BEFORE_ON_CONSUME.get(), MockConsumerInterceptor.NO_CLUSTER_ID);
        Assertions.assertNotNull(MockConsumerInterceptor.CLUSTER_META);
        isValidClusterId(MockConsumerInterceptor.CLUSTER_META.get().clusterId());
        Assertions.assertEquals(
            MockConsumerInterceptor.CLUSTER_ID_BEFORE_ON_CONSUME.get().clusterId(),
            MockConsumerInterceptor.CLUSTER_META.get().clusterId()
        );

        // Cluster ID in deserializer before deserialize
        Assertions.assertNotEquals(MockDeserializer.clusterIdBeforeDeserialize.get(), MockDeserializer.noClusterId);
        Assertions.assertNotNull(MockDeserializer.clusterMeta);
        isValidClusterId(MockDeserializer.clusterMeta.get().clusterId());
        Assertions.assertEquals(
            MockDeserializer.clusterIdBeforeDeserialize.get().clusterId(),
            MockDeserializer.clusterMeta.get().clusterId()
        );

        Assertions.assertNotNull(MockConsumerMetricsReporter.CLUSTER_META);
        isValidClusterId(MockConsumerMetricsReporter.CLUSTER_META.get().clusterId());

        // All components received the same cluster id
        String id = MockProducerInterceptor.CLUSTER_META.get().clusterId();
        Assertions.assertEquals(id, MockSerializer.CLUSTER_META.get().clusterId());
        Assertions.assertEquals(id, MockProducerMetricsReporter.CLUSTER_META.get().clusterId());
        Assertions.assertEquals(id, MockConsumerInterceptor.CLUSTER_META.get().clusterId());
        Assertions.assertEquals(id, MockDeserializer.clusterMeta.get().clusterId());
        Assertions.assertEquals(id, MockConsumerMetricsReporter.CLUSTER_META.get().clusterId());
        Assertions.assertEquals(id, MockBrokerMetricsReporter.CLUSTER_META.get().clusterId());

        MockConsumerInterceptor.resetCounters();
        MockProducerInterceptor.resetCounters();
    }

    private static void sendRecords(Producer<byte[], byte[]> producer) throws Exception {
        for (int i = 0; i < NUM_RECORDS; i++) {
            ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(TP.topic(), TP.partition(), String.valueOf(i).getBytes(), String.valueOf(i).getBytes());
            producer.send(record).get();
        }
    }

    private void consumeRecords(Consumer<byte[], byte[]> consumer) throws InterruptedException {
        List<ConsumerRecord<byte[], byte[]>> records = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(records::add);
            return records.size() >= NUM_RECORDS;
        }, 60000, "Timed out before consuming expected " + NUM_RECORDS + " records.");

        for (int i = 0; i < NUM_RECORDS; i++) {
            ConsumerRecord<byte[], byte[]> record = records.get(i);
            Assertions.assertEquals(TOPIC, record.topic());
            Assertions.assertEquals(PARTITION, record.partition());
            Assertions.assertEquals(i, record.offset());
        }
    }
}