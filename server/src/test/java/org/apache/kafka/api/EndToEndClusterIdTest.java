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
import org.apache.kafka.server.config.ServerConfigs;
import org.apache.kafka.server.metrics.MetricConfigs;
import org.apache.kafka.test.MockConsumerInterceptor;
import org.apache.kafka.test.MockDeserializer;
import org.apache.kafka.test.MockMetricsReporter;
import org.apache.kafka.test.MockProducerInterceptor;
import org.apache.kafka.test.MockSerializer;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.BeforeEach;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.kafka.test.TestUtils.isValidClusterId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/** The test cases here verify the following conditions.
 * 1. The ProducerInterceptor receives the cluster id after the onSend() method is called and before onAcknowledgement() method is called.
 * 2. The Serializer receives the cluster id before the serialize() method is called.
 * 3. The producer MetricReporter receives the cluster id after send() method is called on KafkaProducer.
 * 4. The ConsumerInterceptor receives the cluster id before the onConsume() method.
 * 5. The Deserializer receives the cluster id before the deserialize() method is called.
 * 6. The consumer MetricReporter receives the cluster id after poll() is called on KafkaConsumer.
 * 7. The broker MetricReporter receives the cluster id after the broker startup is over.
 * 8. The broker KafkaMetricReporter receives the cluster id after the broker startup is over.
 * 9. All the components receive the same cluster id.
 */
@ClusterTestDefaults(serverProperties = {
    @ClusterConfigProperty(key = MetricConfigs.METRIC_REPORTER_CLASSES_CONFIG, value = "org.apache.kafka.api.EndToEndClusterIdTest$MockCommonMetricsReporter"),
    @ClusterConfigProperty(key = "offsets.topic.replication.factor", value = "1"),
})
public class EndToEndClusterIdTest {

    private static final String TOPIC = "e2etopic";
    private static final int PARTITION = 0;
    private static final TopicPartition TP = new TopicPartition(TOPIC, PARTITION);
    private final ClusterInstance clusterInstance;
    private String clusterBrokerId;
    private String controllerId;
    private static final String PRODUCER_CLIENT_ID = "producerClientId";
    private static final String CONSUMER_CLIENT_ID = "consumerClientId";

    EndToEndClusterIdTest(ClusterInstance clusterInstance) {
        this.clusterInstance = clusterInstance;
    }

    @BeforeEach
    public void setup() throws InterruptedException {
        this.clusterInstance.createTopic(TOPIC, 2, (short) 1);
        clusterBrokerId = String.valueOf(clusterInstance.brokerIds().iterator().next());
        controllerId = String.valueOf(clusterInstance.controllerIds().iterator().next());
        MockDeserializer.resetStaticVariables();
    }

    public static class MockCommonMetricsReporter extends MockMetricsReporter implements ClusterResourceListener {
        public static final Map<String, ClusterResource> CLUSTER_RESOURCE_MAP = new ConcurrentHashMap<>();
        public String brokerId;
        public String controllerId;

        @Override
        public void configure(Map<String, ?> configs) {
            super.configure(configs);

            String roles = (String) configs.get("process.roles");
            if (roles == null) return;

            String id = (String) configs.get(ServerConfigs.BROKER_ID_CONFIG);
            controllerId = roles.contains("controller") ? id : null;
            brokerId    = roles.contains("broker")    ? id : null;
        }

        @Override
        public void onUpdate(ClusterResource clusterMetadata) {
            if (clientId != null) CLUSTER_RESOURCE_MAP.put(clientId, clusterMetadata);
            if (brokerId != null) CLUSTER_RESOURCE_MAP.put(brokerId, clusterMetadata);
            if (controllerId != null) CLUSTER_RESOURCE_MAP.put(controllerId, clusterMetadata);
        }
    }

    @ClusterTest
    public void testEndToEndWithClassicProtocol() throws Exception {
        testEndToEnd(GroupProtocol.CLASSIC);
    }

    @ClusterTest
    public void testEndToEndWithConsumerProtocol() throws Exception {
        testEndToEnd(GroupProtocol.CONSUMER);
    }

    public void testEndToEnd(GroupProtocol groupProtocol) throws Exception {
        MockConsumerInterceptor.resetCounters();
        MockProducerInterceptor.resetCounters();

        ClusterResource brokerClusterResource = MockCommonMetricsReporter.CLUSTER_RESOURCE_MAP.get(clusterBrokerId);
        assertNotNull(brokerClusterResource);
        isValidClusterId(brokerClusterResource.clusterId());
        ClusterResource controllerClusterResource = MockCommonMetricsReporter.CLUSTER_RESOURCE_MAP.get(controllerId);
        assertNotNull(controllerClusterResource);
        isValidClusterId(controllerClusterResource.clusterId());

        Map<String, Object> producerConfig = Map.of(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, MockProducerInterceptor.class.getName(),
            "mock.interceptor.append", "mock",
            ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockCommonMetricsReporter.class.getName(),
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, MockSerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, MockSerializer.class.getName(),
            ProducerConfig.CLIENT_ID_CONFIG, PRODUCER_CLIENT_ID);
        try (var producer = clusterInstance.<String, String>producer(producerConfig)) {
            // Send one record and make sure clusterId is set after sending and before onAcknowledgement
            sendRecord(producer);
        }
        assertNotEquals(MockProducerInterceptor.CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT.get(), MockProducerInterceptor.NO_CLUSTER_ID);
        assertNotNull(MockProducerInterceptor.CLUSTER_META.get());
        assertEquals(
            MockProducerInterceptor.CLUSTER_ID_BEFORE_ON_ACKNOWLEDGEMENT.get().clusterId(),
            MockProducerInterceptor.CLUSTER_META.get().clusterId()
        );
        isValidClusterId(MockProducerInterceptor.CLUSTER_META.get().clusterId());

        // Make sure the serializer sees Cluster ID before serialize method
        assertNotEquals(MockSerializer.CLUSTER_ID_BEFORE_SERIALIZE.get(), MockSerializer.NO_CLUSTER_ID);
        assertNotNull(MockSerializer.CLUSTER_META.get());
        isValidClusterId(MockSerializer.CLUSTER_META.get().clusterId());

        ClusterResource producerClusterResource = MockCommonMetricsReporter.CLUSTER_RESOURCE_MAP.get(PRODUCER_CLIENT_ID);
        assertNotNull(producerClusterResource);
        isValidClusterId(producerClusterResource.clusterId());

        Map<String, Object> consumerConfig = Map.of(ConsumerConfig.INTERCEPTOR_CLASSES_CONFIG, MockConsumerInterceptor.class.getName(),
            ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, MockCommonMetricsReporter.class.getName(),
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, MockDeserializer.class.getName(),
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, MockDeserializer.class.getName(),
            ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.name(),
            ConsumerConfig.CLIENT_ID_CONFIG, CONSUMER_CLIENT_ID);
        try (var consumer = clusterInstance.<String, String>consumer(consumerConfig)) {
            consumer.assign(List.of(TP));
            consumer.seek(TP, 0);
            // consume and verify that values are modified by interceptors
            consumeRecord(consumer);
        }

        // Check that cluster id is present after the first poll call.
        assertNotEquals(MockConsumerInterceptor.CLUSTER_ID_BEFORE_ON_CONSUME.get(), MockConsumerInterceptor.NO_CLUSTER_ID);
        assertNotNull(MockConsumerInterceptor.CLUSTER_META.get());
        isValidClusterId(MockConsumerInterceptor.CLUSTER_META.get().clusterId());
        assertEquals(
            MockConsumerInterceptor.CLUSTER_ID_BEFORE_ON_CONSUME.get().clusterId(),
            MockConsumerInterceptor.CLUSTER_META.get().clusterId()
        );

        assertNotEquals(MockDeserializer.clusterIdBeforeDeserialize.get(), MockDeserializer.noClusterId);
        assertNotNull(MockDeserializer.clusterMeta);
        isValidClusterId(MockDeserializer.clusterMeta.get().clusterId());
        assertEquals(
            MockDeserializer.clusterIdBeforeDeserialize.get().clusterId(),
            MockDeserializer.clusterMeta.get().clusterId()
        );

        ClusterResource consumerClusterResource = MockCommonMetricsReporter.CLUSTER_RESOURCE_MAP.get(CONSUMER_CLIENT_ID);
        assertNotNull(consumerClusterResource);
        isValidClusterId(consumerClusterResource.clusterId());

        // Make sure everyone receives the same cluster id.
        String id = MockProducerInterceptor.CLUSTER_META.get().clusterId();
        assertEquals(id, MockSerializer.CLUSTER_META.get().clusterId());
        assertEquals(id, MockCommonMetricsReporter.CLUSTER_RESOURCE_MAP.get(PRODUCER_CLIENT_ID).clusterId());
        assertEquals(id, MockConsumerInterceptor.CLUSTER_META.get().clusterId());
        assertEquals(id, MockDeserializer.clusterMeta.get().clusterId());
        assertEquals(id, MockCommonMetricsReporter.CLUSTER_RESOURCE_MAP.get(CONSUMER_CLIENT_ID).clusterId());
        assertEquals(id, MockCommonMetricsReporter.CLUSTER_RESOURCE_MAP.get(clusterBrokerId).clusterId());
        assertEquals(id, MockCommonMetricsReporter.CLUSTER_RESOURCE_MAP.get(controllerId).clusterId());

        MockConsumerInterceptor.resetCounters();
        MockProducerInterceptor.resetCounters();
    }

    private static void sendRecord(Producer<String, String> producer) throws Exception {
        ProducerRecord<String, String> record = new ProducerRecord<>(TP.topic(), TP.partition(), "0", "0");
        producer.send(record).get();
    }

    private void consumeRecord(Consumer<String, String> consumer) throws InterruptedException {
        List<ConsumerRecord<String, String>> records = new ArrayList<>();
        TestUtils.waitForCondition(() -> {
            consumer.poll(Duration.ofMillis(100)).forEach(records::add);
            return !records.isEmpty();
        }, 60000, "Timed out before consuming expected record.");

        ConsumerRecord<String, String> record = records.get(0);
        assertEquals(TOPIC, record.topic());
        assertEquals(PARTITION, record.partition());
        assertEquals(0, record.offset());
    }
}