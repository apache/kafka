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

package kafka.admin;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.test.api.ClusterConfigProperty;
import org.apache.kafka.common.test.api.ClusterInstance;
import org.apache.kafka.common.test.api.ClusterTest;
import org.apache.kafka.common.test.api.ClusterTestExtensions;
import org.apache.kafka.common.test.api.Type;
import org.apache.kafka.server.telemetry.ClientTelemetry;
import org.apache.kafka.server.telemetry.ClientTelemetryReceiver;

import org.junit.jupiter.api.extension.ExtendWith;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Arrays.asList;
import static org.apache.kafka.clients.admin.AdminClientConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(value = ClusterTestExtensions.class)
public class ClientTelemetryTest {

    @ClusterTest(
            types = Type.KRAFT, 
            brokers = 3,
            serverProperties = {
                    @ClusterConfigProperty(key = METRIC_REPORTER_CLASSES_CONFIG, value = "kafka.admin.ClientTelemetryTest$GetIdClientTelemetry"),
            })
    public void testClientInstanceId(ClusterInstance clusterInstance) throws InterruptedException, ExecutionException {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        configs.put(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG, true);
        try (Admin admin = Admin.create(configs)) {
            String testTopicName = "test_topic";
            admin.createTopics(Collections.singletonList(new NewTopic(testTopicName, 1, (short) 1)));
            clusterInstance.waitForTopic(testTopicName, 1);

            Map<String, Object> producerConfigs = new HashMap<>();
            producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
            producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

            try (Producer<String, String> producer = new KafkaProducer<>(producerConfigs)) {
                producer.send(new ProducerRecord<>(testTopicName, 0, null, "bar")).get();
                producer.flush();
                Uuid producerClientId = producer.clientInstanceId(Duration.ofSeconds(3));
                assertNotNull(producerClientId);
                assertEquals(producerClientId, producer.clientInstanceId(Duration.ofSeconds(3)));
            }

            Map<String, Object> consumerConfigs = new HashMap<>();
            consumerConfigs.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
            consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
            consumerConfigs.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            consumerConfigs.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

            try (Consumer<String, String> consumer = new KafkaConsumer<>(consumerConfigs)) {
                consumer.assign(Collections.singletonList(new TopicPartition(testTopicName, 0)));
                consumer.seekToBeginning(Collections.singletonList(new TopicPartition(testTopicName, 0)));
                Uuid consumerClientId = consumer.clientInstanceId(Duration.ofSeconds(5));
                //  before poll, the clientInstanceId will return null
                assertNull(consumerClientId);
                List<String> values = new ArrayList<>();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                for (ConsumerRecord<String, String> record : records) {
                    values.add(record.value());
                }
                assertEquals(1, values.size());
                assertEquals("bar", values.get(0));
                consumerClientId = consumer.clientInstanceId(Duration.ofSeconds(3));
                assertNotNull(consumerClientId);
                assertEquals(consumerClientId, consumer.clientInstanceId(Duration.ofSeconds(3)));
            }
            Uuid uuid = admin.clientInstanceId(Duration.ofSeconds(3));
            assertNotNull(uuid);
            assertEquals(uuid, admin.clientInstanceId(Duration.ofSeconds(3)));
        }
    }

    @ClusterTest(types = {Type.CO_KRAFT, Type.KRAFT})
    public void testIntervalMsParser(ClusterInstance clusterInstance) {
        List<String> alterOpts = asList("--bootstrap-server", clusterInstance.bootstrapServers(),
                "--alter", "--entity-type", "client-metrics", "--entity-name", "test", "--add-config", "interval.ms=bbb");
        try (Admin client = clusterInstance.admin()) {
            ConfigCommand.ConfigCommandOptions addOpts = new ConfigCommand.ConfigCommandOptions(toArray(alterOpts));

            Throwable e = assertThrows(ExecutionException.class, () -> ConfigCommand.alterConfig(client, addOpts));
            assertTrue(e.getMessage().contains(InvalidConfigurationException.class.getSimpleName()));
        }
    }

    @ClusterTest(types = Type.KRAFT)
    public void testMetrics(ClusterInstance clusterInstance) {
        Map<String, Object> configs = new HashMap<>();
        configs.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, clusterInstance.bootstrapServers());
        List<String> expectedMetricsName = Arrays.asList("request-size-max", "io-wait-ratio", "response-total",
                "version", "io-time-ns-avg", "network-io-rate");
        try (Admin admin = Admin.create(configs)) {
            Set<String> actualMetricsName = admin.metrics().keySet().stream()
                    .map(MetricName::name)
                    .collect(Collectors.toSet());
            expectedMetricsName.forEach(expectedName -> assertTrue(actualMetricsName.contains(expectedName),
                    String.format("actual metrics name: %s dont contains expected: %s", actualMetricsName,
                            expectedName)));
            assertTrue(actualMetricsName.containsAll(expectedMetricsName));
        }
    }

    private static String[] toArray(List<String>... lists) {
        return Stream.of(lists).flatMap(List::stream).toArray(String[]::new);
    }

    /**
     * We should add a ClientTelemetry into plugins to test the clientInstanceId method Otherwise the
     * {@link  org.apache.kafka.common.protocol.ApiKeys.GET_TELEMETRY_SUBSCRIPTIONS } command will not be supported
     * by the server
     **/
    public static class GetIdClientTelemetry implements ClientTelemetry, MetricsReporter {


        @Override
        public void init(List<KafkaMetric> metrics) {
        }

        @Override
        public void metricChange(KafkaMetric metric) {
        }

        @Override
        public void metricRemoval(KafkaMetric metric) {
        }

        @Override
        public void close() {
        }

        @Override
        public void configure(Map<String, ?> configs) {
        }

        @Override
        public ClientTelemetryReceiver clientReceiver() {
            return (context, payload) -> {
            };
        }
    }

}
