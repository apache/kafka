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
package org.apache.kafka.streams;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.LogCaptureAppender;
import org.apache.kafka.streams.errors.DefaultProductionExceptionHandler;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.LogAndFailExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.internals.UpgradeFromValues;
import org.apache.kafka.streams.processor.FailOnInvalidTimestamp;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.processor.internals.NoOpProcessorWrapper;
import org.apache.kafka.streams.processor.internals.RecordCollectorTest;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.streams.state.BuiltInDslStoreSuppliers;
import org.apache.kafka.streams.utils.TestUtils.RecordingProcessorWrapper;

import org.apache.log4j.Level;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static java.util.Collections.nCopies;
import static org.apache.kafka.common.IsolationLevel.READ_COMMITTED;
import static org.apache.kafka.common.IsolationLevel.READ_UNCOMMITTED;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.StreamsConfig.DEFAULT_DSL_STORE_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.DSL_STORE_SUPPLIERS_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.ENABLE_METRICS_PUSH_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE_V2;
import static org.apache.kafka.streams.StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_KEY_LENGTH;
import static org.apache.kafka.streams.StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_VALUE_LENGTH;
import static org.apache.kafka.streams.StreamsConfig.PROCESSOR_WRAPPER_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.STATE_DIR_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG;
import static org.apache.kafka.streams.StreamsConfig.adminClientPrefix;
import static org.apache.kafka.streams.StreamsConfig.consumerPrefix;
import static org.apache.kafka.streams.StreamsConfig.producerPrefix;
import static org.apache.kafka.streams.internals.StreamsConfigUtils.totalCacheSize;
import static org.apache.kafka.test.StreamsTestUtils.getStreamsConfig;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@Timeout(600)
public class StreamsConfigTest {
    private final Properties props = new Properties();
    private StreamsConfig streamsConfig;

    private final String groupId = "example-application";
    private final String clientId = "client";
    private final int threadIdx = 1;

    @BeforeEach
    public void setUp() {
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, groupId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        props.put("key.deserializer.encoding", StandardCharsets.UTF_8.name());
        props.put("value.deserializer.encoding", StandardCharsets.UTF_16.name());
        streamsConfig = new StreamsConfig(props);
    }

    @Test
    public void testIllegalMetricsRecordingLevel() {
        props.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "illegalConfig");
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void testOsDefaultSocketBufferSizes() {
        props.put(StreamsConfig.SEND_BUFFER_CONFIG, CommonClientConfigs.SEND_BUFFER_LOWER_BOUND);
        props.put(StreamsConfig.RECEIVE_BUFFER_CONFIG, CommonClientConfigs.RECEIVE_BUFFER_LOWER_BOUND);
        new StreamsConfig(props);
    }

    @Test
    public void testInvalidSocketSendBufferSize() {
        props.put(StreamsConfig.SEND_BUFFER_CONFIG, -2);
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void testInvalidSocketReceiveBufferSize() {
        props.put(StreamsConfig.RECEIVE_BUFFER_CONFIG, -2);
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldThrowExceptionIfApplicationIdIsNotSet() {
        props.remove(StreamsConfig.APPLICATION_ID_CONFIG);
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldThrowExceptionIfBootstrapServersIsNotSet() {
        props.remove(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void testGetProducerConfigs() {
        final Map<String, Object> returnedProps = streamsConfig.getProducerConfigs(clientId);
        assertThat(returnedProps.get(ProducerConfig.CLIENT_ID_CONFIG), equalTo(clientId));
        assertThat(returnedProps.get(ProducerConfig.LINGER_MS_CONFIG), equalTo("100"));
    }

    @Test
    public void testGetConsumerConfigs() {
        final Map<String, Object> returnedProps = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertThat(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), equalTo(clientId));
        assertThat(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG), equalTo(groupId));
        assertThat(returnedProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), equalTo("1000"));
        assertNull(returnedProps.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    @Test
    public void testGetGroupInstanceIdConfigs() {
        props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "group-instance-id");
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG), "group-instance-id-1");
        props.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG), "group-instance-id-2");
        props.put(StreamsConfig.globalConsumerPrefix(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG), "group-instance-id-3");
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        Map<String, Object> returnedProps = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertThat(
            returnedProps.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG),
            equalTo("group-instance-id-1-" + threadIdx)
        );

        returnedProps = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertNull(returnedProps.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));

        returnedProps = streamsConfig.getGlobalConsumerConfigs(clientId);
        assertNull(returnedProps.get(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG));
    }

    @Test
    public void consumerConfigMustContainStreamPartitionAssignorConfig() {
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 42);
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, 99L);
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 9);
        props.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, 99_999L);
        props.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, 7L);
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "dummy:host");
        props.put(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG), 100);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> returnedProps = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);

        assertEquals(42, returnedProps.get(StreamsConfig.REPLICATION_FACTOR_CONFIG));
        assertEquals(1, returnedProps.get(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG));
        assertEquals(99L, returnedProps.get(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG));
        assertEquals(9, returnedProps.get(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG));
        assertEquals(99_999L, returnedProps.get(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG));
        assertEquals(
            StreamsPartitionAssignor.class.getName(),
            returnedProps.get(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG)
        );
        assertEquals(7L, returnedProps.get(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG));
        assertEquals("dummy:host", returnedProps.get(StreamsConfig.APPLICATION_SERVER_CONFIG));
        assertEquals(100, returnedProps.get(StreamsConfig.topicPrefix(TopicConfig.SEGMENT_BYTES_CONFIG)));
    }

    @Test
    public void testGetMainConsumerConfigsWithMainConsumerOverriddenPrefix() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "5");
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "50");
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.GROUP_ID_CONFIG), "another-id");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> returnedProps = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertEquals(groupId, returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG));
        assertEquals("50", returnedProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    public void testGetRestoreConsumerConfigs() {
        final Map<String, Object> returnedProps = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), clientId);
        assertNull(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    public void defaultSerdeShouldBeConfigured() {
        final Map<String, Object> serializerConfigs = new HashMap<>();
        serializerConfigs.put("key.serializer.encoding", StandardCharsets.UTF_8.name());
        serializerConfigs.put("value.serializer.encoding", StandardCharsets.UTF_16.name());
        try (final Serializer<String> serializer = new StringSerializer()) {
            final String str = "my string for testing";
            final String topic = "my topic";

            serializer.configure(serializerConfigs, true);
            assertEquals(
                str,
                streamsConfig.defaultKeySerde().deserializer().deserialize(topic, serializer.serialize(topic, str)),
                "Should get the original string after serialization and deserialization with the configured encoding"
            );

            serializer.configure(serializerConfigs, false);
            assertEquals(
                str,
                streamsConfig.defaultValueSerde().deserializer().deserialize(topic, serializer.serialize(topic, str)),
                "Should get the original string after serialization and deserialization with the configured encoding"
            );
        }
    }

    @Test
    public void shouldSupportMultipleBootstrapServers() {
        final List<String> expectedBootstrapServers = Arrays.asList("broker1:9092", "broker2:9092");
        final String bootstrapServersString = String.join(",", expectedBootstrapServers);
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "irrelevant");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServersString);
        final StreamsConfig config = new StreamsConfig(props);

        final List<String> actualBootstrapServers = config.getList(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG);
        assertEquals(expectedBootstrapServers, actualBootstrapServers);
    }

    @Test
    public void shouldSupportPrefixedConsumerConfigs() {
        props.put(consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "earliest");
        props.put(consumerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportPrefixedRestoreConsumerConfigs() {
        props.put(consumerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfConsumerConfig() {
        props.put(consumerPrefix("interceptor.statsd.host"), "host");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertEquals("host", consumerConfigs.get("interceptor.statsd.host"));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfRestoreConsumerConfig() {
        props.put(consumerPrefix("interceptor.statsd.host"), "host");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertEquals("host", consumerConfigs.get("interceptor.statsd.host"));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfProducerConfig() {
        props.put(producerPrefix("interceptor.statsd.host"), "host");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);
        assertEquals("host", producerConfigs.get("interceptor.statsd.host"));
    }

    @Test
    public void shouldSupportPrefixedProducerConfigs() {
        props.put(producerPrefix(ProducerConfig.BUFFER_MEMORY_CONFIG), 10);
        props.put(producerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> configs = streamsConfig.getProducerConfigs(clientId);
        assertEquals(10, configs.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        assertEquals(1, configs.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldBeSupportNonPrefixedConsumerConfigs() {
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertEquals("earliest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldBeSupportNonPrefixedRestoreConsumerConfigs() {
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs(groupId);
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportNonPrefixedProducerConfigs() {
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 10);
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> configs = streamsConfig.getProducerConfigs(clientId);
        assertEquals(10, configs.get(ProducerConfig.BUFFER_MEMORY_CONFIG));
        assertEquals(1, configs.get(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldForwardCustomConfigsWithNoPrefixToAllClients() {
        props.put("custom.property.host", "host");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        final Map<String, Object> restoreConsumerConfigs = streamsConfig.getRestoreConsumerConfigs(clientId);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);
        final Map<String, Object> adminConfigs = streamsConfig.getAdminConfigs(clientId);
        assertEquals("host", consumerConfigs.get("custom.property.host"));
        assertEquals("host", restoreConsumerConfigs.get("custom.property.host"));
        assertEquals("host", producerConfigs.get("custom.property.host"));
        assertEquals("host", adminConfigs.get("custom.property.host"));
    }

    @Test
    public void shouldOverrideNonPrefixedCustomConfigsWithPrefixedConfigs() {
        props.put("custom.property.host", "host0");
        props.put(consumerPrefix("custom.property.host"), "host1");
        props.put(producerPrefix("custom.property.host"), "host2");
        props.put(adminClientPrefix("custom.property.host"), "host3");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        final Map<String, Object> restoreConsumerConfigs = streamsConfig.getRestoreConsumerConfigs(clientId);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);
        final Map<String, Object> adminConfigs = streamsConfig.getAdminConfigs(clientId);
        assertEquals("host1", consumerConfigs.get("custom.property.host"));
        assertEquals("host1", restoreConsumerConfigs.get("custom.property.host"));
        assertEquals("host2", producerConfigs.get("custom.property.host"));
        assertEquals("host3", adminConfigs.get("custom.property.host"));
    }

    @Test
    public void shouldOverrideAdminDefaultAdminClientEnableTelemetry() {
        final Map<String, Object> returnedProps = streamsConfig.getAdminConfigs(clientId);
        assertTrue((boolean) returnedProps.get(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG));
    }

    @Test
    public void shouldSupportNonPrefixedAdminConfigs() {
        props.put(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG, 10);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> configs = streamsConfig.getAdminConfigs(clientId);
        assertEquals(10, configs.get(AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG));
    }

    @Test
    public void shouldThrowStreamsExceptionIfKeySerdeConfigFails() {
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        assertThrows(StreamsException.class, streamsConfig::defaultKeySerde);
    }

    @Test
    public void shouldThrowStreamsExceptionIfValueSerdeConfigFails() {
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        assertThrows(StreamsException.class, streamsConfig::defaultValueSerde);
    }

    @Test
    public void shouldOverrideStreamsDefaultConsumerConfigs() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG), "latest");
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "10");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertEquals("latest", consumerConfigs.get(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG));
        assertEquals("10", consumerConfigs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    public void shouldOverrideStreamsDefaultProducerConfigs() {
        props.put(StreamsConfig.producerPrefix(ProducerConfig.LINGER_MS_CONFIG), "10000");
        props.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), "30000");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);
        assertEquals("10000", producerConfigs.get(ProducerConfig.LINGER_MS_CONFIG));
        assertEquals("30000", producerConfigs.get(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG));
    }

    @Test
    public void shouldNotThrowIfTransactionTimeoutSmallerThanCommitIntervalForAtLeastOnce() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10000L);
        props.put(StreamsConfig.producerPrefix(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), 3000);
        new StreamsConfig(props);
    }

    @Test
    public void shouldOverrideStreamsDefaultConsumerConfigsOnRestoreConsumer() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "10");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertEquals("10", consumerConfigs.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    public void shouldResetToDefaultIfConsumerAutoCommitIsOverridden() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "true");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs("a", "b", threadIdx);
        assertEquals("false", consumerConfigs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    public void shouldResetToDefaultIfRestoreConsumerAutoCommitIsOverridden() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "true");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertEquals("false", consumerConfigs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    public void shouldResetToDefaultIfConsumerGroupProtocolIsOverridden() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.GROUP_PROTOCOL_CONFIG), "consumer");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs("a", "b", threadIdx);
        assertEquals("classic", consumerConfigs.get(ConsumerConfig.GROUP_PROTOCOL_CONFIG));
    }

    @Test
    public void shouldResetToDefaultIfRestoreConsumerGroupProtocolIsOverridden() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.GROUP_PROTOCOL_CONFIG), "consumer");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertEquals("classic", consumerConfigs.get(ConsumerConfig.GROUP_PROTOCOL_CONFIG));
    }

    @Test
    public void testGetRestoreConsumerConfigsWithRestoreConsumerOverriddenPrefix() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "5");
        props.put(StreamsConfig.restoreConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "50");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> returnedProps = streamsConfig.getRestoreConsumerConfigs(clientId);
        assertEquals("50", returnedProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    public void testGetGlobalConsumerConfigs() {
        final Map<String, Object> returnedProps = streamsConfig.getGlobalConsumerConfigs(clientId);
        assertEquals(returnedProps.get(ConsumerConfig.CLIENT_ID_CONFIG), clientId + "-global-consumer");
        assertNull(returnedProps.get(ConsumerConfig.GROUP_ID_CONFIG));
    }

    @Test
    public void shouldSupportPrefixedGlobalConsumerConfigs() {
        props.put(consumerPrefix(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG), 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getGlobalConsumerConfigs(clientId);
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldSupportPrefixedPropertiesThatAreNotPartOfGlobalConsumerConfig() {
        props.put(consumerPrefix("interceptor.statsd.host"), "host");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getGlobalConsumerConfigs(clientId);
        assertEquals("host", consumerConfigs.get("interceptor.statsd.host"));
    }

    @Test
    public void shouldBeSupportNonPrefixedGlobalConsumerConfigs() {
        props.put(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG, 1);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getGlobalConsumerConfigs(groupId);
        assertEquals(1, consumerConfigs.get(ConsumerConfig.METRICS_NUM_SAMPLES_CONFIG));
    }

    @Test
    public void shouldResetToDefaultIfGlobalConsumerAutoCommitIsOverridden() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG), "true");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getGlobalConsumerConfigs(clientId);
        assertEquals("false", consumerConfigs.get(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG));
    }

    @Test
    public void shouldResetToDefaultIfGlobalConsumerGroupProtocolIsOverridden() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.GROUP_PROTOCOL_CONFIG), "consumer");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getGlobalConsumerConfigs(clientId);
        assertEquals("classic", consumerConfigs.get(ConsumerConfig.GROUP_PROTOCOL_CONFIG));
    }

    @Test
    public void testGetGlobalConsumerConfigsWithGlobalConsumerOverriddenPrefix() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "5");
        props.put(StreamsConfig.globalConsumerPrefix(ConsumerConfig.MAX_POLL_RECORDS_CONFIG), "50");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> returnedProps = streamsConfig.getGlobalConsumerConfigs(clientId);
        assertEquals("50", returnedProps.get(ConsumerConfig.MAX_POLL_RECORDS_CONFIG));
    }

    @Test
    public void shouldSetInternalLeaveGroupOnCloseConfigToFalseInConsumer() {
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertThat(consumerConfigs.get("internal.leave.group.on.close"), is(false));
    }

    @Test
    public void shouldNotSetInternalThrowOnFetchStableOffsetUnsupportedConfigToFalseInConsumerForEosDisabled() {
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertThat(consumerConfigs.get("internal.throw.on.fetch.stable.offset.unsupported"), is(nullValue()));
    }

    @Test
    public void shouldNotSetInternalThrowOnFetchStableOffsetUnsupportedConfigToFalseInConsumerForEosV2() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertThat(consumerConfigs.get("internal.throw.on.fetch.stable.offset.unsupported"), is(true));
    }

    @Test
    public void shouldNotSetInternalAutoDowngradeTxnCommitToTrueInProducerForEosV2() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);
        assertThat(producerConfigs.get("internal.auto.downgrade.txn.commit"), is(nullValue()));
    }

    @Test
    public void shouldAcceptAtLeastOnce() {
        // don't use `StreamsConfig.AT_LEAST_ONCE` to actually do a useful test
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "at_least_once");
        new StreamsConfig(props);
    }

    @Test
    public void shouldThrowExceptionIfNotAtLeastOnceOrExactlyOnce() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "bad_value");
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldAcceptBuiltInMetricsLatestVersion() {
        // don't use `StreamsConfig.METRICS_LATEST` to actually do a useful test
        props.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, "latest");
        new StreamsConfig(props);
    }

    @Test
    public void shouldSetDefaultBuiltInMetricsVersionIfNoneIsSpecified() {
        final StreamsConfig config = new StreamsConfig(props);
        assertThat(config.getString(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG), is(StreamsConfig.METRICS_LATEST));
    }

    @Test
    public void shouldThrowIfBuiltInMetricsVersionInvalid() {
        final String invalidVersion = "0.0.1";
        props.put(StreamsConfig.BUILT_IN_METRICS_VERSION_CONFIG, invalidVersion);
        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertThat(
            exception.getMessage(),
            containsString("Invalid value " + invalidVersion + " for configuration built.in.metrics.version")
        );
    }

    @Test
    public void shouldResetToDefaultIfConsumerIsolationLevelIsOverriddenIfEosV2Enabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "anyValue");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertThat(
            consumerConfigs.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG),
            equalTo(READ_COMMITTED.toString())
        );
    }

    @Test
    public void shouldAllowSettingConsumerIsolationLevelIfEosDisabled() {
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, READ_UNCOMMITTED.toString());
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        assertThat(
            consumerConfigs.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG),
            equalTo(READ_UNCOMMITTED.toString())
        );
    }

    @Test
    public void shouldResetToDefaultIfProducerEnableIdempotenceIsOverriddenIfEosV2Enabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "anyValue");
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);
        assertTrue((Boolean) producerConfigs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
    }

    @Test
    public void shouldAllowSettingProducerEnableIdempotenceIfEosDisabled() {
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);
        assertThat(producerConfigs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG), equalTo(false));
    }

    @Test
    public void shouldSetDifferentDefaultsIfEosV2Enabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        final Map<String, Object> consumerConfigs = streamsConfig.getMainConsumerConfigs(groupId, clientId, threadIdx);
        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);

        assertThat(
            consumerConfigs.get(ConsumerConfig.ISOLATION_LEVEL_CONFIG),
            equalTo(READ_COMMITTED.toString())
        );
        assertTrue((Boolean) producerConfigs.get(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG));
        assertThat(producerConfigs.get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG), equalTo(Integer.MAX_VALUE));
        assertThat(producerConfigs.get(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG), equalTo(10000));
        assertThat(streamsConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG), equalTo(100L));
    }

    @Test
    public void shouldOverrideUserConfigTransactionalIdIfEosV2Enabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "user-TxId");
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);

        assertThat(producerConfigs.get(ProducerConfig.TRANSACTIONAL_ID_CONFIG), is(nullValue()));
    }

    @Test
    public void shouldNotOverrideUserConfigRetriesIfExactlyV2OnceEnabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        final int numberOfRetries = 42;
        props.put(ProducerConfig.RETRIES_CONFIG, numberOfRetries);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        final Map<String, Object> producerConfigs = streamsConfig.getProducerConfigs(clientId);

        assertThat(producerConfigs.get(ProducerConfig.RETRIES_CONFIG), equalTo(numberOfRetries));
    }

    @Test
    public void shouldNotOverrideUserConfigCommitIntervalMsIfExactlyOnceV2Enabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        final long commitIntervalMs = 73L;
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        assertThat(streamsConfig.getLong(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG), equalTo(commitIntervalMs));
    }

    @Test
    public void shouldThrowExceptionIfCommitIntervalMsIsNegative() {
        final long commitIntervalMs = -1;
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, commitIntervalMs);
        try {
            new StreamsConfig(props);
            fail("Should throw ConfigException when commitIntervalMs is set to a negative value");
        } catch (final ConfigException e) {
            assertEquals(
                "Invalid value -1 for configuration commit.interval.ms: Value must be at least 0",
                e.getMessage()
            );
        }
    }

    @Test
    public void shouldUseNewConfigsWhenPresent() {
        final Properties props = getStreamsConfig();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.LongSerde.class);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, MockTimestampExtractor.class);

        final StreamsConfig config = new StreamsConfig(props);
        assertInstanceOf(Serdes.LongSerde.class, config.defaultKeySerde());
        assertInstanceOf(Serdes.LongSerde.class, config.defaultValueSerde());
        assertInstanceOf(MockTimestampExtractor.class, config.defaultTimestampExtractor());
    }

    @Test
    public void shouldUseCorrectDefaultsWhenNoneSpecified() {
        final StreamsConfig config = new StreamsConfig(getStreamsConfig());

        assertInstanceOf(FailOnInvalidTimestamp.class, config.defaultTimestampExtractor());
        assertThrows(ConfigException.class, config::defaultKeySerde);
        assertThrows(ConfigException.class, config::defaultValueSerde);
    }

    @Test
    public void shouldSpecifyCorrectKeySerdeClassOnError() {
        final Properties props = getStreamsConfig();
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig config = new StreamsConfig(props);
        try {
            config.defaultKeySerde();
            fail("Test should throw a StreamsException");
        } catch (final StreamsException e) {
            assertEquals(
                "Failed to configure key serde class org.apache.kafka.streams.StreamsConfigTest$MisconfiguredSerde",
                e.getMessage()
            );
        }
    }

    @Test
    public void shouldSpecifyCorrectValueSerdeClassOnError() {
        final Properties props = getStreamsConfig();
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, MisconfiguredSerde.class);
        final StreamsConfig config = new StreamsConfig(props);
        try {
            config.defaultValueSerde();
            fail("Test should throw a StreamsException");
        } catch (final StreamsException e) {
            assertEquals(
                "Failed to configure value serde class org.apache.kafka.streams.StreamsConfigTest$MisconfiguredSerde",
                e.getMessage()
            );
        }
    }

    @Test
    public void shouldThrowExceptionIfMaxInFlightRequestsGreaterThanFiveIfEosV2Enabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 7);
        final StreamsConfig streamsConfig = new StreamsConfig(props);
        try {
            streamsConfig.getProducerConfigs(clientId);
            fail("Should throw ConfigException when ESO is enabled and maxInFlight requests exceeds 5");
        } catch (final ConfigException e) {
            assertEquals(
                "Invalid value 7 for configuration max.in.flight.requests.per.connection:" +
                    " Can't exceed 5 when exactly-once processing is enabled",
                e.getMessage()
            );
        }
    }

    @Test
    public void shouldAllowToSpecifyMaxInFlightRequestsPerConnectionAsStringIfEosV2Enabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "3");

        new StreamsConfig(props).getProducerConfigs(clientId);
    }

    @Test
    public void shouldThrowConfigExceptionIfMaxInFlightRequestsPerConnectionIsInvalidStringIfEosV2Enabled() {
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, EXACTLY_ONCE_V2);
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "not-a-number");

        try {
            new StreamsConfig(props).getProducerConfigs(clientId);
            fail("Should throw ConfigException when EOS is enabled and maxInFlight cannot be paresed into an integer");
        } catch (final ConfigException e) {
            assertEquals(
                "Invalid value not-a-number for configuration max.in.flight.requests.per.connection:" +
                " String value could not be parsed as 32-bit integer",
                e.getMessage()
            );
        }
    }

    @Test
    public void shouldStateDirStartsWithJavaIOTmpDir() {
        final String expectedPrefix = System.getProperty("java.io.tmpdir") + File.separator;
        final String actual = streamsConfig.getString(STATE_DIR_CONFIG);
        assertTrue(actual.startsWith(expectedPrefix));
    }

    @Test
    public void shouldSpecifyNoOptimizationWhenNotExplicitlyAddedToConfigs() {
        final String expectedOptimizeConfig = "none";
        final String actualOptimizedConifig = streamsConfig.getString(TOPOLOGY_OPTIMIZATION_CONFIG);
        assertEquals(expectedOptimizeConfig, actualOptimizedConifig, "Optimization should be \"none\"");
    }

    @Test
    public void shouldSpecifyOptimizationWhenExplicitlyAddedToConfigs() {
        final String expectedOptimizeConfig = "all";
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, "all");
        final StreamsConfig config = new StreamsConfig(props);
        final String actualOptimizedConifig = config.getString(TOPOLOGY_OPTIMIZATION_CONFIG);
        assertEquals(expectedOptimizeConfig, actualOptimizedConifig, "Optimization should be \"all\"");
    }

    @Test
    public void shouldThrowConfigExceptionWhenOptimizationConfigNotValueInRange() {
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, "maybe");
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldSpecifyRocksdbWhenNotExplicitlyAddedToConfigs() {
        final String expectedDefaultStoreType = StreamsConfig.ROCKS_DB;
        final String actualDefaultStoreType = streamsConfig.getString(DEFAULT_DSL_STORE_CONFIG);
        assertEquals(expectedDefaultStoreType, actualDefaultStoreType, "default.dsl.store should be \"rocksDB\"");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldSpecifyInMemoryWhenExplicitlyAddedToConfigs() {
        final String expectedDefaultStoreType = StreamsConfig.IN_MEMORY;
        props.put(DEFAULT_DSL_STORE_CONFIG, expectedDefaultStoreType);
        final StreamsConfig config = new StreamsConfig(props);
        final String actualDefaultStoreType = config.getString(DEFAULT_DSL_STORE_CONFIG);
        assertEquals(expectedDefaultStoreType, actualDefaultStoreType, "default.dsl.store should be \"in_memory\"");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldThrowConfigExceptionWhenStoreTypeConfigNotValueInRange() {
        props.put(DEFAULT_DSL_STORE_CONFIG, "bad_config");
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldSpecifyRocksdbDslSupplierWhenNotExplicitlyAddedToConfigs() {
        final Class<?> expectedDefaultStoreType = BuiltInDslStoreSuppliers.RocksDBDslStoreSuppliers.class;
        final Class<?> actualDefaultStoreType = streamsConfig.getClass(DSL_STORE_SUPPLIERS_CLASS_CONFIG);
        assertEquals(
            expectedDefaultStoreType,
            actualDefaultStoreType,
            "default " + DSL_STORE_SUPPLIERS_CLASS_CONFIG + " should be " + expectedDefaultStoreType
        );
    }

    @Test
    public void shouldSpecifyInMemoryDslSupplierWhenExplicitlyAddedToConfigs() {
        final Class<?> expectedDefaultStoreType = BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class;
        props.put(DSL_STORE_SUPPLIERS_CLASS_CONFIG, BuiltInDslStoreSuppliers.InMemoryDslStoreSuppliers.class);
        final StreamsConfig config = new StreamsConfig(props);
        final Class<?> actualDefaultStoreType = config.getClass(DSL_STORE_SUPPLIERS_CLASS_CONFIG);
        assertEquals(
            expectedDefaultStoreType,
            actualDefaultStoreType,
            "default " + DSL_STORE_SUPPLIERS_CLASS_CONFIG + " should be " + expectedDefaultStoreType
        );
    }

    @Test
    public void shouldSetDefaultAcceptableRecoveryLag() {
        final StreamsConfig config = new StreamsConfig(props);
        assertThat(config.getLong(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG), is(10000L));
    }

    @Test
    public void shouldThrowConfigExceptionIfAcceptableRecoveryLagIsOutsideBounds() {
        props.put(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, -1L);
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldSetDefaultNumStandbyReplicas() {
        final StreamsConfig config = new StreamsConfig(props);
        assertThat(config.getInt(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG), is(0));
    }

    @Test
    public void shouldThrowConfigExceptionIfNumStandbyReplicasIsOutsideBounds() {
        props.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, -1L);
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldSetDefaultMaxWarmupReplicas() {
        final StreamsConfig config = new StreamsConfig(props);
        assertThat(config.getInt(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG), is(2));
    }

    @Test
    public void shouldThrowConfigExceptionIfMaxWarmupReplicasIsOutsideBounds() {
        props.put(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, 0L);
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldSetDefaultProbingRebalanceInterval() {
        final StreamsConfig config = new StreamsConfig(props);
        assertThat(config.getLong(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG), is(10 * 60 * 1000L));
    }

    @Test
    public void shouldThrowConfigExceptionIfProbingRebalanceIntervalIsOutsideBounds() {
        props.put(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, (60 * 1000L) - 1);
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldDefaultToEmptyListIfRackAwareAssignmentTagsIsNotSet() {
        final StreamsConfig config = new StreamsConfig(props);
        assertTrue(config.getList(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG).isEmpty());
    }

    @Test
    public void shouldThrowExceptionWhenClientTagsExceedTheLimit() {
        final int limit = StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE + 1;
        for (int i = 0; i < limit; i++) {
            props.put(StreamsConfig.clientTagPrefix("k" + i), "v" + i);
        }
        final ConfigException exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertEquals(
            String.format("At most %s client tags can be specified using %s prefix.",
                          StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE,
                          StreamsConfig.CLIENT_TAG_PREFIX
            ), exception.getMessage()
        );
    }

    @Test
    public void shouldThrowExceptionWhenRackAwareAssignmentTagsExceedsMaxListSize() {
        final int limit = StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE + 1;
        final List<String> rackAwareAssignmentTags = new ArrayList<>();
        for (int i = 0; i < limit; i++) {
            final String clientTagKey = "k" + i;
            rackAwareAssignmentTags.add(clientTagKey);
            props.put(StreamsConfig.clientTagPrefix(clientTagKey), "v" + i);
        }

        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, String.join(",", rackAwareAssignmentTags));
        final ConfigException exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertEquals(
            String.format("Invalid value %s for configuration %s: exceeds maximum list size of [%s].",
                          rackAwareAssignmentTags,
                          StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG,
                          StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_LIST_SIZE),
            exception.getMessage()
        );
    }

    @Test
    public void shouldSetRackAwareAssignmentTags() {
        props.put(StreamsConfig.clientTagPrefix("cluster"), "cluster-1");
        props.put(StreamsConfig.clientTagPrefix("zone"), "eu-central-1a");
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, "cluster,zone");
        final StreamsConfig config = new StreamsConfig(props);
        assertEquals(new HashSet<>(config.getList(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG)),
                     Set.of("cluster", "zone"));
    }

    @Test
    public void shouldGetEmptyMapIfClientTagsAreNotSet() {
        final StreamsConfig config = new StreamsConfig(props);
        assertTrue(config.getClientTags().isEmpty());
    }

    @Test
    public void shouldGetClientTagsMapWhenSet() {
        props.put(StreamsConfig.clientTagPrefix("zone"), "eu-central-1a");
        props.put(StreamsConfig.clientTagPrefix("cluster"), "cluster-1");
        final StreamsConfig config = new StreamsConfig(props);
        final Map<String, String> clientTags = config.getClientTags();
        assertEquals(clientTags.size(), 2);
        assertEquals(clientTags.get("zone"), "eu-central-1a");
        assertEquals(clientTags.get("cluster"), "cluster-1");
    }

    @Test
    public void shouldThrowExceptionWhenClientTagRackAwarenessIsConfiguredWithUnknownTags() {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, "cluster");
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldThrowExceptionWhenClientTagKeyExceedMaxLimit() {
        final String key = String.join("", nCopies(MAX_RACK_AWARE_ASSIGNMENT_TAG_KEY_LENGTH + 1, "k"));
        props.put(StreamsConfig.clientTagPrefix(key), "eu-central-1a");
        final ConfigException exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertEquals(
            String.format("Invalid value %s for configuration %s: Tag key exceeds maximum length of %s.",
                          key, StreamsConfig.CLIENT_TAG_PREFIX, StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_KEY_LENGTH),
            exception.getMessage()
        );
    }

    @Test
    public void shouldThrowExceptionWhenClientTagValueExceedMaxLimit() {
        final String value = String.join("", nCopies(MAX_RACK_AWARE_ASSIGNMENT_TAG_VALUE_LENGTH + 1, "v"));
        props.put(StreamsConfig.clientTagPrefix("x"), value);
        final ConfigException exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertEquals(
            String.format("Invalid value %s for configuration %s: Tag value exceeds maximum length of %s.",
                          value, StreamsConfig.CLIENT_TAG_PREFIX, StreamsConfig.MAX_RACK_AWARE_ASSIGNMENT_TAG_VALUE_LENGTH),
            exception.getMessage()
        );
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldUseStateStoreCacheMaxBytesWhenBothOldAndNewConfigsAreSet() {
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 100);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10);
        final StreamsConfig config = new StreamsConfig(props);
        assertEquals(totalCacheSize(config), 100);
    }

    @Test
    @SuppressWarnings("deprecation")
    public void shouldUseCacheMaxBytesBufferingConfigWhenOnlyDeprecatedConfigIsSet() {
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10);
        final StreamsConfig config = new StreamsConfig(props);
        assertEquals(totalCacheSize(config), 10);
    }

    @Test
    public void shouldUseStateStoreCacheMaxBytesWhenNewConfigIsSet() {
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 10);
        final StreamsConfig config = new StreamsConfig(props);
        assertEquals(totalCacheSize(config), 10);
    }

    @Test
    public void shouldUseDefaultStateStoreCacheMaxBytesConfigWhenNoConfigIsSet() {
        final StreamsConfig config = new StreamsConfig(props);
        assertEquals(totalCacheSize(config), 10 * 1024 * 1024);
    }

    @Test
    public void testCaseInsensitiveSecurityProtocol() {
        final String saslSslLowerCase = SecurityProtocol.SASL_SSL.name.toLowerCase(Locale.ROOT);
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, saslSslLowerCase);
        final StreamsConfig config = new StreamsConfig(props);
        assertEquals(saslSslLowerCase, config.originalsStrings().get(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void testInvalidSecurityProtocol() {
        props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "abc");
        final ConfigException ce = assertThrows(ConfigException.class,
                () -> new StreamsConfig(props));
        assertTrue(ce.getMessage().contains(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG));
    }

    @Test
    public void shouldThrowExceptionWhenTopologyOptimizationOnAndOff() {
        final String value = String.join(",", StreamsConfig.OPTIMIZE, StreamsConfig.NO_OPTIMIZATION);
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, value);
        final ConfigException exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertTrue(exception.getMessage().contains("is not a valid optimization config"));
    }

    @Test
    public void shouldThrowExceptionWhenTopologyOptimizationOffAndSet() {
        final String value = String.join(",", StreamsConfig.NO_OPTIMIZATION, StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS);
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, value);
        final ConfigException exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertTrue(exception.getMessage().contains("is not a valid optimization config"));
    }

    @Test
    public void shouldThrowExceptionWhenOptimizationDoesNotExistInList() {
        final String value = String.join(",",
                                         StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS,
                                         "topology.optimization.does.not.exist",
                                         StreamsConfig.MERGE_REPARTITION_TOPICS);
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, value);
        final ConfigException exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertTrue(exception.getMessage().contains("Unrecognized config."));
    }

    @Test
    public void shouldThrowExceptionWhenTopologyOptimizationDoesNotExist() {
        final String value = String.join(",", "topology.optimization.does.not.exist");
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, value);
        final ConfigException exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));
        assertTrue(exception.getMessage().contains("Unrecognized config."));
    }

    @Test
    public void shouldEnableSelfJoin() {
        final String value = StreamsConfig.SINGLE_STORE_SELF_JOIN;
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, value);
        final StreamsConfig config = new StreamsConfig(props);
        assertEquals(config.getString(TOPOLOGY_OPTIMIZATION_CONFIG), StreamsConfig.SINGLE_STORE_SELF_JOIN);
    }

    @Test
    public void shouldAllowMultipleOptimizations() {
        final String value = String.join(",",
                                         StreamsConfig.SINGLE_STORE_SELF_JOIN,
                                         StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS,
                                         StreamsConfig.MERGE_REPARTITION_TOPICS);
        props.put(TOPOLOGY_OPTIMIZATION_CONFIG, value);
        final StreamsConfig config = new StreamsConfig(props);
        final List<String> configs = Arrays.asList(config.getString(TOPOLOGY_OPTIMIZATION_CONFIG).split(","));
        assertEquals(3, configs.size());
        assertTrue(configs.contains(StreamsConfig.SINGLE_STORE_SELF_JOIN));
        assertTrue(configs.contains(StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS));
        assertTrue(configs.contains(StreamsConfig.MERGE_REPARTITION_TOPICS));
    }

    @Test
    public void shouldEnableAllOptimizationsWithOptimizeConfig() {
        final Set<String> configs = StreamsConfig.verifyTopologyOptimizationConfigs(StreamsConfig.OPTIMIZE);
        assertEquals(3, configs.size());
        assertTrue(configs.contains(StreamsConfig.REUSE_KTABLE_SOURCE_TOPICS));
        assertTrue(configs.contains(StreamsConfig.MERGE_REPARTITION_TOPICS));
        assertTrue(configs.contains(StreamsConfig.SINGLE_STORE_SELF_JOIN));
    }

    @Test
    public void shouldNotEnableAnyOptimizationsWithNoOptimizationConfig() {
        final Set<String> configs = StreamsConfig.verifyTopologyOptimizationConfigs(StreamsConfig.NO_OPTIMIZATION);
        assertEquals(0, configs.size());
    }

    @Test
    public void shouldReturnDefaultRackAwareAssignmentConfig() {
        final String strategy = streamsConfig.getString(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG);
        assertEquals("none", strategy);
    }

    @Test
    public void shouldtSetMinTrafficRackAwareAssignmentConfig() {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC);
        assertEquals("min_traffic", new StreamsConfig(props).getString(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG));
    }

    @Test
    public void shouldtSetBalanceSubtopologyRackAwareAssignmentConfig() {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_BALANCE_SUBTOPOLOGY);
        assertEquals("balance_subtopology", new StreamsConfig(props).getString(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG));
    }

    @Test
    public void shouldThrowIfNotSetCorrectRackAwareAssignmentConfig() {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, "invalid");
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldReturnDefaultRackAwareAssignmentTrafficCost() {
        final Integer cost = streamsConfig.getInt(StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG);
        assertNull(cost);
    }

    @Test
    public void shouldReturnRackAwareAssignmentTrafficCost() {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG, "10");
        assertEquals(Integer.valueOf(10), new StreamsConfig(props).getInt(RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG));
    }

    @Test
    public void shouldReturnDefaultRackAwareAssignmentNonOverlapCost() {
        final Integer cost = streamsConfig.getInt(StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG);
        assertNull(cost);
    }

    @Test
    public void shouldReturnRackAwareAssignmentNonOverlapCost() {
        props.put(StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG, "10");
        assertEquals(Integer.valueOf(10), new StreamsConfig(props).getInt(RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG));
    }

    @Test
    public void shouldReturnTaskAssignorClass() {
        props.put(StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG, "LegacyStickyTaskAssignor");
        assertEquals("LegacyStickyTaskAssignor", new StreamsConfig(props).getString(TASK_ASSIGNOR_CLASS_CONFIG));
    }

    @Test
    public void shouldReturnDefaultClientSupplier() {
        final KafkaClientSupplier supplier = streamsConfig.getKafkaClientSupplier();
        assertInstanceOf(DefaultKafkaClientSupplier.class, supplier);
    }

    @Test
    public void shouldThrowOnInvalidClientSupplier() {
        props.put(StreamsConfig.DEFAULT_CLIENT_SUPPLIER_CONFIG, "invalid.class");
        assertThrows(ConfigException.class, () -> new StreamsConfig(props));
    }

    @Test
    public void shouldReturnDefaultProcessorWrapperClass() {
        final String defaultWrapperClassName = streamsConfig.getClass(PROCESSOR_WRAPPER_CLASS_CONFIG).getName();
        assertThat(defaultWrapperClassName, equalTo(NoOpProcessorWrapper.class.getName()));
    }

    @Test
    public void shouldAllowConfiguringProcessorWrapperWithClass() {
        props.put(StreamsConfig.PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class);
        new StreamsConfig(props);
    }

    @Test
    public void shouldAllowConfiguringProcessorWrapperWithClassName() {
        props.put(StreamsConfig.PROCESSOR_WRAPPER_CLASS_CONFIG, RecordingProcessorWrapper.class.getName());
        new StreamsConfig(props);
    }

    @Test
    public void shouldSupportAllUpgradeFromValues() {
        for (final UpgradeFromValues upgradeFrom : UpgradeFromValues.values()) {
            props.put(StreamsConfig.UPGRADE_FROM_CONFIG, upgradeFrom.toString());
            try {
                new StreamsConfig(props);
            } catch (final Exception fatal) {
                throw new AssertionError("StreamsConfig did not accept `upgrade.from` config value `" + upgradeFrom + "`");
            }
        }
    }

    @Test
    public void shouldNotSetEnableMetricCollectionByDefault() {
        assertNull(
            streamsConfig.getMainConsumerConfigs("groupId", "clientId", 0)
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertNull(
            streamsConfig.getRestoreConsumerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertNull(
            streamsConfig.getGlobalConsumerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertNull(
            streamsConfig.getProducerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
    }

    @Test
    public void shouldEnableMetricCollectionForAllInternalClientsByDefault() {
        props.put(ENABLE_METRICS_PUSH_CONFIG, true);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        assertTrue(
            (Boolean) streamsConfig.getMainConsumerConfigs("groupId", "clientId", 0)
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertTrue(
            (Boolean) streamsConfig.getRestoreConsumerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertTrue(
            (Boolean) streamsConfig.getGlobalConsumerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertTrue(
            (Boolean) streamsConfig.getProducerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertTrue(
            (Boolean) streamsConfig.getAdminConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
    }

    @Test
    public void shouldDisableMetricCollectionForAllInternalClients() {
        props.put(ENABLE_METRICS_PUSH_CONFIG, false);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        assertFalse(
            (Boolean) streamsConfig.getMainConsumerConfigs("groupId", "clientId", 0)
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertFalse(
            (Boolean) streamsConfig.getRestoreConsumerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertFalse(
            (Boolean) streamsConfig.getGlobalConsumerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertFalse(
            (Boolean) streamsConfig.getProducerConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
        assertFalse(
            (Boolean) streamsConfig.getAdminConfigs("clientId")
                .get(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG)
        );
    }

    @Test
    public void shouldThrowConfigExceptionWhenMainConsumerMetricsDisabledStreamsMetricsPushEnabled() {
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), false);

        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));

        assertThat(
                exception.getMessage(),
                containsString("main.consumer metrics push is disabled")
        );
    }

    @Test
    public void shouldThrowConfigExceptionConsumerMetricsDisabledStreamsMetricsPushEnabled() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), false);

        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));

        assertThat(
                exception.getMessage(),
                containsString("Kafka Streams metrics push enabled but consumer.enable.metrics is false, the setting needs to be consistent between the two")
        );
    }

    @Test
    public void shouldThrowConfigExceptionConsumerMetricsEnabledButMainConsumerAndAdminMetricsDisabled() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), true);
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), false);
        props.put(StreamsConfig.adminClientPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), false);

        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));

        assertThat(
                exception.getMessage(),
                containsString("Kafka Streams metrics push and consumer.enable.metrics are enabled, but main.consumer and admin.client metrics push are disabled")
        );
    }

    @Test
    public void shouldThrowConfigExceptionConsumerMetricsEnabledButMainConsumerMetricsDisabled() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), true);
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), false);

        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));

        assertThat(
                exception.getMessage(),
                containsString("main.consumer metrics push is disabled")
        );
    }

    @Test
    public void shouldThrowConfigExceptionConsumerMetricsEnabledButAdminClientMetricsDisabled() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), true);
        props.put(StreamsConfig.adminClientPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), false);

        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));

        assertThat(
                exception.getMessage(),
                containsString("admin.client metrics push is disabled")
        );
    }

    @Test
    public void shouldEnableMetricsForMainConsumerWhenConsumerPrefixDisabledMetricsPushEnabled() {
        props.put(StreamsConfig.consumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), false);
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), true);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        assertTrue(
                (Boolean) streamsConfig.getMainConsumerConfigs("groupId", "clientId", 0)
                        .get(StreamsConfig.mainConsumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG))
        );
    }

    @Test
    public void shouldEnableMetricsForMainConsumerWhenStreamMetricsPushDisabledButMainConsumerEnabled() {
        props.put(StreamsConfig.ENABLE_METRICS_PUSH_CONFIG, false);
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), true);
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        assertTrue(
                (Boolean) streamsConfig.getMainConsumerConfigs("groupId", "clientId", 0)
                        .get(StreamsConfig.mainConsumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG))
        );
    }

    @Test
    public void shouldThrowConfigExceptionWhenAdminClientMetricsDisabledStreamsMetricsPushEnabled() {
        props.put(StreamsConfig.adminClientPrefix(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG), false);

        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));

        assertThat(
                exception.getMessage(),
                containsString("admin.client metrics push is disabled")
        );
    }

    @Test
    public void shouldThrowConfigExceptionWhenAdminClientAndMainConsumerMetricsDisabledStreamsMetricsPushEnabled() {
        props.put(StreamsConfig.mainConsumerPrefix(ConsumerConfig.ENABLE_METRICS_PUSH_CONFIG), false);
        props.put(StreamsConfig.adminClientPrefix(AdminClientConfig.ENABLE_METRICS_PUSH_CONFIG), false);

        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));

        assertThat(
                exception.getMessage(),
                containsString("main.consumer and admin.client metrics push are disabled")
        );
    }

    @Test
    public void shouldGetDefaultValueProcessingExceptionHandler() {
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        assertEquals("org.apache.kafka.streams.errors.LogAndFailProcessingExceptionHandler",   streamsConfig.processingExceptionHandler().getClass().getName());
    }

    @Test
    public void shouldOverrideDefaultProcessingExceptionHandler() {
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, "org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler");
        final StreamsConfig streamsConfig = new StreamsConfig(props);

        assertEquals("org.apache.kafka.streams.errors.LogAndContinueProcessingExceptionHandler",   streamsConfig.processingExceptionHandler().getClass().getName());
    }

    @Test
    public void testInvalidProcessingExceptionHandler() {
        props.put(StreamsConfig.PROCESSING_EXCEPTION_HANDLER_CLASS_CONFIG, "org.apache.kafka.streams.errors.InvalidProcessingExceptionHandler");
        final Exception exception = assertThrows(ConfigException.class, () -> new StreamsConfig(props));

        assertThat(
                exception.getMessage(),
                containsString("Invalid value org.apache.kafka.streams.errors.InvalidProcessingExceptionHandler " +
                        "for configuration processing.exception.handler: Class org.apache.kafka.streams.errors.InvalidProcessingExceptionHandler could not be found.")
        );
    }

    @Test
    public void shouldSetAndGetDeserializationExceptionHandlerWhenOnlyNewConfigIsSet() {
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsConfig = new StreamsConfig(props);
        assertEquals(LogAndContinueExceptionHandler.class, streamsConfig.deserializationExceptionHandler().getClass());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseNewDeserializationExceptionHandlerWhenBothConfigsAreSet() {
        props.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndFailExceptionHandler.class);

        try (LogCaptureAppender streamsConfigLogs = LogCaptureAppender.createAndRegister(StreamsConfig.class)) {
            streamsConfigLogs.setClassLogger(StreamsConfig.class, Level.WARN);
            streamsConfig = new StreamsConfig(props);
            assertEquals(LogAndContinueExceptionHandler.class, streamsConfig.deserializationExceptionHandler().getClass());

            final long warningMessageWhenBothConfigsAreSet = streamsConfigLogs.getMessages().stream()
                .filter(m -> m.contains("Both the deprecated and new config for deserialization exception handler are configured."))
                .count();
            assertEquals(1, warningMessageWhenBothConfigsAreSet);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseOldDeserializationExceptionHandlerWhenOnlyOldConfigIsSet() {
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG, LogAndContinueExceptionHandler.class);
        streamsConfig = new StreamsConfig(props);
        assertEquals(LogAndContinueExceptionHandler.class, streamsConfig.deserializationExceptionHandler().getClass());
    }

    @Test
    public void shouldSetAndGetProductionExceptionHandlerWhenOnlyNewConfigIsSet() {
        props.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, RecordCollectorTest.ProductionExceptionHandlerMock.class);
        streamsConfig = new StreamsConfig(props);
        assertEquals(RecordCollectorTest.ProductionExceptionHandlerMock.class, streamsConfig.productionExceptionHandler().getClass());
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseNewProductionExceptionHandlerWhenBothConfigsAreSet() {
        props.put(StreamsConfig.PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, RecordCollectorTest.ProductionExceptionHandlerMock.class);
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, DefaultProductionExceptionHandler.class);

        try (LogCaptureAppender streamsConfigLogs = LogCaptureAppender.createAndRegister(StreamsConfig.class)) {
            streamsConfigLogs.setClassLogger(StreamsConfig.class, Level.WARN);
            streamsConfig = new StreamsConfig(props);
            assertEquals(RecordCollectorTest.ProductionExceptionHandlerMock.class, streamsConfig.productionExceptionHandler().getClass());

            final long warningMessageWhenBothConfigsAreSet = streamsConfigLogs.getMessages().stream()
                .filter(m -> m.contains("Both the deprecated and new config for production exception handler are configured."))
                .count();
            assertEquals(1, warningMessageWhenBothConfigsAreSet);
        }
    }

    @SuppressWarnings("deprecation")
    @Test
    public void shouldUseOldProductionExceptionHandlerWhenOnlyOldConfigIsSet() {
        props.put(StreamsConfig.DEFAULT_PRODUCTION_EXCEPTION_HANDLER_CLASS_CONFIG, RecordCollectorTest.ProductionExceptionHandlerMock.class);
        streamsConfig = new StreamsConfig(props);
        assertEquals(RecordCollectorTest.ProductionExceptionHandlerMock.class, streamsConfig.productionExceptionHandler().getClass());
    }

    static class MisconfiguredSerde implements Serde<Object> {
        @Override
        public void configure(final Map<String, ?>  configs, final boolean isKey) {
            throw new RuntimeException("boom");
        }

        @Override
        public Serializer<Object> serializer() {
            return null;
        }

        @Override
        public Deserializer<Object> deserializer() {
            return null;
        }
    }

    public static class MockTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
            return 0;
        }
    }
}
