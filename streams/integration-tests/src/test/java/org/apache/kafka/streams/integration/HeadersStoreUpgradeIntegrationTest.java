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
package org.apache.kafka.streams.integration;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStore;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;

@Tag("integration")
public class HeadersStoreUpgradeIntegrationTest {
    private static final String STORE_NAME = "store";
    private String inputStream;

    private KafkaStreams kafkaStreams;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    public String safeTestName;

    @BeforeEach
    public void createTopics(final TestInfo testInfo) throws Exception {
        safeTestName = safeUniqueTestName(testInfo);
        inputStream = "input-stream-" + safeTestName;
        CLUSTER.createTopic(inputStream);
    }

    private Properties props() {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @AfterEach
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30L));
            kafkaStreams.cleanUp();
        }
    }

    @Test
    public void shouldMigrateInMemoryTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        shouldMigrateTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(false);
    }

    @Test
    public void shouldMigratePersistentTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        shouldMigrateTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(true);
    }

    private void shouldMigrateTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi(final boolean persistentStore) throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                persistentStore ? Stores.persistentTimestampedKeyValueStore(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedKeyValueProcessor::new, STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
        kafkaStreams.start();

        processKeyValueAndVerifyTimestampedValue("key1", "value1", 11L);
        processKeyValueAndVerifyTimestampedValue("key2", "value2", 22L);
        processKeyValueAndVerifyTimestampedValue("key3", "value3", 33L);

        kafkaStreams.close();
        kafkaStreams = null;

        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

        streamsBuilderForNewStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                persistentStore ? Stores.persistentTimestampedKeyValueStoreWithHeaders(STORE_NAME) : Stores.inMemoryKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME);

        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
        kafkaStreams.start();

        // Verify legacy data can be read with empty headers
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", 11L);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", 22L);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", 33L);

        // Process new records with headers
        final Headers headers = new RecordHeaders();
        headers.add("source", "test".getBytes());

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, headers, headers);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, headers, headers);

        kafkaStreams.close();
    }

    @Test
    public void shouldProxyTimestampedKeyValueStoreToTimestampedKeyValueStoreWithHeadersUsingPapi() throws Exception {
        final StreamsBuilder streamsBuilderForOldStore = new StreamsBuilder();

        streamsBuilderForOldStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilder(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedKeyValueProcessor::new, STORE_NAME);

        final Properties props = props();
        kafkaStreams = new KafkaStreams(streamsBuilderForOldStore.build(), props);
        kafkaStreams.start();

        processKeyValueAndVerifyTimestampedValue("key1", "value1", 11L);
        processKeyValueAndVerifyTimestampedValue("key2", "value2", 22L);
        processKeyValueAndVerifyTimestampedValue("key3", "value3", 33L);

        kafkaStreams.close();
        kafkaStreams = null;



        final StreamsBuilder streamsBuilderForNewStore = new StreamsBuilder();

        streamsBuilderForNewStore.addStateStore(
            Stores.timestampedKeyValueStoreBuilderWithHeaders(
                Stores.persistentTimestampedKeyValueStore(STORE_NAME),
                Serdes.String(),
                Serdes.String()))
            .stream(inputStream, Consumed.with(Serdes.String(), Serdes.String()))
            .process(TimestampedKeyValueWithHeadersProcessor::new, STORE_NAME);

        kafkaStreams = new KafkaStreams(streamsBuilderForNewStore.build(), props);
        kafkaStreams.start();

        // Verify legacy data can be read with empty headers
        verifyLegacyValuesWithEmptyHeaders("key1", "value1", 11L);
        verifyLegacyValuesWithEmptyHeaders("key2", "value2", 22L);
        verifyLegacyValuesWithEmptyHeaders("key3", "value3", 33L);

        // Process new records with headers
        final RecordHeaders headers = new RecordHeaders();
        headers.add("source", "proxy-test".getBytes());
        final Headers expectedHeaders = new RecordHeaders();

        processKeyValueWithTimestampAndHeadersAndVerify("key3", "value3", 333L, headers, expectedHeaders);
        processKeyValueWithTimestampAndHeadersAndVerify("key4new", "value4", 444L, headers, expectedHeaders);

        kafkaStreams.close();
    }

    private <K, V> void processKeyValueAndVerifyTimestampedValue(final K key,
                                                                 final V value,
                                                                 final long timestamp)
        throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class),
            timestamp,
            false);

        TestUtils.waitForCondition(
            () -> {
                try {
                    final ReadOnlyKeyValueStore<K, ValueAndTimestamp<V>> store =
                        IntegrationTestUtils.getStore(STORE_NAME, kafkaStreams, QueryableStoreTypes.timestampedKeyValueStore());

                    if (store == null) {
                        return false;
                    }

                    final ValueAndTimestamp<V> result = store.get(key);
                    return result != null && result.value().equals(value) && result.timestamp() == timestamp;
                } catch (final Exception swallow) {
                    swallow.printStackTrace();
                    System.err.println(swallow.getMessage());
                    return false;
                }
            },
            60_000L,
            "Could not get expected result in time.");
    }

    private <K, V> void processKeyValueWithTimestampAndHeadersAndVerify(final K key,
                                                                        final V value,
                                                                        final long timestamp,
                                                                        final Headers headers,
                                                                        final Headers expectedHeaders)
        throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputStream,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class),
            headers,
            timestamp,
            false);

        TestUtils.waitForCondition(
            () -> {
                try {
                    final ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> store = IntegrationTestUtils
                        .getStore(STORE_NAME, kafkaStreams, QueryableStoreTypes.keyValueStore());

                    if (store == null)
                        return false;

                    final ValueTimestampHeaders<V> result = store.get(key);
                    return result != null
                        && result.value().equals(value)
                        && result.timestamp() == timestamp
                        && result.headers().equals(expectedHeaders);
                } catch (final Exception swallow) {
                    swallow.printStackTrace();
                    System.err.println(swallow.getMessage());
                    return false;
                }
            },
            60_000L,
            "Could not get expected result in time.");
    }

    private <K, V> void verifyLegacyValuesWithEmptyHeaders(final K key,
                                                           final V value,
                                                           final long timestamp) throws Exception {
        TestUtils.waitForCondition(
            () -> {
                try {
                    final ReadOnlyKeyValueStore<K, ValueTimestampHeaders<V>> store = IntegrationTestUtils
                        .getStore(STORE_NAME, kafkaStreams, QueryableStoreTypes.keyValueStore());

                    if (store == null)
                        return false;

                    final ValueTimestampHeaders<V> result = store.get(key);
                    return result != null
                        && result.value().equals(value)
                        && result.timestamp() == timestamp
                        && result.headers().toArray().length == 0;
                } catch (final Exception swallow) {
                    swallow.printStackTrace();
                    System.err.println(swallow.getMessage());
                    return false;
                }
            },
            60_000L,
            "Could not get expected result in time.");
    }

    private static class TimestampedKeyValueProcessor implements Processor<String, String, Void, Void> {
        private TimestampedKeyValueStore<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            store.put(record.key(), ValueAndTimestamp.make(record.value(), record.timestamp()));
        }
    }

    private static class TimestampedKeyValueWithHeadersProcessor implements Processor<String, String, Void, Void> {
        private TimestampedKeyValueStoreWithHeaders<String, String> store;

        @Override
        public void init(final ProcessorContext<Void, Void> context) {
            store = context.getStateStore(STORE_NAME);
        }

        @Override
        public void process(final Record<String, String> record) {
            store.put(record.key(), ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers()));
        }
    }
}