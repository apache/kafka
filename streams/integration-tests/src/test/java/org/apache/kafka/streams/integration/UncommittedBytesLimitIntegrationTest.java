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
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.IsolationLevel;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Processing pauses while a thread's uncommitted state store bytes exceed
 * {@code statestore.uncommitted.max.bytes}, and resumes once a commit drains them. These tests check that the
 * pause cannot become permanent: every byte it counts must be one that a commit releases.
 */
@Timeout(600)
@Tag("integration")
public class UncommittedBytesLimitIntegrationTest {

    private static final int NUM_BROKERS = 1;
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static final String STORE_NAME = "init-store";
    private static final int INIT_WRITE_BYTES = 4096;

    private String applicationId;
    private String inputTopic;
    private String outputTopic;
    private Properties streamsConfig;
    private KafkaStreams streams;

    @BeforeAll
    public static void startCluster() throws Exception {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void before(final TestInfo testInfo) throws Exception {
        applicationId = "uncommitted-bytes-limit-" + safeUniqueTestName(testInfo);
        inputTopic = applicationId + "-input";
        outputTopic = applicationId + "-output";
        CLUSTER.createTopic(inputTopic, 1, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);

        streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfig.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfig.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        streamsConfig.put(StreamsConfig.TRANSACTIONAL_STATE_STORES_CONFIG, true);
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    }

    @AfterEach
    public void after() throws Exception {
        if (streams != null) {
            streams.close(Duration.ofSeconds(30));
            streams.cleanUp();
        }
        CLUSTER.deleteAllTopics();
    }

    @Test
    public void shouldProcessRecordsWhenProcessorInitWritesExceedUncommittedBytesLimit() throws Exception {
        streamsConfig.put(StreamsConfig.STATESTORE_UNCOMMITTED_MAX_BYTES_CONFIG, INIT_WRITE_BYTES / 4L);
        startApplication(true);

        produceRecord("k", "v");

        final List<KeyValue<String, String>> results =
            waitUntilMinKeyValueRecordsReceived(getConsumerConfig(), outputTopic, 1, 60000);
        assertEquals(List.of(KeyValue.pair("k", "v")), results);
    }

    @Test
    public void shouldProcessRecordsWhenUncommittedBytesLimitIsBelowEmptyStoreOverhead() throws Exception {
        streamsConfig.put(StreamsConfig.STATESTORE_UNCOMMITTED_MAX_BYTES_CONFIG, 1L);
        startApplication(false);

        produceRecord("k1", "v1");
        produceRecord("k2", "v2");
        produceRecord("k3", "v3");

        final List<KeyValue<String, String>> results =
            waitUntilMinKeyValueRecordsReceived(getConsumerConfig(), outputTopic, 3, 60000);
        assertEquals(List.of(KeyValue.pair("k1", "v1"), KeyValue.pair("k2", "v2"), KeyValue.pair("k3", "v3")), results);
    }

    private void startApplication(final boolean writeOnInit) throws Exception {
        final Topology topology = new Topology();
        topology.addSource("source", new StringDeserializer(), new StringDeserializer(), inputTopic);
        topology.addProcessor("writer", () -> new StoreWritingProcessor(writeOnInit), "source");
        topology.addStateStore(
            Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore(STORE_NAME), Serdes.String(), Serdes.ByteArray())
                .withCachingDisabled(),
            "writer");
        topology.addSink("sink", outputTopic, new StringSerializer(), new StringSerializer(), "writer");

        streams = new KafkaStreams(topology, streamsConfig);
        startApplicationAndWaitUntilRunning(streams);
    }

    /**
     * Writes every record to the store before forwarding it, and optionally writes a large value in {@code init()}.
     */
    private static class StoreWritingProcessor implements Processor<String, String, String, String> {
        private final boolean writeOnInit;
        private ProcessorContext<String, String> context;
        private KeyValueStore<String, byte[]> store;

        StoreWritingProcessor(final boolean writeOnInit) {
            this.writeOnInit = writeOnInit;
        }

        @Override
        public void init(final ProcessorContext<String, String> context) {
            this.context = context;
            store = context.getStateStore(STORE_NAME);
            if (writeOnInit) {
                store.put("init", new byte[INIT_WRITE_BYTES]);
            }
        }

        @Override
        public void process(final Record<String, String> record) {
            store.put(record.key(), record.value().getBytes(StandardCharsets.UTF_8));
            context.forward(record);
        }
    }

    private void produceRecord(final String key, final String value) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");

        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic,
            List.of(new KeyValue<>(key, value)),
            producerConfig,
            CLUSTER.time
        );
    }

    private Properties getConsumerConfig() {
        final Properties consumerConfig = new Properties();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-" + applicationId);
        consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerConfig.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString().toLowerCase(Locale.ROOT));
        return consumerConfig;
    }
}
