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
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.integration.utils.KafkaProtocolFaultProxy;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * A basic smoke test that wires a real {@code source -> count -> sink} Kafka Streams app through the
 * {@link KafkaProtocolFaultProxy} (no {@code FaultInjectingClientSupplier}). It processes 10 records
 * cleanly through the proxy, then arms a fatal broker error on the commit boundary ({@code EndTxn}) so
 * the next commit fails and the stream thread throws — captured via the uncaught-exception handler.
 */
@Tag("integration")
@Timeout(120)
public class ProxyFaultBasicIntegrationTest {

    private static final int NUM_KEYS = 10;

    private EmbeddedKafkaCluster cluster;
    private KafkaProtocolFaultProxy proxy;
    private KafkaStreams streams;
    private String inputTopic;
    private String outputTopic;
    private String appId;
    private final AtomicReference<Throwable> uncaught = new AtomicReference<>();

    @BeforeEach
    public void setUp(final TestInfo info) throws Exception {
        cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        final String base = safeUniqueTestName(info);
        appId = "proxy-fault-basic-" + base;
        inputTopic = appId + "-in";
        outputTopic = appId + "-out";
        cluster.createTopic(inputTopic, 1, 1);
        cluster.createTopic(outputTopic, 1, 1);
        proxy = KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers());
    }

    @AfterEach
    public void tearDown() {
        if (streams != null) {
            streams.close();
            streams.cleanUp();
        }
        if (proxy != null) {
            proxy.close();
        }
        if (cluster != null) {
            cluster.stop();
        }
    }

    @Test
    public void shouldThrowWhenCommitFaultInjectedAfterTenRecords() throws Exception {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()))
            .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .count(Materialized.as("counts"))
            .toStream()
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        // Route the whole app through the proxy.
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
        props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
        props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0); // emit every update -> 10 in, 10 out
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        streams = new KafkaStreams(builder.build(), props);
        streams.setUncaughtExceptionHandler(t -> {
            uncaught.compareAndSet(null, t);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
        });
        startApplicationAndWaitUntilRunning(streams);

        // Phase 1: 10 distinct keys -> 10 output records, processed and committed cleanly through the proxy.
        produce(NUM_KEYS);
        waitUntilMinKeyValueRecordsReceived(consumerConfig(), outputTopic, NUM_KEYS, 60_000);

        // Phase 2: arm a fatal commit-boundary fault. The next EndTxn the app issues gets a fatal error,
        // so commitTransaction fails fatally and the stream thread throws.
        proxy.injectError(ApiKeys.END_TXN, Errors.INVALID_TXN_STATE).everyTime();

        // Nudge another processing+commit cycle.
        produce(NUM_KEYS);

        TestUtils.waitForCondition(
            () -> uncaught.get() != null || streams.state() == KafkaStreams.State.ERROR,
            60_000L,
            () -> "expected the stream thread to throw after the injected EndTxn fault; state=" + streams.state());

        assertNotNull(uncaught.get(), "an exception should have propagated to the uncaught-exception handler");
        assertTrue(streams.state() == KafkaStreams.State.ERROR
                || streams.state() == KafkaStreams.State.PENDING_ERROR
                || streams.state() == KafkaStreams.State.NOT_RUNNING,
            "app should have shut down after the fatal fault; state=" + streams.state());
    }

    private void produce(final int numKeys) {
        final Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers()); // producer talks to broker directly
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final List<KeyValue<String, String>> records = new ArrayList<>();
        for (int i = 0; i < numKeys; i++) {
            records.add(new KeyValue<>("k" + i, "v" + i));
        }
        IntegrationTestUtils.produceKeyValuesSynchronously(inputTopic, records, p, Time.SYSTEM);
    }

    private Properties consumerConfig() {
        final Properties c = new Properties();
        c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        c.put(ConsumerConfig.GROUP_ID_CONFIG, "verify-" + appId);
        c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class);
        c.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return c;
    }
}
