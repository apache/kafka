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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;
import org.apache.kafka.test.faultproxy.FaultRule;
import org.apache.kafka.test.faultproxy.KafkaProtocolFaultProxy;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Tag("integration")
@Timeout(120)
public class FaultProxyExampleIntegrationTest {

    @Test
    public void shouldStayExactlyOnceWhenCommitIsFencedOnce() throws Exception {
        final int numRecords = 300;
        final EmbeddedKafkaCluster cluster = new EmbeddedKafkaCluster(1);
        cluster.start();
        try {
            final String input = "in";
            final String output = "out";
            cluster.createTopic(input, 2, 1);
            cluster.createTopic(output, 2, 1);

            try (final KafkaProtocolFaultProxy proxy =
                     KafkaProtocolFaultProxy.inFrontOf(cluster.bootstrapServers())) {

                // Deterministic fault: fence the FIRST transactional commit. Streams treats PRODUCER_FENCED
                // as a TaskMigrated (recoverable) -> the task is re-initialized and processing continues.
                final FaultRule fence =
                    proxy.injectError(ApiKeys.END_TXN, Errors.PRODUCER_FENCED).once();

                final StreamsBuilder builder = new StreamsBuilder();
                builder.stream(input, Consumed.with(Serdes.String(), Serdes.String()))
                    .groupByKey()
                    .count(Materialized.<String, Long>as(Stores.persistentKeyValueStore("counts"))
                        .withKeySerde(Serdes.String()).withValueSerde(Serdes.Long()))
                    .toStream()
                    .to(output);

                final Properties props = new Properties();
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fault-proxy-example");
                // NB: point Streams at the PROXY, not the broker.
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, proxy.bootstrapServers());
                props.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
                props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
                props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                try (final KafkaStreams streams = new KafkaStreams(builder.build(), props)) {
                    streams.cleanUp();
                    IntegrationTestUtils.startApplicationAndWaitUntilRunning(
                        singletonList(streams), Duration.ofSeconds(60));

                    // Produce directly to the broker (bypass the proxy for input load).
                    final Properties producerConfig = new Properties();
                    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
                    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                    final List<KeyValue<String, String>> records = new ArrayList<>();
                    for (int i = 0; i < numRecords; i++) {
                        records.add(KeyValue.pair("k" + (i % 3), "v" + i));
                    }
                    IntegrationTestUtils.produceKeyValuesSynchronously(
                        input, records, producerConfig, cluster.time);

                    // Oracle: the summed counts converge to exactly numRecords despite the injected fence.
                    TestUtils.waitForCondition(() -> {
                        final ReadOnlyKeyValueStore<String, Long> store = IntegrationTestUtils.getStore(
                            "counts", streams, QueryableStoreTypes.keyValueStore());
                        long sum = 0L;
                        try (final KeyValueIterator<String, Long> all = store.all()) {
                            while (all.hasNext()) {
                                sum += all.next().value;
                            }
                        }
                        return sum == numRecords;
                    }, 60_000L, "counts did not converge to " + numRecords);

                    // The fault actually fired (guards against a hollow pass).
                    assertTrue(fence.timesTriggered() >= 1, "END_TXN fence never fired");
                    assertEquals(KafkaStreams.State.RUNNING, streams.state());
                }
            }
        } finally {
            cluster.stop();
        }
    }
}