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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.IntegrationTest;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.lang.Long.MAX_VALUE;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.StreamsConfig.EXACTLY_ONCE;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateAfterTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getStartedStreams;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class SuppressionDurabilityIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        3,
        mkProperties(mkMap()),
        0L
    );
    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private static final int COMMIT_INTERVAL = 100;
    private final boolean eosEnabled;

    public SuppressionDurabilityIntegrationTest(final boolean eosEnabled) {
        this.eosEnabled = eosEnabled;
    }

    @Parameters(name = "{index}: eosEnabled={0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[] {false}, new Object[] {true});
    }

    private KTable<String, Long> buildCountsTable(final String input, final StreamsBuilder builder) {
        return builder
            .table(
                input,
                Consumed.with(STRING_SERDE, STRING_SERDE),
                Materialized.<String, String, KeyValueStore<Bytes, byte[]>>with(STRING_SERDE, STRING_SERDE)
                    .withCachingDisabled()
                    .withLoggingDisabled()
            )
            .groupBy((k, v) -> new KeyValue<>(v, k), Grouped.with(STRING_SERDE, STRING_SERDE))
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as("counts").withCachingDisabled());
    }

    @Test
    public void shouldRecoverBufferAfterShutdown() {
        final String testId = "-shouldRecoverBufferAfterShutdown";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final String input = "input" + testId;
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        cleanStateBeforeTest(CLUSTER, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> valueCounts = buildCountsTable(input, builder);

        final KStream<String, Long> suppressedCounts = valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(3L).emitEarlyWhenFull()))
            .toStream();

        final AtomicInteger eventCount = new AtomicInteger(0);
        suppressedCounts.foreach((key, value) -> eventCount.incrementAndGet());

        suppressedCounts
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        final Properties streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, Integer.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosEnabled ? EXACTLY_ONCE : AT_LEAST_ONCE)
        ));

        KafkaStreams driver = getStartedStreams(streamsConfig, builder, true);
        try {
            // start by putting some stuff in the buffer
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v2", scaledTime(2L)),
                    new KeyValueTimestamp<>("k3", "v3", scaledTime(3L))
                )
            );
            verifyOutput(
                outputRaw,
                asList(
                    new KeyValueTimestamp<>("v1", 1L, scaledTime(1L)),
                    new KeyValueTimestamp<>("v2", 1L, scaledTime(2L)),
                    new KeyValueTimestamp<>("v3", 1L, scaledTime(3L))
                )
            );
            assertThat(eventCount.get(), is(0));

            // flush two of the first three events out.
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k4", "v4", scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", "v5", scaledTime(5L))
                )
            );
            verifyOutput(
                outputRaw,
                asList(
                    new KeyValueTimestamp<>("v4", 1L, scaledTime(4L)),
                    new KeyValueTimestamp<>("v5", 1L, scaledTime(5L))
                )
            );
            assertThat(eventCount.get(), is(2));
            verifyOutput(
                outputSuppressed,
                asList(
                    new KeyValueTimestamp<>("v1", 1L, scaledTime(1L)),
                    new KeyValueTimestamp<>("v2", 1L, scaledTime(2L))
                )
            );

            // bounce to ensure that the history, including retractions,
            // get restored properly. (i.e., we shouldn't see those first events again)

            // restart the driver
            driver.close();
            assertThat(driver.state(), is(KafkaStreams.State.NOT_RUNNING));
            driver = getStartedStreams(streamsConfig, builder, false);


            // flush those recovered buffered events out.
            produceSynchronously(
                input,
                asList(
                    new KeyValueTimestamp<>("k6", "v6", scaledTime(6L)),
                    new KeyValueTimestamp<>("k7", "v7", scaledTime(7L)),
                    new KeyValueTimestamp<>("k8", "v8", scaledTime(8L))
                )
            );
            verifyOutput(
                outputRaw,
                asList(
                    new KeyValueTimestamp<>("v6", 1L, scaledTime(6L)),
                    new KeyValueTimestamp<>("v7", 1L, scaledTime(7L)),
                    new KeyValueTimestamp<>("v8", 1L, scaledTime(8L))
                )
            );
            assertThat(eventCount.get(), is(5));
            verifyOutput(
                outputSuppressed,
                asList(
                    new KeyValueTimestamp<>("v3", 1L, scaledTime(3L)),
                    new KeyValueTimestamp<>("v4", 1L, scaledTime(4L)),
                    new KeyValueTimestamp<>("v5", 1L, scaledTime(5L))
                )
            );

        } finally {
            driver.close();
            cleanStateAfterTest(CLUSTER, driver);
        }
    }

    private void verifyOutput(final String topic, final List<KeyValueTimestamp<String, Long>> keyValueTimestamps) {
        final Properties properties = mkProperties(
            mkMap(
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<String>) STRING_DESERIALIZER).getClass().getName()),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<Long>) LONG_DESERIALIZER).getClass().getName())
            )
        );
        IntegrationTestUtils.verifyKeyValueTimestamps(properties, topic, keyValueTimestamps);

    }

    /**
     * scaling to ensure that there are commits in between the various test events,
     * just to exercise that everything works properly in the presence of commits.
     */
    private long scaledTime(final long unscaledTime) {
        return COMMIT_INTERVAL * 2 * unscaledTime;
    }

    private void produceSynchronously(final String topic, final List<KeyValueTimestamp<String, String>> toProduce) {
        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, toProduce);
    }
}