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

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsConfig.InternalConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.EmitStrategy.StrategyType;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindowedDeserializer;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.apache.kafka.streams.kstream.internals.TimeWindow;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Properties;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

@Tag("integration")
@Timeout(600)
public class TimeWindowedCogroupedStreamIntegrationTest {
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS,
        mkProperties(mkMap(
            mkEntry("log.retention.hours", "-1"),
            mkEntry("log.retention.bytes", "-1")
        ))
    );

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private StreamsBuilder builder;
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String streamOneInput;
    private String streamTwoInput;
    private String outputTopic;
    private String safeTestName;

    @BeforeEach
    public void before(final TestInfo testInfo) throws InterruptedException {
        builder = new StreamsBuilder();
        safeTestName = safeUniqueTestName(testInfo);
        createTopics();
        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(InternalConfig.EMIT_INTERVAL_MS_KSTREAMS_WINDOWED_AGGREGATION, 0);
        streamsConfiguration.put(StreamsConfig.WINDOW_STORE_CHANGE_LOG_ADDITIONAL_RETENTION_MS_CONFIG, Long.MAX_VALUE);
    }

    @AfterEach
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(60));
            kafkaStreams.cleanUp();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @ParameterizedTest
    // @CsvSource({"ON_WINDOW_UPDATE, true", "ON_WINDOW_UPDATE, false"}) passing
    @CsvSource({"ON_WINDOW_CLOSE, false"})
    //@CsvSource({"ON_WINDOW_UPDATE, true", "ON_WINDOW_UPDATE, false", "ON_WINDOW_CLOSE, true", "ON_WINDOW_CLOSE, false"})
    public void shouldCogroupWindowedWithNoGrace(final StrategyType type, final boolean withCache) throws Exception {
        produceMessages(
            streamOneInput,
            new KeyValueTimestamp<>("A", "1", 0),
            new KeyValueTimestamp<>("A", "2", 5),
            new KeyValueTimestamp<>("B", "3", 10)
        );

        produceMessages(
            streamTwoInput,
            new KeyValueTimestamp<>("A", "a", 2),
            new KeyValueTimestamp<>("B", "b", 4),
            new KeyValueTimestamp<>("A", "c", 12)
        );

        final Serde<Windowed<String>> windowedSerde = WindowedSerdes.timeWindowedSerdeFrom(String.class, 10L);
        final KStream<String, String> streamOne = builder.stream(streamOneInput, Consumed.with(Serdes.String(), Serdes.String()));
        final KStream<String, String> streamTwo = builder.stream(streamTwoInput, Consumed.with(Serdes.String(), Serdes.String()));

        streamOne.groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
            .cogroup((key, value, aggregate) -> aggregate + "+s1:" + value)
            .cogroup(
                streamTwo.groupByKey(Grouped.with(Serdes.String(), Serdes.String())),
                (key, value, aggregate) -> aggregate + "+s2:" + value
            )
            .windowedBy(TimeWindows.ofSizeWithNoGrace(ofMillis(10L)))
            .emitStrategy(StrategyType.forType(type))
            .aggregate(
                () -> "0",  // Use lambda instead of MockInitializer
                getMaterialized(withCache)
            )
            .toStream()
            .to(outputTopic, Produced.with(windowedSerde, Serdes.String()));

        startStreams();

        final boolean emitFinal = type == StrategyType.ON_WINDOW_CLOSE;
        final List<KeyValueTimestamp<Windowed<String>, String>> windowedMessages = receiveMessagesWithTimestamp(
            new TimeWindowedDeserializer<>(new StringDeserializer(), 10L),
            new StringDeserializer(),
            10L,
            String.class,
            emitFinal ? 2 : 6
        );

        final List<KeyValueTimestamp<Windowed<String>, String>> expectedResult;
        if (emitFinal) {
            expectedResult = asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+s1:1+s1:2+s2:a", 5),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)), "0+s1:3", 10)
            );
        } else {
            expectedResult = asList(
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+s1:1", 0),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+s1:1+s2:a", 2),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(0L, 10L)), "0+s2:b", 4),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(0L, 10L)), "0+s1:1+s2:a+s1:2", 5),
                new KeyValueTimestamp<>(new Windowed<>("B", new TimeWindow(10L, 20L)), "0+s1:3", 10),
                new KeyValueTimestamp<>(new Windowed<>("A", new TimeWindow(10L, 20L)), "0+s2:c", 12)
            );
        }

        assertThat(windowedMessages, is(expectedResult));
    }

    private void produceMessages(final String topic, final KeyValueTimestamp<String, String>... records) {
        IntegrationTestUtils.produceSynchronously(
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                StringSerializer.class,
                StringSerializer.class
            ),
            false,
            topic,
            Optional.empty(),
            Arrays.asList(records)
        );
    }

    private Materialized getMaterialized(final boolean withCache) {
        if (withCache) {
            return Materialized.with(null, Serdes.String()).withCachingEnabled();
        }
        return Materialized.with(null, Serdes.String()).withCachingDisabled();
    }

    private void createTopics() throws InterruptedException {
        streamOneInput = "stream-one-" + safeTestName;
        streamTwoInput = "stream-two-" + safeTestName;
        outputTopic = "output-" + safeTestName;
        CLUSTER.createTopic(streamOneInput, 1, 1);
        CLUSTER.createTopic(streamTwoInput, 1, 1);
        CLUSTER.createTopic(outputTopic);
    }

    private void startStreams() {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private <K, V> List<KeyValueTimestamp<K, V>> receiveMessagesWithTimestamp(
        final Deserializer<K> keyDeserializer,
        final Deserializer<V> valueDeserializer,
        final long windowSize,
        final Class<?> innerClass,
        final int numMessages) throws Exception {
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, keyDeserializer.getClass().getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, valueDeserializer.getClass().getName());
        consumerProperties.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, windowSize);
        if (keyDeserializer instanceof TimeWindowedDeserializer) {
            consumerProperties.setProperty(StreamsConfig.WINDOWED_INNER_CLASS_SERDE,
                Serdes.serdeFrom(innerClass).getClass().getName());
        }
        return IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(
            consumerProperties,
            outputTopic,
            numMessages,
            60 * 1000
        );
    }
}