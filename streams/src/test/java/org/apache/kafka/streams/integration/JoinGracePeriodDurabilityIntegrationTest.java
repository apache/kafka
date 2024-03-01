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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Joined;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.MockValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getStartedStreams;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(Parameterized.class)
@Category({IntegrationTest.class})
public class JoinGracePeriodDurabilityIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        3,
        mkProperties(mkMap()),
        0L
    );

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public TestName testName = new TestName();

    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private static final long COMMIT_INTERVAL = 100L;

    @SuppressWarnings("deprecation")
    @Parameterized.Parameters(name = "{0}")
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][] {
            {StreamsConfig.AT_LEAST_ONCE},
            {StreamsConfig.EXACTLY_ONCE},
            {StreamsConfig.EXACTLY_ONCE_V2}
        });
    }

    @Parameterized.Parameter
    public String processingGuaranteee;

    @Test
    @SuppressWarnings("deprecation")
    public void shouldRecoverBufferAfterShutdown() {
        final String testId = safeUniqueTestName(testName);
        final String appId = "appId_" + testId;
        final String streamInput = "Streaminput" + testId;
        final String tableInput = "Tableinput" + testId;
        final String storeName = "grace";
        final String output = "output" + testId;

        // create multiple partitions as a trap, in case the buffer doesn't properly set the
        // partition on the records, but instead relies on the default key partitioner
        cleanStateBeforeTest(CLUSTER, 2, streamInput, tableInput, output);

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> stream = builder.stream(streamInput, Consumed.with(STRING_SERDE, STRING_SERDE));
        final KTable<String, String> table = builder.table(tableInput, Consumed.with(STRING_SERDE, STRING_SERDE), Materialized.as(
            Stores.persistentVersionedKeyValueStore(storeName, Duration.ofMillis(1000))));
        final KStream<String, String> joinedStream = stream.join(table,
            MockValueJoiner.TOSTRING_JOINER,
            Joined.with(Serdes.String(), Serdes.String(), Serdes.String(), "Grace", Duration.ofMillis(5))
        );

        final AtomicInteger eventCount = new AtomicInteger(0);
        joinedStream.foreach((key, value) -> eventCount.incrementAndGet());

        joinedStream.to(output);

        final Properties streamsConfig = mkObjectProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, Long.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuaranteee),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
            mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
            mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class)
        ));

        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);

        KafkaStreams driver = getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronouslyToPartitionZero(
                tableInput,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(0L)),
                    new KeyValueTimestamp<>("k2", "v2", scaledTime(0L)),
                    new KeyValueTimestamp<>("k3", "v3", scaledTime(0L)),
                    new KeyValueTimestamp<>("k4", "v4", scaledTime(0L)),
                    new KeyValueTimestamp<>("k5", "v5", scaledTime(0L)),
                    new KeyValueTimestamp<>("k6", "v6", scaledTime(0L))
                )
            );
            produceSynchronouslyToPartitionZero(
                streamInput,
                asList(
                    new KeyValueTimestamp<>("k1", "v1", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v2", scaledTime(2L)),
                    new KeyValueTimestamp<>("k3", "v3", scaledTime(7L))
                )
            );
            verifyOutput(
                output,
                asList(
                    new KeyValueTimestamp<>("k1", "v1+v1", scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", "v2+v2", scaledTime(2L))
                )
            );
            assertThat(eventCount.get(), is(2));

            produceSynchronouslyToPartitionZero(
                streamInput,
                asList(
                    new KeyValueTimestamp<>("k4", "v4", scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", "v5", scaledTime(5L))
                )
            );

            // bounce to ensure that the history, including retractions,
            // get restored properly. (i.e., we shouldn't see those first events again)

            // restart the driver
            driver.close();
            assertThat(driver.state(), is(KafkaStreams.State.NOT_RUNNING));
            driver = getStartedStreams(streamsConfig, builder, false);


            // flush those recovered buffered events out.
            produceSynchronouslyToPartitionZero(
                streamInput,
                asList(
                    new KeyValueTimestamp<>("k6", "v6", scaledTime(20L))
                )
            );
            verifyOutput(
                output,
                asList(
                    new KeyValueTimestamp<>("k4", "v4+v4", scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", "v5+v5", scaledTime(5L)),
                    new KeyValueTimestamp<>("k3", "v3+v3", scaledTime(7L))
                    )
            );
            assertThat("There should only be 5 output events.", eventCount.get(), is(5));

        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    private void verifyOutput(final String topic, final List<KeyValueTimestamp<String, String>> keyValueTimestamps) {
        final Properties properties = mkProperties(
            mkMap(
                mkEntry(ConsumerConfig.GROUP_ID_CONFIG, "test-group"),
                mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ((Deserializer<String>) STRING_DESERIALIZER).getClass().getName()),
                mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ((Deserializer<String>) STRING_DESERIALIZER).getClass().getName())
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

    private static void produceSynchronouslyToPartitionZero(final String topic, final List<KeyValueTimestamp<String, String>> toProduce) {
        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.of(0), toProduce);
    }
}