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
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.ProcessorSupplier;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static java.lang.Long.MAX_VALUE;
import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.getStartedStreams;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxRecords;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

@Tag("integration")
@Timeout(600)
public class SuppressionDurabilityIntegrationTest {
    private static final long NOW = Instant.now().toEpochMilli();
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    public TestInfo testInfo;

    private static final StringDeserializer STRING_DESERIALIZER = new StringDeserializer();
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final Serde<String> STRING_SERDE = Serdes.String();
    private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();
    private static final long COMMIT_INTERVAL = 100L;

    @ParameterizedTest
    @ValueSource(strings = {StreamsConfig.AT_LEAST_ONCE, StreamsConfig.EXACTLY_ONCE_V2})
    public void shouldRecoverBufferAfterShutdown(final String processingGuarantee, final TestInfo testInfo) {
        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId_" + testId;
        final String input = "input" + testId;
        final String storeName = "counts";
        final String outputSuppressed = "output-suppressed" + testId;
        final String outputRaw = "output-raw" + testId;

        // create multiple partitions as a trap, in case the buffer doesn't properly set the
        // partition on the records, but instead relies on the default key partitioner
        cleanStateBeforeTest(CLUSTER, 2, input, outputRaw, outputSuppressed);

        final StreamsBuilder builder = new StreamsBuilder();
        final KTable<String, Long> valueCounts = builder
            .stream(
                input,
                Consumed.with(STRING_SERDE, STRING_SERDE))
            .groupByKey()
            .count(Materialized.<String, Long, KeyValueStore<Bytes, byte[]>>as(storeName).withCachingDisabled());

        final KStream<String, Long> suppressedCounts = valueCounts
            .suppress(untilTimeLimit(ofMillis(MAX_VALUE), maxRecords(3L).emitEarlyWhenFull()))
            .toStream();

        final AtomicInteger eventCount = new AtomicInteger(0);
        suppressedCounts.foreach((key, value) -> eventCount.incrementAndGet());

        // expect all post-suppress records to keep the right input topic
        final MetadataValidator metadataValidator = new MetadataValidator(input);

        suppressedCounts
            .process(metadataValidator)
            .to(outputSuppressed, Produced.with(STRING_SERDE, Serdes.Long()));

        valueCounts
            .toStream()
            .process(metadataValidator)
            .to(outputRaw, Produced.with(STRING_SERDE, Serdes.Long()));

        final Properties streamsConfig = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, Long.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, processingGuarantee),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));

        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, COMMIT_INTERVAL);

        KafkaStreams driver = getStartedStreams(streamsConfig, builder, true);
        try {
            // start by putting some stuff in the buffer
            // note, we send all input records to partition 0
            // to make sure that supppress doesn't erroneously send records to other partitions.
            produceSynchronouslyToPartitionZero(
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
                    new KeyValueTimestamp<>("k1", 1L, scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", 1L, scaledTime(2L)),
                    new KeyValueTimestamp<>("k3", 1L, scaledTime(3L))
                )
            );
            assertThat(eventCount.get(), is(0));

            // flush two of the first three events out.
            produceSynchronouslyToPartitionZero(
                input,
                asList(
                    new KeyValueTimestamp<>("k4", "v4", scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", "v5", scaledTime(5L))
                )
            );
            verifyOutput(
                outputRaw,
                asList(
                    new KeyValueTimestamp<>("k4", 1L, scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", 1L, scaledTime(5L))
                )
            );
            assertThat(eventCount.get(), is(2));
            verifyOutput(
                outputSuppressed,
                asList(
                    new KeyValueTimestamp<>("k1", 1L, scaledTime(1L)),
                    new KeyValueTimestamp<>("k2", 1L, scaledTime(2L))
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
                    new KeyValueTimestamp<>("k6", 1L, scaledTime(6L)),
                    new KeyValueTimestamp<>("k7", 1L, scaledTime(7L)),
                    new KeyValueTimestamp<>("k8", 1L, scaledTime(8L))
                )
            );
            assertThat("suppress has apparently produced some duplicates. There should only be 5 output events.",
                       eventCount.get(), is(5));

            verifyOutput(
                outputSuppressed,
                asList(
                    new KeyValueTimestamp<>("k3", 1L, scaledTime(3L)),
                    new KeyValueTimestamp<>("k4", 1L, scaledTime(4L)),
                    new KeyValueTimestamp<>("k5", 1L, scaledTime(5L))
                )
            );

            metadataValidator.raiseExceptionIfAny();

        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    private static final class MetadataValidator implements ProcessorSupplier<String, Long, String, Long> {
        private static final Logger LOG = LoggerFactory.getLogger(MetadataValidator.class);
        private final AtomicReference<Throwable> firstException = new AtomicReference<>();
        private final String topic;

        public MetadataValidator(final String topic) {
            this.topic = topic;
        }

        @Override
        public Processor<String, Long, String, Long> get() {
            return new Processor<String, Long, String, Long>() {
                private ProcessorContext<String, Long> context;

                @Override
                public void init(final ProcessorContext<String, Long> context) {
                    this.context = context;
                }

                @Override
                public void process(final Record<String, Long> record) {
                    try {
                        assertThat(context.recordMetadata().get().topic(), equalTo(topic));
                    } catch (final Throwable e) {
                        firstException.compareAndSet(null, e);
                        LOG.error("Validation Failed", e);
                    }
                    context.forward(record);
                }
            };
        }

        void raiseExceptionIfAny() {
            final Throwable exception = firstException.get();
            if (exception != null) {
                throw new AssertionError("Got an exception during run", exception);
            }
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
        return NOW + COMMIT_INTERVAL * 2 * unscaledTime;
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