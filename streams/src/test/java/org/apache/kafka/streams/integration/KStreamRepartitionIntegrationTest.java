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

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.KafkaStreams.State.ERROR;
import static org.apache.kafka.streams.KafkaStreams.State.REBALANCING;
import static org.apache.kafka.streams.KafkaStreams.State.RUNNING;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@RunWith(value = Parameterized.class)
@Category({IntegrationTest.class})
public class KStreamRepartitionIntegrationTest {
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }


    private String topicB;
    private String inputTopic;
    private String outputTopic;
    private String applicationId;

    private Properties streamsConfiguration;
    private List<KafkaStreams> kafkaStreamsInstances;

    @Parameter
    public String topologyOptimization;

    @Parameters(name = "Optimization = {0}")
    public static Collection<?> topologyOptimization() {
        return Arrays.asList(new String[][]{
            {StreamsConfig.OPTIMIZE},
            {StreamsConfig.NO_OPTIMIZATION}
        });
    }

    @Rule
    public TestName testName = new TestName();

    @Before
    public void before() throws InterruptedException {
        streamsConfiguration = new Properties();
        kafkaStreamsInstances = new ArrayList<>();

        final String safeTestName = safeUniqueTestName(getClass(), testName);

        topicB = "topic-b-" + safeTestName;
        inputTopic = "input-topic-" + safeTestName;
        outputTopic = "output-topic-" + safeTestName;
        applicationId = "app-" + safeTestName;

        CLUSTER.createTopic(inputTopic, 4, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, topologyOptimization);
    }

    @After
    public void whenShuttingDown() throws IOException {
        kafkaStreamsInstances.stream()
                             .filter(Objects::nonNull)
                             .forEach(KafkaStreams::close);

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    public void shouldThrowAnExceptionWhenNumberOfPartitionsOfRepartitionOperationDoNotMatchSourceTopicWhenJoining() throws InterruptedException {
        final int topicBNumberOfPartitions = 6;
        final String inputTopicRepartitionName = "join-repartition-test";
        final AtomicReference<Throwable> expectedThrowable = new AtomicReference<>();
        final int inputTopicRepartitionedNumOfPartitions = 2;

        CLUSTER.createTopic(topicB, topicBNumberOfPartitions, 1);

        final StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<Integer, String> inputTopicRepartitioned = Repartitioned
            .<Integer, String>as(inputTopicRepartitionName)
            .withNumberOfPartitions(inputTopicRepartitionedNumOfPartitions);

        final KStream<Integer, String> topicBStream = builder
            .stream(topicB, Consumed.with(Serdes.Integer(), Serdes.String()));

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .repartition(inputTopicRepartitioned)
               .join(topicBStream, (value1, value2) -> value2, JoinWindows.of(Duration.ofSeconds(10)))
               .to(outputTopic);

        builder.build(streamsConfiguration);

        startStreams(builder, REBALANCING, ERROR, (t, e) -> expectedThrowable.set(e));

        final String expectedMsg = String.format("Number of partitions [%s] of repartition topic [%s] " +
                                                 "doesn't match number of partitions [%s] of the source topic.",
                                                 inputTopicRepartitionedNumOfPartitions,
                                                 toRepartitionTopicName(inputTopicRepartitionName),
                                                 topicBNumberOfPartitions);
        assertNotNull(expectedThrowable.get());
        assertTrue(expectedThrowable.get().getMessage().contains(expectedMsg));
    }

    @Test
    public void shouldDeductNumberOfPartitionsFromRepartitionOperation() throws Exception {
        final String topicBMapperName = "topic-b-mapper";
        final int topicBNumberOfPartitions = 6;
        final String inputTopicRepartitionName = "join-repartition-test";
        final int inputTopicRepartitionedNumOfPartitions = 3;

        final long timestamp = System.currentTimeMillis();

        CLUSTER.createTopic(topicB, topicBNumberOfPartitions, 1);

        final List<KeyValue<Integer, String>> expectedRecords = Arrays.asList(
            new KeyValue<>(1, "A"),
            new KeyValue<>(2, "B")
        );

        sendEvents(timestamp, expectedRecords);
        sendEvents(topicB, timestamp, expectedRecords);

        final StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<Integer, String> inputTopicRepartitioned = Repartitioned
            .<Integer, String>as(inputTopicRepartitionName)
            .withNumberOfPartitions(inputTopicRepartitionedNumOfPartitions);

        final KStream<Integer, String> topicBStream = builder
            .stream(topicB, Consumed.with(Serdes.Integer(), Serdes.String()))
            .map(KeyValue::new, Named.as(topicBMapperName));

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .repartition(inputTopicRepartitioned)
               .join(topicBStream, (value1, value2) -> value2, JoinWindows.of(Duration.ofSeconds(10)))
               .to(outputTopic);

        builder.build(streamsConfiguration);

        startStreams(builder);

        assertEquals(inputTopicRepartitionedNumOfPartitions,
                     getNumberOfPartitionsForTopic(toRepartitionTopicName(inputTopicRepartitionName)));

        assertEquals(inputTopicRepartitionedNumOfPartitions,
                     getNumberOfPartitionsForTopic(toRepartitionTopicName(topicBMapperName)));

        validateReceivedMessages(
            new IntegerDeserializer(),
            new StringDeserializer(),
            expectedRecords
        );
    }

    @Test
    public void shouldDoProperJoiningWhenNumberOfPartitionsAreValidWhenUsingRepartitionOperation() throws Exception {
        final String topicBRepartitionedName = "topic-b-scale-up";
        final String inputTopicRepartitionedName = "input-topic-scale-up";

        final long timestamp = System.currentTimeMillis();

        CLUSTER.createTopic(topicB, 1, 1);

        final List<KeyValue<Integer, String>> expectedRecords = Arrays.asList(
            new KeyValue<>(1, "A"),
            new KeyValue<>(2, "B")
        );

        sendEvents(timestamp, expectedRecords);
        sendEvents(topicB, timestamp, expectedRecords);

        final StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<Integer, String> inputTopicRepartitioned = Repartitioned
            .<Integer, String>as(inputTopicRepartitionedName)
            .withNumberOfPartitions(4);

        final Repartitioned<Integer, String> topicBRepartitioned = Repartitioned
            .<Integer, String>as(topicBRepartitionedName)
            .withNumberOfPartitions(4);

        final KStream<Integer, String> topicBStream = builder
            .stream(topicB, Consumed.with(Serdes.Integer(), Serdes.String()))
            .repartition(topicBRepartitioned);

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .repartition(inputTopicRepartitioned)
               .join(topicBStream, (value1, value2) -> value2, JoinWindows.of(Duration.ofSeconds(10)))
               .to(outputTopic);

        startStreams(builder);

        assertEquals(4, getNumberOfPartitionsForTopic(toRepartitionTopicName(topicBRepartitionedName)));
        assertEquals(4, getNumberOfPartitionsForTopic(toRepartitionTopicName(inputTopicRepartitionedName)));

        validateReceivedMessages(
            new IntegerDeserializer(),
            new StringDeserializer(),
            expectedRecords
        );
    }

    @Test
    public void shouldUseStreamPartitionerForRepartitionOperation() throws Exception {
        final int partition = 1;
        final String repartitionName = "partitioner-test";
        final long timestamp = System.currentTimeMillis();
        final AtomicInteger partitionerInvocation = new AtomicInteger(0);

        final List<KeyValue<Integer, String>> expectedRecords = Arrays.asList(
            new KeyValue<>(1, "A"),
            new KeyValue<>(2, "B")
        );

        sendEvents(timestamp, expectedRecords);

        final StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<Integer, String> repartitioned = Repartitioned
            .<Integer, String>as(repartitionName)
            .withStreamPartitioner((topic, key, value, numPartitions) -> {
                partitionerInvocation.incrementAndGet();
                return partition;
            });

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .repartition(repartitioned)
               .to(outputTopic);

        startStreams(builder);

        final String topic = toRepartitionTopicName(repartitionName);

        validateReceivedMessages(
            new IntegerDeserializer(),
            new StringDeserializer(),
            expectedRecords
        );

        assertTrue(topicExists(topic));
        assertEquals(expectedRecords.size(), partitionerInvocation.get());
    }

    @Test
    public void shouldPerformSelectKeyWithRepartitionOperation() throws Exception {
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "10"),
                new KeyValue<>(2, "20")
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .selectKey((key, value) -> Integer.valueOf(value))
               .repartition()
               .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new IntegerDeserializer(),
            new StringDeserializer(),
            Arrays.asList(
                new KeyValue<>(10, "10"),
                new KeyValue<>(20, "20")
            )
        );

        final String topology = builder.build().describe().toString();

        assertEquals(1, countOccurrencesInTopology(topology, "Sink: .*-repartition.*"));
    }

    @Test
    public void shouldCreateRepartitionTopicIfKeyChangingOperationWasNotPerformed() throws Exception {
        final String repartitionName = "dummy";
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .repartition(Repartitioned.as(repartitionName))
               .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new IntegerDeserializer(),
            new StringDeserializer(),
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        final String topology = builder.build().describe().toString();

        assertTrue(topicExists(toRepartitionTopicName(repartitionName)));
        assertEquals(1, countOccurrencesInTopology(topology, "Sink: .*dummy-repartition.*"));
    }

    @Test
    public void shouldPerformKeySelectOperationWhenRepartitionOperationIsUsedWithKeySelector() throws Exception {
        final String repartitionedName = "new-key";
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<String, String> repartitioned = Repartitioned.<String, String>as(repartitionedName)
            .withKeySerde(Serdes.String());

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .selectKey((key, value) -> key.toString(), Named.as(repartitionedName))
               .repartition(repartitioned)
               .groupByKey()
               .count()
               .toStream()
               .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new StringDeserializer(),
            new LongDeserializer(),
            Arrays.asList(
                new KeyValue<>("1", 1L),
                new KeyValue<>("2", 1L)
            )
        );

        final String topology = builder.build().describe().toString();
        final String repartitionTopicName = toRepartitionTopicName(repartitionedName);

        assertTrue(topicExists(repartitionTopicName));
        assertEquals(1, countOccurrencesInTopology(topology, "Sink: .*" + repartitionedName + "-repartition.*"));
        assertEquals(1, countOccurrencesInTopology(topology, "<-- " + repartitionedName + "\n"));
    }

    @Test
    public void shouldCreateRepartitionTopicWithSpecifiedNumberOfPartitions() throws Exception {
        final String repartitionName = "new-partitions";
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .repartition(Repartitioned.<Integer, String>as(repartitionName).withNumberOfPartitions(2))
               .groupByKey()
               .count()
               .toStream()
               .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new IntegerDeserializer(),
            new LongDeserializer(),
            Arrays.asList(
                new KeyValue<>(1, 1L),
                new KeyValue<>(2, 1L)
            )
        );

        final String repartitionTopicName = toRepartitionTopicName(repartitionName);

        assertTrue(topicExists(repartitionTopicName));
        assertEquals(2, getNumberOfPartitionsForTopic(repartitionTopicName));
    }

    @Test
    public void shouldInheritRepartitionTopicPartitionNumberFromUpstreamTopicWhenNumberOfPartitionsIsNotSpecified() throws Exception {
        final String repartitionName = "new-topic";
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .repartition(Repartitioned.as(repartitionName))
               .groupByKey()
               .count()
               .toStream()
               .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new IntegerDeserializer(),
            new LongDeserializer(),
            Arrays.asList(
                new KeyValue<>(1, 1L),
                new KeyValue<>(2, 1L)
            )
        );

        final String repartitionTopicName = toRepartitionTopicName(repartitionName);

        assertTrue(topicExists(repartitionTopicName));
        assertEquals(4, getNumberOfPartitionsForTopic(repartitionTopicName));
    }

    @Test
    public void shouldCreateOnlyOneRepartitionTopicWhenRepartitionIsFollowedByGroupByKey() throws Exception {
        final String repartitionName = "new-partitions";
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<String, String> repartitioned = Repartitioned.<String, String>as(repartitionName)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String())
            .withNumberOfPartitions(1);

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .selectKey((key, value) -> key.toString())
               .repartition(repartitioned)
               .groupByKey()
               .count()
               .toStream()
               .to(outputTopic);

        startStreams(builder);

        final String topology = builder.build().describe().toString();

        validateReceivedMessages(
            new StringDeserializer(),
            new LongDeserializer(),
            Arrays.asList(
                new KeyValue<>("1", 1L),
                new KeyValue<>("2", 1L)
            )
        );

        assertTrue(topicExists(toRepartitionTopicName(repartitionName)));
        assertEquals(1, countOccurrencesInTopology(topology, "Sink: .*-repartition"));
    }

    @Test
    public void shouldGenerateRepartitionTopicWhenNameIsNotSpecified() throws Exception {
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .selectKey((key, value) -> key.toString())
               .repartition(Repartitioned.with(Serdes.String(), Serdes.String()))
               .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new StringDeserializer(),
            new StringDeserializer(),
            Arrays.asList(
                new KeyValue<>("1", "A"),
                new KeyValue<>("2", "B")
            )
        );

        final String topology = builder.build().describe().toString();

        assertEquals(1, countOccurrencesInTopology(topology, "Sink: .*-repartition"));
    }

    @Test
    public void shouldGoThroughRebalancingCorrectly() throws Exception {
        final String repartitionName = "rebalancing-test";
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<String, String> repartitioned = Repartitioned.<String, String>as(repartitionName)
            .withKeySerde(Serdes.String())
            .withValueSerde(Serdes.String())
            .withNumberOfPartitions(2);

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
               .selectKey((key, value) -> key.toString())
               .repartition(repartitioned)
               .groupByKey()
               .count()
               .toStream()
               .to(outputTopic);

        startStreams(builder);
        final Properties streamsToCloseConfigs = new Properties();
        streamsToCloseConfigs.putAll(streamsConfiguration);
        streamsToCloseConfigs.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath() + "-2");
        final KafkaStreams kafkaStreamsToClose = startStreams(builder, streamsToCloseConfigs);

        validateReceivedMessages(
            new StringDeserializer(),
            new LongDeserializer(),
            Arrays.asList(
                new KeyValue<>("1", 1L),
                new KeyValue<>("2", 1L)
            )
        );

        kafkaStreamsToClose.close();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "C"),
                new KeyValue<>(2, "D")
            )
        );

        validateReceivedMessages(
            new StringDeserializer(),
            new LongDeserializer(),
            Arrays.asList(
                new KeyValue<>("1", 2L),
                new KeyValue<>("2", 2L)
            )
        );

        final String repartitionTopicName = toRepartitionTopicName(repartitionName);

        assertTrue(topicExists(repartitionTopicName));
        assertEquals(2, getNumberOfPartitionsForTopic(repartitionTopicName));
    }

    private int getNumberOfPartitionsForTopic(final String topic) throws Exception {
        try (final AdminClient adminClient = createAdminClient()) {
            final TopicDescription topicDescription = adminClient.describeTopics(Collections.singleton(topic))
                                                                 .values()
                                                                 .get(topic)
                                                                 .get();

            return topicDescription.partitions().size();
        }
    }

    private boolean topicExists(final String topic) throws Exception {
        try (final AdminClient adminClient = createAdminClient()) {
            final Set<String> topics = adminClient.listTopics()
                                                  .names()
                                                  .get();

            return topics.contains(topic);
        }
    }

    private String toRepartitionTopicName(final String input) {
        return applicationId + "-" + input + "-repartition";
    }

    private static AdminClient createAdminClient() {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        return AdminClient.create(properties);
    }

    private static int countOccurrencesInTopology(final String topologyString,
                                                  final String searchPattern) {
        final Matcher matcher = Pattern.compile(searchPattern).matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();

        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }

        return repartitionTopicsFound.size();
    }

    private void sendEvents(final long timestamp,
                            final List<KeyValue<Integer, String>> events) throws Exception {
        sendEvents(inputTopic, timestamp, events);
    }

    private void sendEvents(final String topic,
                            final long timestamp,
                            final List<KeyValue<Integer, String>> events) throws Exception {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            topic,
            events,
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                StringSerializer.class,
                new Properties()
            ),
            timestamp
        );
    }

    private KafkaStreams startStreams(final StreamsBuilder builder) throws InterruptedException {
        return startStreams(builder, REBALANCING, RUNNING, streamsConfiguration, null);
    }

    private KafkaStreams startStreams(final StreamsBuilder builder, final Properties streamsConfiguration) throws InterruptedException {
        return startStreams(builder, REBALANCING, RUNNING, streamsConfiguration, null);
    }

    private KafkaStreams startStreams(final StreamsBuilder builder,
                                      final State expectedOldState,
                                      final State expectedNewState,
                                      final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws InterruptedException {
        return startStreams(builder, expectedOldState, expectedNewState, streamsConfiguration, uncaughtExceptionHandler);
    }

    private KafkaStreams startStreams(final StreamsBuilder builder,
                                      final State expectedOldState,
                                      final State expectedNewState,
                                      final Properties streamsConfiguration,
                                      final Thread.UncaughtExceptionHandler uncaughtExceptionHandler) throws InterruptedException {
        final CountDownLatch latch;
        final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(streamsConfiguration), streamsConfiguration);

        if (uncaughtExceptionHandler == null) {
            latch = new CountDownLatch(1);
        } else {
            latch = new CountDownLatch(2);
            kafkaStreams.setUncaughtExceptionHandler(e -> {
                uncaughtExceptionHandler.uncaughtException(Thread.currentThread(), e);
                latch.countDown();
                if (e instanceof RuntimeException) {
                    throw (RuntimeException) e;
                } else if (e instanceof Error) {
                    throw (Error) e;
                } else {
                    throw new RuntimeException("Unexpected checked exception caught in the uncaught exception handler", e);
                }
            });
        }

        kafkaStreams.setStateListener((newState, oldState) -> {
            if (expectedOldState == oldState && expectedNewState == newState) {
                latch.countDown();
            }
        });

        kafkaStreams.start();

        latch.await(IntegrationTestUtils.DEFAULT_TIMEOUT, TimeUnit.MILLISECONDS);
        kafkaStreamsInstances.add(kafkaStreams);

        return kafkaStreams;
    }

    private <K, V> void validateReceivedMessages(final Deserializer<K> keySerializer,
                                                 final Deserializer<V> valueSerializer,
                                                 final List<KeyValue<K, V>> expectedRecords) throws Exception {

        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(
            ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            keySerializer.getClass().getName()
        );
        consumerProperties.setProperty(
            ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            valueSerializer.getClass().getName()
        );

        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(
            consumerProperties,
            outputTopic,
            expectedRecords
        );
    }
}
