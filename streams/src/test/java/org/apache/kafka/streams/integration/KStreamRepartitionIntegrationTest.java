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
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Category({IntegrationTest.class})
public class KStreamRepartitionIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private static AtomicInteger testNo = new AtomicInteger(0);

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String inputTopic = "input-topic-" + testNo.get();
    private String outputTopic = "output-topic-" + testNo.get();
    private String applicationId = "kstream-repartition-stream-test-" + testNo.get();

    @Before
    public void before() throws InterruptedException {
        CLUSTER.createTopic(inputTopic, 3, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);

        streamsConfiguration = new Properties();

        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
    }

    @After
    public void whenShuttingDown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
        testNo.incrementAndGet();
    }

    @Test
    public void shouldNotCreateRepartitionTopicIfKeyChangingOperationWasNotPerformed() throws ExecutionException, InterruptedException {
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

        assertFalse(topicExists(toRepartitionTopicName(repartitionName)));
        assertEquals(0, getCountOfRepartitionTopicsFound(topology, "Sink: .*-smart-repartition"));
    }

    @Test
    public void shouldCreateRepartitionTopicWhenRepartitionKeySelectorIsUsed() throws ExecutionException, InterruptedException {
        final String repartitionName = "new-key";
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
            .repartition((key, value) -> key.toString(), Repartitioned.as(repartitionName))
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

        assertTrue(topicExists(toRepartitionTopicName(repartitionName)));
        assertEquals(1, getCountOfRepartitionTopicsFound(topology, "Sink: .*" + repartitionName + "-repartition.*"));
    }

    @Test
    public void shouldCreateRepartitionTopicWhenNumberOfPartitionsIsSpecified() throws ExecutionException, InterruptedException {
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
            .repartition(Repartitioned.<Integer, String>as(repartitionName).withNumberOfPartitions(1))
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

        final String topology = builder.build().describe().toString();

        assertTrue(topicExists(toRepartitionTopicName(repartitionName)));
        assertEquals(1, getCountOfRepartitionTopicsFound(topology, "Sink: .*" + repartitionName + "-repartition.*"));
    }

    @Test
    public void shouldCreateOnlyOneRepartitionTopicForKStreamGroupBy() throws ExecutionException, InterruptedException {
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
            .selectKey((key, value) -> key.toString())
            .repartition(Repartitioned.<String, String>as(repartitionName).withNumberOfPartitions(1))
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
        assertEquals(1, getCountOfRepartitionTopicsFound(topology, "Sink: .*-repartition"));
    }

    @Test
    public void shouldGenerateRepartitionTopicWhenNameIsNotSpecified() throws ExecutionException, InterruptedException {
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
            .repartition()
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

        assertEquals(1, getCountOfRepartitionTopicsFound(topology, "Sink: .*-repartition"));
    }

    private boolean topicExists(final String topic) throws InterruptedException, ExecutionException {
        try (AdminClient adminClient = createAdminClient()) {
            final Set<String> topics = adminClient.listTopics()
                .names()
                .get();

            return topics.contains(topic);
        }
    }

    private String toRepartitionTopicName(final String input) {
        return applicationId + "-" + input + "-repartition";
    }

    private AdminClient createAdminClient() {
        final Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());

        return AdminClient.create(properties);
    }

    private int getCountOfRepartitionTopicsFound(final String topologyString,
                                                 final String searchPattern) {
        final Matcher matcher = Pattern.compile(searchPattern).matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }
        return repartitionTopicsFound.size();
    }

    private void sendEvents(final long timestamp,
                            final List<KeyValue<Integer, String>> events) throws ExecutionException, InterruptedException {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
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

    private void startStreams(final StreamsBuilder builder) {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private <K, V> void validateReceivedMessages(final Deserializer<K> keySerializer,
                                                 final Deserializer<V> valueSerializer,
                                                 final List<KeyValue<K, V>> expectedRecords) throws InterruptedException {

        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "kstream-repartition-test-" + testNo);
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
