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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
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
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertEquals;

@Category({IntegrationTest.class})
public class KStreamRepartitionIntegrationTest {
    private static final int NUM_BROKERS = 1;
    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    private static volatile AtomicInteger testNo = new AtomicInteger(0);
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;
    private String inputTopic = "input-topic";
    private String outputTopic = "output-topic";

    @Before
    public void before() throws InterruptedException {
        CLUSTER.createTopic(inputTopic, 3, 1);
        CLUSTER.createTopic(outputTopic, 1, 1);

        streamsConfiguration = new Properties();
        final String applicationId = "kstream-repartition-stream-test-" + testNo.incrementAndGet();
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
    }

    @Test
    public void shouldNotCreateRepartitionTopicIfKeyChangingOperationWasNotPerformed() throws ExecutionException, InterruptedException {
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
            .repartition(Repartitioned.as("smart-repartition"))
            .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new IntegerDeserializer(),
            new StringDeserializer(),
            Arrays.asList(
                new KeyValueTimestamp<>(1, "A", timestamp),
                new KeyValueTimestamp<>(2, "B", timestamp)
            )
        );

        final String topology = builder.build().describe().toString();

        assertEquals(0, getCountOfRepartitionTopicsFound(topology));
    }

    @Test
    public void shouldCreateRepartitionTopicWhenRepartitionKeySelectorIsUsed() throws ExecutionException, InterruptedException {
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        StreamsBuilder builder = new StreamsBuilder();

        final Repartitioned<String, String> repartitioned = Repartitioned.<String, String>as("smart-repartition")
            .withKeySerde(Serdes.String());

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
            .repartition((key, value) -> key.toString(), repartitioned)
            .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new StringDeserializer(),
            new StringDeserializer(),
            Arrays.asList(
                new KeyValueTimestamp<>("1", "A", timestamp),
                new KeyValueTimestamp<>("2", "B", timestamp)
            )
        );

        final String topology = builder.build().describe().toString();

        assertEquals(1, getCountOfRepartitionTopicsFound(topology));
    }

    @Test
    public void shouldNotBlowUpThings() throws ExecutionException, InterruptedException {
        final long timestamp = System.currentTimeMillis();

        sendEvents(
            timestamp,
            Arrays.asList(
                new KeyValue<>(1, "A"),
                new KeyValue<>(2, "B")
            )
        );

        StreamsBuilder builder = new StreamsBuilder();

        builder.stream(inputTopic, Consumed.with(Serdes.Integer(), Serdes.String()))
            .repartition(Repartitioned.as("smart-repartition"))
            .to(outputTopic);

        startStreams(builder);

        validateReceivedMessages(
            new IntegerDeserializer(),
            new StringDeserializer(),
            Arrays.asList(
                new KeyValueTimestamp<>(1, "A", timestamp),
                new KeyValueTimestamp<>(2, "B", timestamp)
            )
        );
    }

    private int getCountOfRepartitionTopicsFound(final String topologyString) {
        final Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }
        return repartitionTopicsFound.size();
    }

    private void sendEvents(long timestamp,
                            List<KeyValue<Integer, String>> events) throws ExecutionException, InterruptedException {
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

    private void startStreams(StreamsBuilder builder) {
        kafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration);
        kafkaStreams.start();
    }

    private <K, V> void validateReceivedMessages(Deserializer<K> keySerializer,
                                                 Deserializer<V> valueSerializer,
                                                 final List<KeyValueTimestamp<K, V>> expectedRecords)
        throws InterruptedException {
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

        IntegrationTestUtils.waitUntilFinalKeyValueTimestampRecordsReceived(
            consumerProperties,
            outputTopic,
            expectedRecords
        );
    }
}
