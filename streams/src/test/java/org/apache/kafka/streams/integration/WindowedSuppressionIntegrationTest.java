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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.*;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import static java.time.Duration.ofMillis;
import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.StreamsConfig.AT_LEAST_ONCE;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.DEFAULT_TIMEOUT;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Category(IntegrationTest.class)
public class WindowedSuppressionIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        1,
        mkProperties(mkMap()),
        0L
    );
    private static final StringSerializer STRING_SERIALIZER = new StringSerializer();
    private static final int COMMIT_INTERVAL = 100;

    final Properties producerConfig = mkProperties(mkMap(
        mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
        mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
        mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ((Serializer<String>) STRING_SERIALIZER).getClass().getName()),
        mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
    ));

    final Properties consumerConfigString = mkProperties(mkMap(
        mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
        mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
        mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
        mkEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    ));

    final Properties consumerConfigDouble = mkProperties(mkMap(
        mkEntry(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
        mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()),
        mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getName()),
        mkEntry(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    ));

    final Producer<String, String> producer = new KafkaProducer<>(producerConfig);
    final Consumer<Object, Object> outputConsumer = new KafkaConsumer<>(consumerConfigDouble);
    final Consumer<Object, Object> lateArrivedConsumer = new KafkaConsumer<>(consumerConfigString);

    private final String inputTopic = "inputTopic-" + WindowedSuppressionIntegrationTest.class.getSimpleName();
    private final String outputTopic = "outputTopic-" + WindowedSuppressionIntegrationTest.class.getSimpleName();
    private final String lateArrived = "lateArrived-" + WindowedSuppressionIntegrationTest.class.getSimpleName();

    @Before
    public void setUp() {
        initConsumer(outputConsumer, outputTopic);
        initConsumer(lateArrivedConsumer, lateArrived);
        cleanStateBeforeTest(CLUSTER, inputTopic, outputTopic, lateArrived);
    }

    private void initConsumer(final Consumer<Object, Object> consumer, final String topic) {
        final List<TopicPartition> partitions =
            consumer.partitionsFor(topic)
                .stream()
                .map(pi -> new TopicPartition(pi.topic(), pi.partition()))
                .collect(Collectors.toList());
        consumer.assign(partitions);
        consumer.seekToBeginning(partitions);
    }


    @Test
    public void shouldEmitRecordAfterWindowClose() {
        final String testId = "-shouldEmitRecordAfterWindowClose";
        final String appId = getClass().getSimpleName().toLowerCase(Locale.getDefault()) + testId;
        final long windowSize = 1000L;


        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> inputStream = builder.stream(inputTopic, Consumed.with(Serdes.String(), Serdes.String()));

        inputStream
            .groupByKey()
            .windowedBy(TimeWindows.of(ofMillis(windowSize)).grace(ofMillis(windowSize)))
            .aggregate(
                () -> 0.0,
                (key, value, aggregate) -> Double.parseDouble(value),
                Named.as("aggregation" + appId),
                Materialized.<String, Double>as(Stores.inMemoryWindowStore("store" + appId, Duration.ofMillis(windowSize), Duration.ofMillis(windowSize), true))
                    .withKeySerde(Serdes.String())
                    .withValueSerde(Serdes.Double()),
                lateArrived
            )
            .suppress(untilWindowCloses(Suppressed.BufferConfig.unbounded()))
            .toStream()
            .selectKey((k, v) -> k.key())
            .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        final Properties streamsConfig = getStreamsConfig(appId);
        final KafkaStreams driver = IntegrationTestUtils.getStartedStreams(streamsConfig, builder, true);
        try {
            produceSynchronously(
                inputTopic,
                asList(
                    new KeyValueTimestamp<>("k1", "0", 0L),
                    new KeyValueTimestamp<>("k1", "0.3", windowSize / 3),
                    new KeyValueTimestamp<>("k1", "0.5", windowSize / 2),
                    new KeyValueTimestamp<>("k1", "0.9", windowSize - 1),
                    new KeyValueTimestamp<>("k1", "1.5", windowSize + windowSize / 2),
                    new KeyValueTimestamp<>("k1", "2", windowSize * 2),
                    new KeyValueTimestamp<>("k1", "3", windowSize * 3)
                )
            );

            final List<ConsumerRecord<Object, Object>> windows01And12 = waitForRecords(outputConsumer);
            assertThat(windows01And12.size(), is(2));
            assertThat(windows01And12.get(0).value(), is(0.9));
            assertThat(windows01And12.get(1).value(), is(1.5));

            produceSynchronously(
                inputTopic,
                asList(
                    new KeyValueTimestamp<>("k1", "2.1", windowSize / 2 + 1), //late
                    new KeyValueTimestamp<>("k1", "4", windowSize * 4)
                )
            );
            final List<ConsumerRecord<Object, Object>> windows23 = waitForRecords(outputConsumer);
            assertThat(windows23.size(), is(1));
            assertThat(windows23.get(0).value(), is(2.0));

            final List<ConsumerRecord<Object, Object>> lateArrival = waitForRecords(lateArrivedConsumer);
            assertThat(lateArrival.size(), is(1));
            assertThat(lateArrival.get(0).value(), is("2.1"));

        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    private List<ConsumerRecord<Object, Object>> waitForRecords(final Consumer<Object, Object> consumer) {
        final long start = System.currentTimeMillis();
        final List<ConsumerRecord<Object, Object>> result = new ArrayList<>();
        while ((System.currentTimeMillis() - start) < DEFAULT_TIMEOUT) {
            final ConsumerRecords<Object, Object> records = consumer.poll(ofMillis(500));
            if (!records.isEmpty()) {
                records.iterator().forEachRemaining(result::add);
            }
        }
        return result;
    }

    private void produceSynchronously(final String topic, final List<KeyValueTimestamp<String, String>> toProduce) {
        for (final KeyValueTimestamp<String, String> record : toProduce) {
            final Future<RecordMetadata> future = producer.send(
                new ProducerRecord<>(
                    topic,
                    null,
                    record.timestamp(),
                    record.key(),
                    record.value(),
                    null
                )
            );
            producer.flush();
            try {
                future.get();
            } catch (final InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private Properties getStreamsConfig(final String appId) {
        return mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.POLL_MS_CONFIG, Integer.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, Integer.toString(COMMIT_INTERVAL)),
            mkEntry(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, AT_LEAST_ONCE),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath())
        ));
    }

    @After
    public void tearDown() {
        outputConsumer.close();
        lateArrivedConsumer.close();
        producer.close();
    }
}
