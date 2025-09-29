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
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.server.util.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Repartitioned;
import org.apache.kafka.streams.kstream.TableJoined;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.utils.UniqueTopicSerdeScope;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;

import static java.time.Duration.ofSeconds;
import static java.util.Arrays.asList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.startApplicationAndWaitUntilRunning;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.utils.TestUtils.waitForApplicationState;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;

@Timeout(600)
@Tag("integration")
public class KTableKTableForeignKeyInnerJoinCustomPartitionerIntegrationTest {
    private static final int NUM_BROKERS = 1;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private static final MockTime MOCK_TIME = CLUSTER.time;
    private static final String TABLE_1 = "table1";
    private static final String TABLE_2 = "table2";
    private static final String OUTPUT = "output-";
    private Properties streamsConfig;
    private Properties streamsConfigTwo;
    private Properties streamsConfigThree;
    private KafkaStreams streams;
    private KafkaStreams streamsTwo;
    private KafkaStreams streamsThree;
    private static final Properties CONSUMER_CONFIG = new Properties();
    private static final Properties PRODUCER_CONFIG_1 = new Properties();
    private static final Properties PRODUCER_CONFIG_2 = new Properties();

    static class MultiPartitioner implements StreamPartitioner<String, Void> {

        @Override
        public Optional<Set<Integer>> partitions(final String topic, final String key, final Void value, final int numPartitions) {
            return Optional.of(new HashSet<>(Arrays.asList(0, 1, 2)));
        }
    }

    @BeforeAll
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        //Use multiple partitions to ensure distribution of keys.

        CLUSTER.createTopic(TABLE_1, 4, 1);
        CLUSTER.createTopic(TABLE_2, 4, 1);
        CLUSTER.createTopic(OUTPUT, 4, 1);

        PRODUCER_CONFIG_1.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG_1.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG_1.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        PRODUCER_CONFIG_1.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        PRODUCER_CONFIG_2.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        PRODUCER_CONFIG_2.put(ProducerConfig.ACKS_CONFIG, "all");
        PRODUCER_CONFIG_2.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        PRODUCER_CONFIG_2.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);

        final List<KeyValue<String, String>> table1 = asList(
            new KeyValue<>("ID123-1", "ID123-A1"),
            new KeyValue<>("ID123-2", "ID123-A2"),
            new KeyValue<>("ID123-3", "ID123-A3"),
            new KeyValue<>("ID123-4", "ID123-A4")
        );

        final List<KeyValue<String, String>> table2 = Collections.singletonList(
            new KeyValue<>("ID123", "BBB")
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_1, table1, PRODUCER_CONFIG_1, MOCK_TIME);
        IntegrationTestUtils.produceKeyValuesSynchronously(TABLE_2, table2, PRODUCER_CONFIG_2, MOCK_TIME);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-ktable-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @BeforeEach
    public void before(final TestInfo testInfo) throws IOException {
        final String stateDirBasePath = TestUtils.tempDirectory().getPath();
        final String safeTestName = safeUniqueTestName(testInfo);
        streamsConfig = getStreamsConfig(safeTestName);
        streamsConfigTwo = getStreamsConfig(safeTestName);
        streamsConfigThree = getStreamsConfig(safeTestName);
        streamsConfig.put(StreamsConfig.STATE_DIR_CONFIG, stateDirBasePath + "-1");
        streamsConfigTwo.put(StreamsConfig.STATE_DIR_CONFIG, stateDirBasePath + "-2");
        streamsConfigThree.put(StreamsConfig.STATE_DIR_CONFIG, stateDirBasePath + "-3");
    }

    @AfterEach
    public void after() throws IOException {
        if (streams != null) {
            streams.close(Duration.ofSeconds(60));
            streams = null;
        }
        if (streamsTwo != null) {
            streamsTwo.close(Duration.ofSeconds(60));
            streamsTwo = null;
        }
        if (streamsThree != null) {
            streamsThree.close(Duration.ofSeconds(60));
            streamsThree = null;
        }
        IntegrationTestUtils.purgeLocalStreamsState(asList(streamsConfig, streamsConfigTwo, streamsConfigThree));
    }

    @Test
    public void shouldInnerJoinMultiPartitionQueryable() throws Exception {
        final Set<KeyValue<String, String>> expectedOne = new HashSet<>();
        expectedOne.add(new KeyValue<>("ID123-1", "value1=ID123-A1,value2=BBB"));
        expectedOne.add(new KeyValue<>("ID123-2", "value1=ID123-A2,value2=BBB"));
        expectedOne.add(new KeyValue<>("ID123-3", "value1=ID123-A3,value2=BBB"));
        expectedOne.add(new KeyValue<>("ID123-4", "value1=ID123-A4,value2=BBB"));

        verifyKTableKTableJoin(expectedOne);
    }

    @Test
    public void shouldThrowIllegalArgumentExceptionWhenCustomPartitionerReturnsMultiplePartitions() throws Exception {
        final String innerJoinType = "INNER";
        final String queryableName = innerJoinType + "-store1";

        streams = prepareTopologyWithNonSingletonPartitions(queryableName, streamsConfig);
        streamsTwo = prepareTopologyWithNonSingletonPartitions(queryableName, streamsConfigTwo);
        streamsThree = prepareTopologyWithNonSingletonPartitions(queryableName, streamsConfigThree);

        final List<KafkaStreams> kafkaStreamsList = asList(streams, streamsTwo, streamsThree);

        for (final KafkaStreams stream: kafkaStreamsList) {
            stream.setUncaughtExceptionHandler(e -> {
                assertThat(e.getCause().getMessage(), equalTo("The partitions returned by StreamPartitioner#partitions method when used for FK join should be a singleton set"));
                return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
            });
        }

        for (final KafkaStreams stream: kafkaStreamsList) {
            stream.start();
        }

        // the streams applications should have shut down into `ERROR` due to the IllegalStateException
        waitForApplicationState(Arrays.asList(streams, streamsTwo, streamsThree), KafkaStreams.State.ERROR, ofSeconds(240));
    }

    private void verifyKTableKTableJoin(final Set<KeyValue<String, String>> expectedResult) throws Exception {
        final String innerJoinType = "INNER";
        final String queryableName = innerJoinType + "-store1";

        streams = prepareTopology(queryableName, streamsConfig);
        streamsTwo = prepareTopology(queryableName, streamsConfigTwo);
        streamsThree = prepareTopology(queryableName, streamsConfigThree);

        final List<KafkaStreams> kafkaStreamsList = asList(streams, streamsTwo, streamsThree);
        startApplicationAndWaitUntilRunning(kafkaStreamsList, ofSeconds(120));

        final Set<KeyValue<String, String>> result = new HashSet<>(waitUntilMinKeyValueRecordsReceived(
            CONSUMER_CONFIG,
            OUTPUT,
            expectedResult.size()));

        assertThat(expectedResult, equalTo(result));
    }

    private Properties getStreamsConfig(final String testName) {
        final Properties streamsConfig = new Properties();
        streamsConfig.put(StreamsConfig.APPLICATION_ID_CONFIG, "KTable-FKJ-Partitioner-" + testName);
        streamsConfig.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfig.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfig.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L);

        return streamsConfig;
    }

    private static KafkaStreams prepareTopology(final String queryableName, final Properties streamsConfig) {

        final UniqueTopicSerdeScope serdeScope = new UniqueTopicSerdeScope();
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> table1 = builder.stream(TABLE_1,
            Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true), serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)))
            .repartition(repartitionA())
            .toTable(Named.as("table.a"));

        final KTable<String, String> table2 = builder
            .stream(TABLE_2,
                Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true), serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)))
            .repartition(repartitionB())
            .toTable(Named.as("table.b"));

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized;
        if (queryableName != null) {
            materialized = Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(queryableName)
                .withKeySerde(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true))
                .withValueSerde(serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
                .withCachingDisabled();
        } else {
            throw new RuntimeException("Current implementation of joinOnForeignKey requires a materialized store");
        }

        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "value1=" + value1 + ",value2=" + value2;

        final TableJoined<String, String> tableJoined = TableJoined.with(
            (topic, key, value, numPartitions) -> Optional.of(Collections.singleton(Math.abs(getKeyB(key).hashCode()) % numPartitions)),
            (topic, key, value, numPartitions) -> Optional.of(Collections.singleton(Math.abs(key.hashCode()) % numPartitions))
        );

        table1.join(table2, KTableKTableForeignKeyInnerJoinCustomPartitionerIntegrationTest::getKeyB, joiner, tableJoined, materialized)
            .toStream()
            .to(OUTPUT,
                Produced.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                    serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)));

        return new KafkaStreams(builder.build(streamsConfig), streamsConfig);
    }

    private static KafkaStreams prepareTopologyWithNonSingletonPartitions(final String queryableName, final Properties streamsConfig) {

        final UniqueTopicSerdeScope serdeScope = new UniqueTopicSerdeScope();
        final StreamsBuilder builder = new StreamsBuilder();

        final KTable<String, String> table1 = builder.stream(TABLE_1,
                        Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true), serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)))
                .repartition(repartitionA())
                .toTable(Named.as("table.a"));

        final KTable<String, String> table2 = builder
                .stream(TABLE_2,
                        Consumed.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true), serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)))
                .repartition(repartitionB())
                .toTable(Named.as("table.b"));

        final Materialized<String, String, KeyValueStore<Bytes, byte[]>> materialized;
        if (queryableName != null) {
            materialized = Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(queryableName)
                    .withKeySerde(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true))
                    .withValueSerde(serdeScope.decorateSerde(Serdes.String(), streamsConfig, false))
                    .withCachingDisabled();
        } else {
            throw new RuntimeException("Current implementation of joinOnForeignKey requires a materialized store");
        }

        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "value1=" + value1 + ",value2=" + value2;

        final TableJoined<String, String> tableJoined = TableJoined.with(
                new MultiPartitioner(),
                (topic, key, value, numPartitions) -> Optional.of(Collections.singleton(Math.abs(key.hashCode()) % numPartitions))
        );

        table1.join(table2, KTableKTableForeignKeyInnerJoinCustomPartitionerIntegrationTest::getKeyB, joiner, tableJoined, materialized)
                .toStream()
                .to(OUTPUT,
                        Produced.with(serdeScope.decorateSerde(Serdes.String(), streamsConfig, true),
                                serdeScope.decorateSerde(Serdes.String(), streamsConfig, false)));

        return new KafkaStreams(builder.build(streamsConfig), streamsConfig);
    }

    private static Repartitioned<String, String> repartitionA() {
        final Repartitioned<String, String> repartitioned = Repartitioned.as("a");
        return repartitioned.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            .withStreamPartitioner((topic, key, value, numPartitions) -> Optional.of(Collections.singleton(Math.abs(getKeyB(key).hashCode()) % numPartitions)))
            .withNumberOfPartitions(4);
    }

    private static Repartitioned<String, String> repartitionB() {
        final Repartitioned<String, String> repartitioned = Repartitioned.as("b");
        return repartitioned.withKeySerde(Serdes.String()).withValueSerde(Serdes.String())
            .withStreamPartitioner((topic, key, value, numPartitions) -> Optional.of(Collections.singleton(Math.abs(key.hashCode()) % numPartitions)))
            .withNumberOfPartitions(4);
    }

    private static String getKeyB(final String value) {
        return value.substring(0, value.indexOf("-"));
    }

}
