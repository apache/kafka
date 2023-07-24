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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.Tag;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Timeout(600)
@Tag("integration")
public class KTableKTableForeignKeyJoinDistributedTest {
    private static final int NUM_BROKERS = 1;
    private static final String LEFT_TABLE = "left_table";
    private static final String RIGHT_TABLE = "right_table";
    private static final String OUTPUT = "output-topic";
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    @BeforeAll
    public static void startCluster() throws IOException, InterruptedException {
        CLUSTER.start();
        CLUSTER.createTopic(INPUT_TOPIC, 2, 1);
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    private static final Properties CONSUMER_CONFIG = new Properties();

    private static final String INPUT_TOPIC = "input-topic";

    private KafkaStreams client1;
    private KafkaStreams client2;

    private volatile boolean client1IsOk = false;
    private volatile boolean client2IsOk = false;

    @BeforeEach
    public void setupTopics() throws InterruptedException {
        CLUSTER.createTopic(LEFT_TABLE, 1, 1);
        CLUSTER.createTopic(RIGHT_TABLE, 1, 1);
        CLUSTER.createTopic(OUTPUT, 11, 1);

        //Fill test tables
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        final List<KeyValue<String, String>> leftTable = Arrays.asList(
                new KeyValue<>("lhsValue1", "lhsValue1|rhs1"),
                new KeyValue<>("lhsValue2", "lhsValue2|rhs2"),
                new KeyValue<>("lhsValue3", "lhsValue3|rhs3"),
                new KeyValue<>("lhsValue4", "lhsValue4|rhs4")
        );
        final List<KeyValue<String, String>> rightTable = Arrays.asList(
                new KeyValue<>("rhs1", "rhsValue1"),
                new KeyValue<>("rhs2", "rhsValue2"),
                new KeyValue<>("rhs3", "rhsValue3")
        );

        IntegrationTestUtils.produceKeyValuesSynchronously(LEFT_TABLE, leftTable, producerConfig, CLUSTER.time);
        IntegrationTestUtils.produceKeyValuesSynchronously(RIGHT_TABLE, rightTable, producerConfig, CLUSTER.time);

        CONSUMER_CONFIG.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        CONSUMER_CONFIG.put(ConsumerConfig.GROUP_ID_CONFIG, "ktable-ktable-distributed-consumer");
        CONSUMER_CONFIG.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        CONSUMER_CONFIG.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    }

    @AfterEach
    public void after() {
        client1.close();
        client2.close();
        quietlyCleanStateAfterTest(CLUSTER, client1);
        quietlyCleanStateAfterTest(CLUSTER, client2);
    }

    public Properties getStreamsConfiguration(final TestInfo testInfo) {
        final String safeTestName = safeUniqueTestName(getClass(), testInfo);
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        return streamsConfiguration;
    }


    private void configureBuilder(final StreamsBuilder builder) {
        final KTable<String, String> left = builder.table(
                LEFT_TABLE
        );
        final KTable<String, String> right = builder.table(
                RIGHT_TABLE
        );

        final Function<String, String> extractor = value -> value.split("\\|")[1];
        final ValueJoiner<String, String, String> joiner = (value1, value2) -> "(" + value1 + "," + value2 + ")";

        final KTable<String, String> fkJoin = left.join(right, extractor, joiner);
        fkJoin
                .toStream()
                .to(OUTPUT);
    }

    @Test
    public void shouldBeInitializedWithDefaultSerde(final TestInfo testInfo) throws Exception {
        final Properties streamsConfiguration1 = getStreamsConfiguration(testInfo);
        final Properties streamsConfiguration2 = getStreamsConfiguration(testInfo);

        //Each streams client needs to have it's own StreamsBuilder in order to simulate
        //a truly distributed run
        final StreamsBuilder builder1 = new StreamsBuilder();
        configureBuilder(builder1);
        final StreamsBuilder builder2 = new StreamsBuilder();
        configureBuilder(builder2);


        createClients(
                builder1.build(streamsConfiguration1),
                streamsConfiguration1,
                builder2.build(streamsConfiguration2),
                streamsConfiguration2
        );

        setStateListenersForVerification(thread -> !thread.activeTasks().isEmpty());

        startClients();

        waitUntilBothClientAreOK(
                "At least one client did not reach state RUNNING with active tasks"
        );
        final Set<KeyValue<String, String>> expectedResult = new HashSet<>();
        expectedResult.add(new KeyValue<>("lhsValue1", "(lhsValue1|rhs1,rhsValue1)"));
        expectedResult.add(new KeyValue<>("lhsValue2", "(lhsValue2|rhs2,rhsValue2)"));
        expectedResult.add(new KeyValue<>("lhsValue3", "(lhsValue3|rhs3,rhsValue3)"));
        final Set<KeyValue<String, String>> result = new HashSet<>(IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
                CONSUMER_CONFIG,
                OUTPUT,
                expectedResult.size()));

        assertEquals(expectedResult, result);
        //Check that both clients are still running
        assertEquals(KafkaStreams.State.RUNNING, client1.state());
        assertEquals(KafkaStreams.State.RUNNING, client2.state());
    }

    private void createClients(final Topology topology1,
                               final Properties streamsConfiguration1,
                               final Topology topology2,
                               final Properties streamsConfiguration2) {

        client1 = new KafkaStreams(topology1, streamsConfiguration1);
        client2 = new KafkaStreams(topology2, streamsConfiguration2);
    }

    private void setStateListenersForVerification(final Predicate<ThreadMetadata> taskCondition) {
        client1.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING &&
                    client1.metadataForLocalThreads().stream().allMatch(taskCondition)) {
                client1IsOk = true;
            }
        });
        client2.setStateListener((newState, oldState) -> {
            if (newState == KafkaStreams.State.RUNNING &&
                    client2.metadataForLocalThreads().stream().allMatch(taskCondition)) {
                client2IsOk = true;
            }
        });
    }

    private void startClients() {
        client1.start();
        client2.start();
    }

    private void waitUntilBothClientAreOK(final String message) throws Exception {
        TestUtils.waitForCondition(() -> client1IsOk && client2IsOk,
                30 * 1000,
                message + ": "
                        + "Client 1 is " + (!client1IsOk ? "NOT " : "") + "OK, "
                        + "client 2 is " + (!client2IsOk ? "NOT " : "") + "OK."
        );
    }
}
