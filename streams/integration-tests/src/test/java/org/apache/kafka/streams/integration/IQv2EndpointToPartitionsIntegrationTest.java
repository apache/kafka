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
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.StreamsMetadata;
import org.apache.kafka.streams.ThreadMetadata;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Stream;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

@Timeout(600)
@Tag("integration")
public class IQv2EndpointToPartitionsIntegrationTest {
    private String appId;
    private String inputTopicTwoPartitions;
    private String outputTopicTwoPartitions;
    private Properties streamsApplicationProperties = new Properties();
    private Properties streamsSecondApplicationProperties = new Properties();

    private static EmbeddedKafkaCluster cluster;
    private static final int NUM_BROKERS = 3;
    private static final String EXPECTED_STORE_NAME = "IQTest-count";

    public void startCluster(final int standbyConfig) throws IOException {
        final Properties properties = new Properties();
        properties.put(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, standbyConfig);
        cluster = new EmbeddedKafkaCluster(NUM_BROKERS, properties);
        cluster.start();
    }

    public void setUp() throws InterruptedException {
        appId = safeUniqueTestName("endpointIntegrationTest");
        inputTopicTwoPartitions = appId + "-input-two";
        outputTopicTwoPartitions = appId + "-output-two";
        cluster.createTopic(inputTopicTwoPartitions, 2, 1);
        cluster.createTopic(outputTopicTwoPartitions, 2, 1);
    }

    public void closeCluster() {
        cluster.stop();
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(streamsApplicationProperties);
        if (!streamsSecondApplicationProperties.isEmpty()) {
            IntegrationTestUtils.purgeLocalStreamsState(streamsSecondApplicationProperties);
        }
    }

    @ParameterizedTest(name = "{3}")
    @MethodSource("groupProtocolParameters")
    public void shouldGetCorrectHostPartitionInformation(final String groupProtocolConfig,
                                                         final boolean usingStandbyReplicas,
                                                         final int numStandbyReplicas,
                                                         final String testName) throws Exception {
        try {
            startCluster(usingStandbyReplicas ? numStandbyReplicas : 0);
            setUp();

            final Properties streamOneProperties = new Properties();
            streamOneProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks1");
            streamOneProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks1");
            streamOneProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:2020");
            streamOneProperties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocolConfig);
            if (usingStandbyReplicas) {
                streamOneProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
            }
            streamsApplicationProperties = props(streamOneProperties);

            final Properties streamTwoProperties = new Properties();
            streamTwoProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks2");
            streamTwoProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks2");
            streamTwoProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:3030");
            streamTwoProperties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocolConfig);
            if (usingStandbyReplicas) {
                streamTwoProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
            }
            streamsSecondApplicationProperties = props(streamTwoProperties);

            final Topology topology = complexTopology();
            try (final KafkaStreams streamsOne = new KafkaStreams(topology, streamsApplicationProperties)) {
                IntegrationTestUtils.startApplicationAndWaitUntilRunning(streamsOne);
                waitForCondition(() -> !streamsOne.metadataForAllStreamsClients().isEmpty(),
                        IntegrationTestUtils.DEFAULT_TIMEOUT,
                        () -> "Kafka Streams didn't get metadata about the client.");
                waitForCondition(() -> streamsOne.metadataForAllStreamsClients().iterator().next().topicPartitions().size() == 4,
                        IntegrationTestUtils.DEFAULT_TIMEOUT,
                        () -> "Kafka Streams one didn't get 4 tasks");
                final List<StreamsMetadata> streamsMetadataAllClients = new ArrayList<>(streamsOne.metadataForAllStreamsClients());
                assertEquals(1, streamsMetadataAllClients.size());
                final StreamsMetadata streamsOneInitialMetadata = streamsMetadataAllClients.get(0);
                assertEquals(2020, streamsOneInitialMetadata.hostInfo().port());
                final Set<TopicPartition> topicPartitions = streamsOneInitialMetadata.topicPartitions();
                assertEquals(4, topicPartitions.size());
                assertEquals(0, streamsOneInitialMetadata.standbyTopicPartitions().size());

                final long repartitionTopicTaskCount = topicPartitions.stream().filter(tp -> tp.topic().contains("-repartition")).count();
                final long sourceTopicTaskCount = topicPartitions.stream().filter(tp -> tp.topic().contains("-input-two")).count();
                assertEquals(2, repartitionTopicTaskCount);
                assertEquals(2, sourceTopicTaskCount);
                final int expectedStandbyCount = usingStandbyReplicas ? 1 : 0;

                try (final KafkaStreams streamsTwo = new KafkaStreams(topology, streamsSecondApplicationProperties)) {
                    streamsTwo.start();
                    waitForCondition(() -> KafkaStreams.State.RUNNING == streamsTwo.state() && KafkaStreams.State.RUNNING == streamsOne.state(),
                            IntegrationTestUtils.DEFAULT_TIMEOUT,
                            () -> "Kafka Streams one or two never transitioned to a RUNNING state.");

                    waitForCondition(() ->  {
                        final ThreadMetadata threadMetadata = streamsOne.metadataForLocalThreads().iterator().next();
                        return threadMetadata.activeTasks().size() == 2 && threadMetadata.standbyTasks().size() == expectedStandbyCount;
                    }, TestUtils.DEFAULT_MAX_WAIT_MS,
                            "KafkaStreams one never released active tasks and received standby task");

                    waitForCondition(() -> {
                        final ThreadMetadata threadMetadata = streamsTwo.metadataForLocalThreads().iterator().next();
                        return threadMetadata.activeTasks().size() == 2 && threadMetadata.standbyTasks().size() == expectedStandbyCount;
                    }, TestUtils.DEFAULT_MAX_WAIT_MS,
                            "KafkaStreams two never received active tasks and standby");

                    waitForCondition(() -> {
                        final List<StreamsMetadata> metadata = new ArrayList<>(streamsTwo.metadataForAllStreamsClients());
                        return metadata.size() == 2 &&
                               metadata.get(0).standbyTopicPartitions().size() == expectedStandbyCount &&
                               metadata.get(1).standbyTopicPartitions().size() == expectedStandbyCount;
                    }, TestUtils.DEFAULT_MAX_WAIT_MS,
                            "Kafka Streams clients 1 and 2 never got metadata about standby tasks");

                    waitForCondition(() -> streamsOne.metadataForAllStreamsClients().iterator().next().topicPartitions().size() == 2,
                            IntegrationTestUtils.DEFAULT_TIMEOUT,
                            () -> "Kafka Streams one didn't give up active tasks");

                    final List<StreamsMetadata> allClientMetadataUpdated = new ArrayList<>(streamsTwo.metadataForAllStreamsClients());

                    final StreamsMetadata streamsOneMetadata = allClientMetadataUpdated.get(0);
                    final Set<TopicPartition> streamsOneActiveTopicPartitions = streamsOneMetadata.topicPartitions();
                    final Set<TopicPartition> streamsOneStandbyTopicPartitions = streamsOneMetadata.standbyTopicPartitions();
                    final Set<String> streamsOneStoreNames = streamsOneMetadata.stateStoreNames();
                    final Set<String> streamsOneStandbyStoreNames = streamsOneMetadata.standbyStateStoreNames();

                    assertEquals(2020, streamsOneMetadata.hostInfo().port());
                    assertEquals(2, streamsOneActiveTopicPartitions.size());
                    assertEquals(expectedStandbyCount, streamsOneStandbyTopicPartitions.size());
                    assertEquals(1, streamsOneStoreNames.size());
                    assertEquals(expectedStandbyCount, streamsOneStandbyStoreNames.size());
                    assertEquals(EXPECTED_STORE_NAME, streamsOneStoreNames.iterator().next());
                    if (usingStandbyReplicas) {
                        assertEquals(EXPECTED_STORE_NAME, streamsOneStandbyStoreNames.iterator().next());
                    }

                    final long streamsOneRepartitionTopicCount = streamsOneActiveTopicPartitions.stream().filter(tp -> tp.topic().contains("-repartition")).count();
                    final long streamsOneSourceTopicCount = streamsOneActiveTopicPartitions.stream().filter(tp -> tp.topic().contains("-input-two")).count();
                    assertEquals(1, streamsOneRepartitionTopicCount);
                    assertEquals(1, streamsOneSourceTopicCount);

                    final StreamsMetadata streamsTwoMetadata = allClientMetadataUpdated.get(1);
                    final Set<TopicPartition> streamsTwoActiveTopicPartitions = streamsTwoMetadata.topicPartitions();
                    final Set<TopicPartition> streamsTwoStandbyTopicPartitions = streamsTwoMetadata.standbyTopicPartitions();
                    final Set<String> streamsTwoStateStoreNames = streamsTwoMetadata.stateStoreNames();
                    final Set<String> streamsTwoStandbyStateStoreNames = streamsTwoMetadata.standbyStateStoreNames();

                    assertEquals(3030, streamsTwoMetadata.hostInfo().port());
                    assertEquals(2, streamsTwoActiveTopicPartitions.size());
                    assertEquals(expectedStandbyCount, streamsTwoStandbyTopicPartitions.size());
                    assertEquals(1, streamsTwoStateStoreNames.size());
                    assertEquals(expectedStandbyCount, streamsTwoStandbyStateStoreNames.size());
                    assertEquals(EXPECTED_STORE_NAME, streamsTwoStateStoreNames.iterator().next());
                    if (usingStandbyReplicas) {
                        assertEquals(EXPECTED_STORE_NAME, streamsTwoStandbyStateStoreNames.iterator().next());
                    }

                    final long streamsTwoRepartitionTopicCount = streamsTwoActiveTopicPartitions.stream().filter(tp -> tp.topic().contains("-repartition")).count();
                    final long streamsTwoSourceTopicCount = streamsTwoActiveTopicPartitions.stream().filter(tp -> tp.topic().contains("-input-two")).count();
                    assertEquals(1, streamsTwoRepartitionTopicCount);
                    assertEquals(1, streamsTwoSourceTopicCount);

                    if (usingStandbyReplicas) {
                        final TopicPartition streamsOneStandbyTopicPartition = streamsOneStandbyTopicPartitions.iterator().next();
                        final TopicPartition streamsTwoStandbyTopicPartition = streamsTwoStandbyTopicPartitions.iterator().next();
                        final String streamsOneStandbyTopicName = streamsOneStandbyTopicPartition.topic();
                        final String streamsTwoStandbyTopicName = streamsTwoStandbyTopicPartition.topic();
                        assertEquals(streamsOneStandbyTopicName, streamsTwoStandbyTopicName);
                        assertNotEquals(streamsOneStandbyTopicPartition.partition(), streamsTwoStandbyTopicPartition.partition());
                    }
                }
            }
        } finally {
            closeCluster();
        }
    }

    private static Stream<Arguments> groupProtocolParameters() {
        return Stream.of(Arguments.of("streams", false, 0, "STREAMS protocol No standby"),
                Arguments.of("classic", false, 0, "CLASSIC protocol No standby"),
                Arguments.of("streams", true, 1, "STREAMS protocol With standby"),
                Arguments.of("classic", true, 1, "CLASSIC protocol With standby"));
    }

    private Properties props(final Properties extraProperties) {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        streamsConfiguration.putAll(extraProperties);
        return streamsConfiguration;
    }

    private Topology complexTopology() {
        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopicTwoPartitions, Consumed.with(Serdes.String(), Serdes.String()))
                .flatMapValues(value -> Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+")))
                .groupBy((key, value) -> value, Grouped.as("IQTest"))
                .count(Materialized.as(EXPECTED_STORE_NAME))
                .toStream().to(outputTopicTwoPartitions, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }
}
