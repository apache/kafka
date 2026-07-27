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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        cluster.createTopic(inputTopicTwoPartitions, 4, 1);
        cluster.createTopic(outputTopicTwoPartitions, 4, 1);
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
            streamOneProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
            streamOneProperties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocolConfig);
            if (usingStandbyReplicas) {
                streamOneProperties.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, numStandbyReplicas);
            }
            streamsApplicationProperties = props(streamOneProperties);

            final Properties streamTwoProperties = new Properties();
            streamTwoProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks2");
            streamTwoProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks2");
            streamTwoProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:3030");
            streamTwoProperties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2);
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
                waitForCondition(() -> streamsOne.metadataForAllStreamsClients().iterator().next().topicPartitions().size() == 8,
                        IntegrationTestUtils.DEFAULT_TIMEOUT,
                        () -> "Kafka Streams one didn't get 8 tasks");
                final List<StreamsMetadata> streamsMetadataAllClients = new ArrayList<>(streamsOne.metadataForAllStreamsClients());
                assertEquals(1, streamsMetadataAllClients.size());
                final StreamsMetadata streamsOneInitialMetadata = streamsMetadataAllClients.get(0);
                assertEquals(2020, streamsOneInitialMetadata.hostInfo().port());
                final Set<TopicPartition> topicPartitions = streamsOneInitialMetadata.topicPartitions();
                assertEquals(8, topicPartitions.size());
                assertEquals(0, streamsOneInitialMetadata.standbyTopicPartitions().size());

                final long repartitionTopicTaskCount = topicPartitions.stream().filter(tp -> tp.topic().contains("-repartition")).count();
                final long sourceTopicTaskCount = topicPartitions.stream().filter(tp -> tp.topic().contains("-input-two")).count();
                assertEquals(4, repartitionTopicTaskCount);
                assertEquals(4, sourceTopicTaskCount);
                final int expectedStandbyCount = usingStandbyReplicas ? 2 : 0;
                final int expectedStandbyStoreCount = usingStandbyReplicas ? 1 : 0;

                try (final KafkaStreams streamsTwo = new KafkaStreams(topology, streamsSecondApplicationProperties)) {
                    streamsTwo.start();
                    waitForCondition(() -> KafkaStreams.State.RUNNING == streamsTwo.state() && KafkaStreams.State.RUNNING == streamsOne.state(),
                            IntegrationTestUtils.DEFAULT_TIMEOUT,
                            () -> "Kafka Streams one or two never transitioned to a RUNNING state.");

                    waitForCondition(() ->  {
                        final int totalActiveOnStreamsOne = streamsOne.metadataForLocalThreads().stream()
                            .mapToInt(t -> t.activeTasks().size()).sum();
                        final int totalStandbyOnStreamsOne = streamsOne.metadataForLocalThreads().stream()
                            .mapToInt(t -> t.standbyTasks().size()).sum();
                        return totalActiveOnStreamsOne == 4 && totalStandbyOnStreamsOne == expectedStandbyCount;
                    }, TestUtils.DEFAULT_MAX_WAIT_MS,
                            "KafkaStreams one never released active tasks and received standby task");

                    waitForCondition(() -> {
                        final int totalActiveOnStreamsTwo = streamsTwo.metadataForLocalThreads().stream()
                            .mapToInt(t -> t.activeTasks().size()).sum();
                        final int totalStandbyOnStreamsTwo = streamsTwo.metadataForLocalThreads().stream()
                            .mapToInt(t -> t.standbyTasks().size()).sum();
                        return totalActiveOnStreamsTwo == 4 && totalStandbyOnStreamsTwo == expectedStandbyCount;
                    }, TestUtils.DEFAULT_MAX_WAIT_MS,
                            "KafkaStreams two never received active tasks and standby");

                    waitForCondition(() -> {
                        final List<StreamsMetadata> metadata = new ArrayList<>(streamsTwo.metadataForAllStreamsClients());
                        return metadata.size() == 2 &&
                               metadata.get(0).standbyTopicPartitions().size() == expectedStandbyCount &&
                               metadata.get(1).standbyTopicPartitions().size() == expectedStandbyCount;
                    }, TestUtils.DEFAULT_MAX_WAIT_MS,
                            "Kafka Streams clients 1 and 2 never got metadata about standby tasks");

                    waitForCondition(() -> streamsOne.metadataForAllStreamsClients().iterator().next().topicPartitions().size() == 4,
                            IntegrationTestUtils.DEFAULT_TIMEOUT,
                            () -> "Kafka Streams one didn't give up active tasks");

                    verifyClientMetadata(usingStandbyReplicas, new ArrayList<>(streamsTwo.metadataForAllStreamsClients()), expectedStandbyCount, expectedStandbyStoreCount);
                }
            }
        } finally {
            closeCluster();
        }
    }

    private static void verifyClientMetadata(
            final boolean usingStandbyReplicas,
            final List<StreamsMetadata> allClientMetadataUpdated,
            final int expectedStandbyCount,
            final int expectedStandbyStoreCount
    ) {
        final StreamsMetadata streamsOneMetadata = allClientMetadataUpdated.get(0);
        final StreamsMetadata streamsTwoMetadata = allClientMetadataUpdated.get(1);

        verifyHostMetadata(streamsOneMetadata, 2020, expectedStandbyCount, expectedStandbyStoreCount, usingStandbyReplicas);
        verifyHostMetadata(streamsTwoMetadata, 3030, expectedStandbyCount, expectedStandbyStoreCount, usingStandbyReplicas);

        if (usingStandbyReplicas) {
            final Set<TopicPartition> streamsOneActiveRepartition = streamsOneMetadata.topicPartitions().stream()
                .filter(tp -> tp.topic().contains("-repartition")).collect(Collectors.toSet());
            final Set<TopicPartition> streamsTwoActiveRepartition = streamsTwoMetadata.topicPartitions().stream()
                .filter(tp -> tp.topic().contains("-repartition")).collect(Collectors.toSet());
            assertEquals(streamsTwoActiveRepartition, streamsOneMetadata.standbyTopicPartitions());
            assertEquals(streamsOneActiveRepartition, streamsTwoMetadata.standbyTopicPartitions());
        }
    }

    private static void verifyHostMetadata(
            final StreamsMetadata metadata,
            final int expectedPort,
            final int expectedStandbyCount,
            final int expectedStandbyStoreCount,
            final boolean usingStandbyReplicas
    ) {
        final Set<TopicPartition> activeTopicPartitions = metadata.topicPartitions();
        final Set<TopicPartition> standbyTopicPartitions = metadata.standbyTopicPartitions();
        final Set<String> storeNames = metadata.stateStoreNames();
        final Set<String> standbyStoreNames = metadata.standbyStateStoreNames();

        assertEquals(expectedPort, metadata.hostInfo().port());
        assertEquals(4, activeTopicPartitions.size());
        assertEquals(expectedStandbyCount, standbyTopicPartitions.size());
        assertEquals(1, storeNames.size());
        assertEquals(expectedStandbyStoreCount, standbyStoreNames.size());
        assertEquals(EXPECTED_STORE_NAME, storeNames.iterator().next());
        if (usingStandbyReplicas) {
            assertEquals(EXPECTED_STORE_NAME, standbyStoreNames.iterator().next());
        }

        final long repartitionCount = activeTopicPartitions.stream().filter(tp -> tp.topic().contains("-repartition")).count();
        final long sourceCount = activeTopicPartitions.stream().filter(tp -> tp.topic().contains("-input-two")).count();
        assertEquals(2, repartitionCount);
        assertEquals(2, sourceCount);
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
