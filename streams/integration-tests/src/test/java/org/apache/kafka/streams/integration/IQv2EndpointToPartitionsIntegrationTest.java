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
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.TestUtils;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger(IQv2EndpointToPartitionsIntegrationTest.class);


    @BeforeAll
    public static void startCluster() throws IOException {
        final Properties properties = new Properties();
        properties.put(GroupCoordinatorConfig.GROUP_COORDINATOR_REBALANCE_PROTOCOLS_CONFIG, "classic,consumer,streams");
        cluster = new EmbeddedKafkaCluster(NUM_BROKERS, properties);
        cluster.start();
    }

    @BeforeEach
    public void setUp(final TestInfo testInfo) throws InterruptedException {
        appId = safeUniqueTestName(testInfo);
        inputTopicTwoPartitions = appId + "-input-two";
        outputTopicTwoPartitions = appId + "-output-two";
        cluster.createTopic(inputTopicTwoPartitions, 2, 1);
        cluster.createTopic(outputTopicTwoPartitions, 2, 1);
    }

    @AfterAll
    public static void closeCluster() {
        cluster.stop();
    }

    @AfterEach
    public void tearDown() throws Exception {
        IntegrationTestUtils.purgeLocalStreamsState(streamsApplicationProperties);
        if (!streamsSecondApplicationProperties.isEmpty()) {
            IntegrationTestUtils.purgeLocalStreamsState(streamsSecondApplicationProperties);
        }
    }


    @ParameterizedTest(name = "{1}" )
    @MethodSource("groupProtocolParameters")
    public void shouldGetCorrectHostPartitionInformation(final String groupProtocolConfig, final String testName) throws Exception {

        final Properties streamOneProperties = new Properties();
        streamOneProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks1");
        streamOneProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks1");
        streamOneProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:2020");
        streamOneProperties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocolConfig);
        streamsApplicationProperties = props(streamOneProperties);

        final Properties streamTwoProperties = new Properties();
        streamTwoProperties.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory(appId).getPath() + "-ks2");
        streamTwoProperties.put(StreamsConfig.CLIENT_ID_CONFIG, appId + "-ks2");
        streamTwoProperties.put(StreamsConfig.APPLICATION_SERVER_CONFIG, "localhost:3030");
        streamTwoProperties.put(StreamsConfig.GROUP_PROTOCOL_CONFIG, groupProtocolConfig);
        streamsSecondApplicationProperties = props(streamTwoProperties);

        final Topology topology = complexTopology();
        try (final KafkaStreams streamsOne = new KafkaStreams(topology, streamsApplicationProperties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(streamsOne);
            List<StreamsMetadata> streamsMetadataAllClients = new ArrayList<>(streamsOne.metadataForAllStreamsClients());
            assertEquals(1, streamsMetadataAllClients.size());
            StreamsMetadata streamsOneMetadataOne = streamsMetadataAllClients.get(0);
            Set<TopicPartition> topicPartitions = streamsOneMetadataOne.topicPartitions();
            assertEquals(2020, streamsOneMetadataOne.hostInfo().port());
            assertEquals(4, topicPartitions.size());

            long repartitionTopicTaskCount = topicPartitions.stream().filter(tp -> tp.topic().contains("-repartition")).count();
            long sourceTopicTaskCount = topicPartitions.stream().filter(tp -> tp.topic().contains("-input-two")).count();
            assertEquals(2, repartitionTopicTaskCount);
            assertEquals(2, sourceTopicTaskCount);

            
            LOG.info("StreamsMetadata topicPartitions for streamsOne: {}", streamsOneMetadataOne.topicPartitions());

            try (final KafkaStreams streamsTwo = new KafkaStreams(topology, streamsSecondApplicationProperties)) {
                streamsTwo.start();
                waitForCondition(() -> KafkaStreams.State.RUNNING == streamsTwo.state() && KafkaStreams.State.RUNNING == streamsOne.state(),
                        IntegrationTestUtils.DEFAULT_TIMEOUT,
                        () -> "Kafka Streams one or two never transitioned to a RUNNING state.");

                streamsMetadataAllClients = new ArrayList<>(streamsTwo.metadataForAllStreamsClients());
                assertEquals(2, streamsMetadataAllClients.size());
                streamsOneMetadataOne = streamsMetadataAllClients.get(0);

                Set<TopicPartition> streamsOneTopicPartitions = streamsOneMetadataOne.topicPartitions();
                LOG.info("StreamsMetadata topicPartitions for streamsOne: {}", streamsOneTopicPartitions);
                assertEquals(2020, streamsOneMetadataOne.hostInfo().port());
                assertEquals(2, streamsOneTopicPartitions.size());

                long streamsOneRepartitionTopicTaskCount = streamsOneTopicPartitions.stream().filter(tp -> tp.topic().contains("-repartition")).count();
                long streamsOneSourceTopicTaskCount = streamsOneTopicPartitions.stream().filter(tp -> tp.topic().contains("-input-two")).count();
                assertEquals(1, streamsOneRepartitionTopicTaskCount);
                assertEquals(1, streamsOneSourceTopicTaskCount);
                
                StreamsMetadata streamsOneMetadataTwo = streamsMetadataAllClients.get(1);
                Set<TopicPartition> streamsTwoTopicPartitions = streamsOneMetadataTwo.topicPartitions();
                LOG.info("StreamsMetadata topicPartitions for streamsTwo: {}", streamsTwoTopicPartitions);
                assertEquals(3030, streamsOneMetadataTwo.hostInfo().port());
                assertEquals(2, streamsTwoTopicPartitions.size());

                long streamsTwoRepartitionTopicTaskCount = streamsTwoTopicPartitions.stream().filter(tp -> tp.topic().contains("-repartition")).count();
                long streamsTwoSourceTopicTaskCount = streamsTwoTopicPartitions.stream().filter(tp -> tp.topic().contains("-input-two")).count();
                assertEquals(1, streamsTwoRepartitionTopicTaskCount);
                assertEquals(1, streamsTwoSourceTopicTaskCount);

            }
        }
    }

    private static Stream<Arguments> groupProtocolParameters() {
        return Stream.of(Arguments.of("streams", "Using STREAMS group protocol"),
                Arguments.of("classic", "Using CLASSIC group protocol"));
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
                .groupBy((key, value) -> value)
                .count()
                .toStream().to(outputTopicTwoPartitions, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

}
