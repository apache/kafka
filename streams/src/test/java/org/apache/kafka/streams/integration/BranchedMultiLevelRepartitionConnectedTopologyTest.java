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

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

/**
 * Test out a topology with 3 level of sub-topology as:
 *           0
 *         /   \
 *        1    3
 *         \  /
 *          2
 * where each pair of the sub topology is connected by repartition topic.
 * The purpose of this test is to verify the robustness of the stream partition assignor algorithm,
 * especially whether it could build the repartition topic counts (step zero) with a complex topology.
 * The traversal path 0 -> 1 -> 2 -> 3 hits the case where sub-topology 2 will be initialized while its
 * parent 3 hasn't been initialized yet.
 */
@Category({IntegrationTest.class})
public class BranchedMultiLevelRepartitionConnectedTopologyTest {

    private static final Logger log = LoggerFactory.getLogger(BranchedMultiLevelRepartitionConnectedTopologyTest.class);

    private static String inputStream;

    private KafkaStreams kafkaStreams;

    private Properties streamsConfiguration;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);
    private final MockTime mockTime = CLUSTER.time;

    @Before
    public void setUp() throws Exception {
        final Properties props = new Properties();
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION, StreamsConfig.NO_OPTIMIZATION);
        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
            "branched-repartition-topic-test",
            CLUSTER.bootstrapServers(),
            Serdes.ByteArray().getClass().getName(),
            Serdes.ByteArray().getClass().getName(),
            props);

        inputStream = "input-stream";
        CLUSTER.createTopic(inputStream, 3, 1);

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testTopologyBuild() throws InterruptedException, ExecutionException {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<byte[], byte[]> input = builder.stream(inputStream);

        final KStream<byte[], byte[]>[] branches = input
            .flatMapValues(value -> Collections.singletonList(new byte[0]))
            .branch((k, v) -> true, (k, v) -> false);

        final KTable<byte[], byte[]> b1 = branches[0]
            .map(KeyValue::new)
            .groupByKey()
            .reduce((k, v) -> v, Materialized.as("odd_store"))
            .toStream()
            .peek((k, v) -> { })
            .map(KeyValue::new)
            .groupByKey()
            .reduce((k, v) -> v, Materialized.as("odd_store_2"));

        final KTable<byte[], byte[]> b2 = branches[1]
            .map(KeyValue::new)
            .groupByKey()
            .reduce((k, v) -> v, Materialized.as("even_store"))
            .toStream()
            .peek((k, v) -> { })
            .map(KeyValue::new)
            .groupByKey()
            .reduce((k, v) -> v, Materialized.as("even_store_2"));

        b1.join(b2, (v1, v2) -> v1, Materialized.as("joined_store"))
            .toStream();

        final Topology topology = builder.build(streamsConfiguration);
        log.info("Built topology: {}", topology.describe());

        final Properties producerConfig = TestUtils.producerConfig(
            CLUSTER.bootstrapServers(), ByteArraySerializer.class, ByteArraySerializer.class);

        final List<KeyValue<byte[], byte[]>> initialKeyValues = Collections.singletonList(
            KeyValue.pair(new byte[1], new byte[1]));

        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputStream, initialKeyValues, producerConfig, mockTime);

        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);

        kafkaStreams.cleanUp();
        kafkaStreams.start();

        TestUtils.waitForCondition(() -> kafkaStreams.state() == KafkaStreams.State.RUNNING,
                                   "Failed to observe stream transits to RUNNING");

        kafkaStreams.close();
    }
}
