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

import java.time.Duration;
import kafka.utils.MockTime;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.StreamsTestUtils;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.DEFAULT_TIMEOUT;
import static org.junit.Assert.assertEquals;

@Category({IntegrationTest.class})
public class RepartitionWithMergeOptimizingIntegrationTest {

    // Allow for twice the default timeout as the records must make more than one roundtrip given the repartitioning
    private static final long TIMEOUT = DEFAULT_TIMEOUT * 2;

    private static final int NUM_BROKERS = 1;
    private static final String INPUT_A_TOPIC = "inputA";
    private static final String INPUT_B_TOPIC = "inputB";
    private static final String COUNT_TOPIC = "outputTopic_0";
    private static final String COUNT_STRING_TOPIC = "outputTopic_1";

    private static final int ONE_REPARTITION_TOPIC = 1;
    private static final int TWO_REPARTITION_TOPICS = 2;

    private final Pattern repartitionTopicPattern = Pattern.compile("Sink: .*-repartition");

    private Properties streamsConfiguration;

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;

    @Before
    public void setUp() throws Exception {
        final Properties props = new Properties();
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 1024 * 10);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 5000);

        streamsConfiguration = StreamsTestUtils.getStreamsConfig(
            "maybe-optimized-with-merge-test-app",
            CLUSTER.bootstrapServers(),
            Serdes.String().getClass().getName(),
            Serdes.String().getClass().getName(),
            props);

        CLUSTER.createTopics(COUNT_TOPIC,
                             COUNT_STRING_TOPIC,
                             INPUT_A_TOPIC,
                             INPUT_B_TOPIC);

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);
    }

    @After
    public void tearDown() throws Exception {
        CLUSTER.deleteAllTopicsAndWait(30_000L);
    }

    @Test
    public void shouldSendCorrectRecords_OPTIMIZED() throws Exception {
        runIntegrationTest(StreamsConfig.OPTIMIZE,
                           ONE_REPARTITION_TOPIC);
    }

    @Test
    public void shouldSendCorrectResults_NO_OPTIMIZATION() throws Exception {
        runIntegrationTest(StreamsConfig.NO_OPTIMIZATION,
                           TWO_REPARTITION_TOPICS);
    }


    private void runIntegrationTest(final String optimizationConfig,
                                    final int expectedNumberRepartitionTopics) throws Exception {

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, String> sourceAStream =
            builder.stream(INPUT_A_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("sourceAStream"));

        final KStream<String, String> sourceBStream =
            builder.stream(INPUT_B_TOPIC, Consumed.with(Serdes.String(), Serdes.String()).withName("sourceBStream"));

        final KStream<String, String> mappedAStream =
            sourceAStream.map((k, v) -> KeyValue.pair(v.split(":")[0], v), Named.as("mappedAStream"));
        final KStream<String, String> mappedBStream =
            sourceBStream.map((k, v) -> KeyValue.pair(v.split(":")[0], v), Named.as("mappedBStream"));

        final KStream<String, String> mergedStream = mappedAStream.merge(mappedBStream, Named.as("mergedStream"));

        mergedStream
            .groupByKey(Grouped.as("long-groupByKey"))
            .count(Named.as("long-count"), Materialized.as(Stores.inMemoryKeyValueStore("long-store")))
            .toStream(Named.as("long-toStream"))
            .to(COUNT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()).withName("long-to"));

        mergedStream
            .groupByKey(Grouped.as("string-groupByKey"))
            .count(Named.as("string-count"), Materialized.as(Stores.inMemoryKeyValueStore("string-store")))
            .toStream(Named.as("string-toStream"))
            .mapValues(v -> v.toString(), Named.as("string-mapValues"))
            .to(COUNT_STRING_TOPIC, Produced.with(Serdes.String(), Serdes.String()).withName("string-to"));

        streamsConfiguration.setProperty(StreamsConfig.TOPOLOGY_OPTIMIZATION, optimizationConfig);

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);

        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_A_TOPIC, getKeyValues(), producerConfig, mockTime);
        IntegrationTestUtils.produceKeyValuesSynchronously(INPUT_B_TOPIC, getKeyValues(), producerConfig, mockTime);

        final Properties consumerConfig1 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, LongDeserializer.class);
        final Properties consumerConfig2 = TestUtils.consumerConfig(CLUSTER.bootstrapServers(), StringDeserializer.class, StringDeserializer.class);

        final Topology topology = builder.build(streamsConfiguration);
        final String topologyString = topology.describe().toString();
        System.out.println(topologyString);

        if (optimizationConfig.equals(StreamsConfig.OPTIMIZE)) {
            assertEquals(EXPECTED_OPTIMIZED_TOPOLOGY, topologyString);
        } else {
            assertEquals(EXPECTED_UNOPTIMIZED_TOPOLOGY, topologyString);
        }

        /*
           confirming number of expected repartition topics here
         */
        assertEquals(expectedNumberRepartitionTopics, getCountOfRepartitionTopicsFound(topologyString));

        final KafkaStreams streams = new KafkaStreams(topology, streamsConfiguration);
        streams.start();

        final List<KeyValue<String, Long>> expectedCountKeyValues = Arrays.asList(KeyValue.pair("A", 6L), KeyValue.pair("B", 6L), KeyValue.pair("C", 6L));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig1, COUNT_TOPIC, expectedCountKeyValues, TIMEOUT);

        final List<KeyValue<String, String>> expectedStringCountKeyValues = Arrays.asList(KeyValue.pair("A", "6"), KeyValue.pair("B", "6"), KeyValue.pair("C", "6"));
        IntegrationTestUtils.waitUntilFinalKeyValueRecordsReceived(consumerConfig2, COUNT_STRING_TOPIC, expectedStringCountKeyValues, TIMEOUT);

        streams.close(Duration.ofSeconds(5));
    }


    private int getCountOfRepartitionTopicsFound(final String topologyString) {
        final Matcher matcher = repartitionTopicPattern.matcher(topologyString);
        final List<String> repartitionTopicsFound = new ArrayList<>();
        while (matcher.find()) {
            repartitionTopicsFound.add(matcher.group());
        }
        return repartitionTopicsFound.size();
    }


    private List<KeyValue<String, String>> getKeyValues() {
        final List<KeyValue<String, String>> keyValueList = new ArrayList<>();
        final String[] keys = new String[]{"X", "Y", "Z"};
        final String[] values = new String[]{"A:foo", "B:foo", "C:foo"};
        for (final String key : keys) {
            for (final String value : values) {
                keyValueList.add(KeyValue.pair(key, value));
            }
        }
        return keyValueList;
    }


    private static final String EXPECTED_OPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                              + "   Sub-topology: 0\n"
                                                              + "    Source: KSTREAM-SOURCE-0000000020 (topics: [long-groupByKey-repartition])\n"
                                                              + "      --> long-count, string-count\n"
                                                              + "    Processor: string-count (stores: [string-store])\n"
                                                              + "      --> string-toStream\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000020\n"
                                                              + "    Processor: long-count (stores: [long-store])\n"
                                                              + "      --> long-toStream\n"
                                                              + "      <-- KSTREAM-SOURCE-0000000020\n"
                                                              + "    Processor: string-toStream (stores: [])\n"
                                                              + "      --> string-mapValues\n"
                                                              + "      <-- string-count\n"
                                                              + "    Processor: long-toStream (stores: [])\n"
                                                              + "      --> long-to\n"
                                                              + "      <-- long-count\n"
                                                              + "    Processor: string-mapValues (stores: [])\n"
                                                              + "      --> string-to\n"
                                                              + "      <-- string-toStream\n"
                                                              + "    Sink: long-to (topic: outputTopic_0)\n"
                                                              + "      <-- long-toStream\n"
                                                              + "    Sink: string-to (topic: outputTopic_1)\n"
                                                              + "      <-- string-mapValues\n"
                                                              + "\n"
                                                              + "  Sub-topology: 1\n"
                                                              + "    Source: sourceAStream (topics: [inputA])\n"
                                                              + "      --> mappedAStream\n"
                                                              + "    Source: sourceBStream (topics: [inputB])\n"
                                                              + "      --> mappedBStream\n"
                                                              + "    Processor: mappedAStream (stores: [])\n"
                                                              + "      --> mergedStream\n"
                                                              + "      <-- sourceAStream\n"
                                                              + "    Processor: mappedBStream (stores: [])\n"
                                                              + "      --> mergedStream\n"
                                                              + "      <-- sourceBStream\n"
                                                              + "    Processor: mergedStream (stores: [])\n"
                                                              + "      --> KSTREAM-FILTER-0000000019\n"
                                                              + "      <-- mappedAStream, mappedBStream\n"
                                                              + "    Processor: KSTREAM-FILTER-0000000019 (stores: [])\n"
                                                              + "      --> KSTREAM-SINK-0000000018\n"
                                                              + "      <-- mergedStream\n"
                                                              + "    Sink: KSTREAM-SINK-0000000018 (topic: long-groupByKey-repartition)\n"
                                                              + "      <-- KSTREAM-FILTER-0000000019\n\n";


    private static final String EXPECTED_UNOPTIMIZED_TOPOLOGY = "Topologies:\n"
                                                                + "   Sub-topology: 0\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000008 (topics: [long-groupByKey-repartition])\n"
                                                                + "      --> long-count\n"
                                                                + "    Processor: long-count (stores: [long-store])\n"
                                                                + "      --> long-toStream\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000008\n"
                                                                + "    Processor: long-toStream (stores: [])\n"
                                                                + "      --> long-to\n"
                                                                + "      <-- long-count\n"
                                                                + "    Sink: long-to (topic: outputTopic_0)\n"
                                                                + "      <-- long-toStream\n"
                                                                + "\n"
                                                                + "  Sub-topology: 1\n"
                                                                + "    Source: KSTREAM-SOURCE-0000000014 (topics: [string-groupByKey-repartition])\n"
                                                                + "      --> string-count\n"
                                                                + "    Processor: string-count (stores: [string-store])\n"
                                                                + "      --> string-toStream\n"
                                                                + "      <-- KSTREAM-SOURCE-0000000014\n"
                                                                + "    Processor: string-toStream (stores: [])\n"
                                                                + "      --> string-mapValues\n"
                                                                + "      <-- string-count\n"
                                                                + "    Processor: string-mapValues (stores: [])\n"
                                                                + "      --> string-to\n"
                                                                + "      <-- string-toStream\n"
                                                                + "    Sink: string-to (topic: outputTopic_1)\n"
                                                                + "      <-- string-mapValues\n"
                                                                + "\n"
                                                                + "  Sub-topology: 2\n"
                                                                + "    Source: sourceAStream (topics: [inputA])\n"
                                                                + "      --> mappedAStream\n"
                                                                + "    Source: sourceBStream (topics: [inputB])\n"
                                                                + "      --> mappedBStream\n"
                                                                + "    Processor: mappedAStream (stores: [])\n"
                                                                + "      --> mergedStream\n"
                                                                + "      <-- sourceAStream\n"
                                                                + "    Processor: mappedBStream (stores: [])\n"
                                                                + "      --> mergedStream\n"
                                                                + "      <-- sourceBStream\n"
                                                                + "    Processor: mergedStream (stores: [])\n"
                                                                + "      --> KSTREAM-FILTER-0000000007, KSTREAM-FILTER-0000000013\n"
                                                                + "      <-- mappedAStream, mappedBStream\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000007 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000006\n"
                                                                + "      <-- mergedStream\n"
                                                                + "    Processor: KSTREAM-FILTER-0000000013 (stores: [])\n"
                                                                + "      --> KSTREAM-SINK-0000000012\n"
                                                                + "      <-- mergedStream\n"
                                                                + "    Sink: KSTREAM-SINK-0000000006 (topic: long-groupByKey-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000007\n"
                                                                + "    Sink: KSTREAM-SINK-0000000012 (topic: string-groupByKey-repartition)\n"
                                                                + "      <-- KSTREAM-FILTER-0000000013\n\n";


}
