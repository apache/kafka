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

import static java.time.Duration.ofMinutes;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
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
import org.apache.kafka.streams.kstream.JoinWindows;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category({IntegrationTest.class})
public class SelfJoinUpgradeIntegrationTest {
    public static final String INPUT_TOPIC = "selfjoin-input";
    public static final String OUTPUT_TOPIC = "selfjoin-output";
    private String inputTopic;
    private String outputTopic;

    private KafkaStreams kafkaStreams;

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Rule
    public TestName testName = new TestName();

    @Before
    public void createTopics() throws Exception {
        inputTopic = INPUT_TOPIC + safeUniqueTestName(getClass(), testName);
        outputTopic = OUTPUT_TOPIC + safeUniqueTestName(getClass(), testName);
        CLUSTER.createTopic(inputTopic);
        CLUSTER.createTopic(outputTopic);
    }

    private Properties props() {
        final Properties streamsConfiguration = new Properties();
        final String safeTestName = safeUniqueTestName(getClass(), testName);
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-" + safeTestName);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                                 Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                                 Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return streamsConfiguration;
    }

    @After
    public void shutdown() {
        if (kafkaStreams != null) {
            kafkaStreams.close(Duration.ofSeconds(30L));
            kafkaStreams.cleanUp();
        }
    }


    @Test
    @SuppressWarnings("unchecked")
    public void shouldUpgradeWithTopologyOptimizationOff() throws Exception {

        final StreamsBuilder streamsBuilderOld = new StreamsBuilder();
        final KStream<String, String> leftOld = streamsBuilderOld.stream(
            inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;
        final KStream<String, String> joinedOld = leftOld.join(
            leftOld,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMinutes(100))
        );
        joinedOld.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


        final Properties props = props();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.NO_OPTIMIZATION);
        kafkaStreams = new KafkaStreams(streamsBuilderOld.build(), props);
        kafkaStreams.start();

        final long currentTime = CLUSTER.time.milliseconds();
        processKeyValueAndVerifyCount("1", "A", currentTime + 42L, asList(
            new KeyValueTimestamp<String, String>("1", "AA", currentTime + 42L)));

        processKeyValueAndVerifyCount("1", "B", currentTime + 43L, asList(
            new KeyValueTimestamp("1", "BA", currentTime + 43L),
            new KeyValueTimestamp("1", "AB", currentTime + 43L),
            new KeyValueTimestamp("1", "BB", currentTime + 43L)));


        kafkaStreams.close();
        kafkaStreams = null;

        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        kafkaStreams = new KafkaStreams(streamsBuilderOld.build(), props);
        kafkaStreams.start();

        final long currentTimeNew = CLUSTER.time.milliseconds();

        processKeyValueAndVerifyCount("1", "C", currentTimeNew + 44L, asList(
            new KeyValueTimestamp("1", "CA", currentTimeNew + 44L),
            new KeyValueTimestamp("1", "CB", currentTimeNew + 44L),
            new KeyValueTimestamp("1", "AC", currentTimeNew + 44L),
            new KeyValueTimestamp("1", "BC", currentTimeNew + 44L),
            new KeyValueTimestamp("1", "CC", currentTimeNew + 44L)));


        kafkaStreams.close();
    }

    @Test
    @SuppressWarnings("unchecked")
    public void shouldRestartWithTopologyOptimizationOn() throws Exception {

        final StreamsBuilder streamsBuilderOld = new StreamsBuilder();
        final KStream<String, String> leftOld = streamsBuilderOld.stream(
            inputTopic, Consumed.with(Serdes.String(), Serdes.String()));
        final ValueJoiner<String, String, String> valueJoiner = (v, v2) -> v + v2;
        final KStream<String, String> joinedOld = leftOld.join(
            leftOld,
            valueJoiner,
            JoinWindows.ofTimeDifferenceWithNoGrace(ofMinutes(100))
        );
        joinedOld.to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));


        final Properties props = props();
        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        kafkaStreams = new KafkaStreams(streamsBuilderOld.build(), props);
        kafkaStreams.start();

        final long currentTime = CLUSTER.time.milliseconds();
        processKeyValueAndVerifyCount("1", "A", currentTime + 42L, asList(
            new KeyValueTimestamp("1", "AA", currentTime + 42L)));

        processKeyValueAndVerifyCount("1", "B", currentTime + 43L, asList(
            new KeyValueTimestamp("1", "BA", currentTime + 43L),
            new KeyValueTimestamp("1", "AB", currentTime + 43L),
            new KeyValueTimestamp("1", "BB", currentTime + 43L)));


        kafkaStreams.close();
        kafkaStreams = null;

        props.put(StreamsConfig.TOPOLOGY_OPTIMIZATION_CONFIG, StreamsConfig.OPTIMIZE);
        kafkaStreams = new KafkaStreams(streamsBuilderOld.build(), props);
        kafkaStreams.start();

        final long currentTimeNew = CLUSTER.time.milliseconds();

        processKeyValueAndVerifyCount("1", "C", currentTimeNew + 44L, asList(
            new KeyValueTimestamp("1", "CA", currentTimeNew + 44L),
            new KeyValueTimestamp("1", "CB", currentTimeNew + 44L),
            new KeyValueTimestamp("1", "AC", currentTimeNew + 44L),
            new KeyValueTimestamp("1", "BC", currentTimeNew + 44L),
            new KeyValueTimestamp("1", "CC", currentTimeNew + 44L)));

        kafkaStreams.close();
    }


    private <K, V> boolean processKeyValueAndVerifyCount(
        final K key,
        final V value,
        final long timestamp,
        final List<KeyValueTimestamp<K, V>> expected)
        throws Exception {

        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            singletonList(KeyValue.pair(key, value)),
            TestUtils.producerConfig(CLUSTER.bootstrapServers(),
                                     StringSerializer.class,
                                     StringSerializer.class),
            timestamp);


        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "group-" + safeTestName);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(StreamsConfig.WINDOW_SIZE_MS_CONFIG, 500L);


        final List<KeyValueTimestamp<K, V>> actual =
            IntegrationTestUtils.waitUntilMinKeyValueWithTimestampRecordsReceived(
            consumerProperties,
            outputTopic,
            expected.size(),
            60 * 1000);

        assertThat(actual, is(expected));

        return actual.equals(expected);
    }


}
