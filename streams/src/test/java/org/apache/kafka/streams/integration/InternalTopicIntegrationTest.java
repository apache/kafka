/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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


import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KeyValueMapper;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import org.apache.kafka.streams.integration.utils.EmbeddedSingleNodeKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import kafka.admin.AdminUtils;
import kafka.log.LogConfig;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.Map;

/**
 * Tests related to internal topics in streams
 */
public class InternalTopicIntegrationTest {
    @ClassRule
    public static EmbeddedSingleNodeKafkaCluster cluster = new EmbeddedSingleNodeKafkaCluster();
    private static final String DEFAULT_INPUT_TOPIC = "inputTopic";
    private static final String DEFAULT_OUTPUT_TOPIC = "outputTopic";
    private static final int DEFAULT_ZK_SESSION_TIMEOUT_MS = 10 * 1000;
    private static final int DEFAULT_ZK_CONNECTION_TIMEOUT_MS = 8 * 1000;

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        cluster.createTopic(DEFAULT_INPUT_TOPIC);
        cluster.createTopic(DEFAULT_OUTPUT_TOPIC);
    }

    /**
     * Validates that any state changelog topics are compacted
     * @return true if topics have a valid config, false otherwise
     */
    private boolean isUsingCompactionForStateChangelogTopics() {
        boolean valid = true;

        // Note: You must initialize the ZkClient with ZKStringSerializer.  If you don't, then
        // createTopic() will only seem to work (it will return without error).  The topic will exist in
        // only ZooKeeper and will be returned when listing topics, but Kafka itself does not create the
        // topic.
        ZkClient zkClient = new ZkClient(
            cluster.zKConnectString(),
            DEFAULT_ZK_SESSION_TIMEOUT_MS,
            DEFAULT_ZK_CONNECTION_TIMEOUT_MS,
            ZKStringSerializer$.MODULE$);
        boolean isSecure = false;
        ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(cluster.zKConnectString()), isSecure);

        Map<String, Properties> topicConfigs = AdminUtils.fetchAllTopicConfigs(zkUtils);
        Iterator it = topicConfigs.iterator();
        while (it.hasNext()) {
            Tuple2<String, Properties> topicConfig = (Tuple2<String, Properties>) it.next();
            String topic = topicConfig._1;
            Properties prop = topicConfig._2;

            // state changelogs should be compacted
            if (topic.endsWith(ProcessorStateManager.STATE_CHANGELOG_TOPIC_SUFFIX)) {
                if (!prop.containsKey(LogConfig.CleanupPolicyProp()) ||
                    !prop.getProperty(LogConfig.CleanupPolicyProp()).equals(LogConfig.Compact())) {
                    valid = false;
                    break;
                }
            }
        }
        zkClient.close();
        return valid;
    }

    @Test
    public void shouldCompactTopicsForStateChangelogs() throws Exception {
        List<String> inputValues = Arrays.asList("hello", "world", "world", "hello world");

        //
        // Step 1: Configure and start a simple word count topology
        //
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();

        Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "compact-topics-integration-test");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, cluster.zKConnectString());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> textLines = builder.stream(DEFAULT_INPUT_TOPIC);

        KStream<String, Long> wordCounts = textLines
            .flatMapValues(new ValueMapper<String, Iterable<String>>() {
                @Override
                public Iterable<String> apply(String value) {
                    return Arrays.asList(value.toLowerCase(Locale.getDefault()).split("\\W+"));
                }
            }).map(new KeyValueMapper<String, String, KeyValue<String, String>>() {
                @Override
                public KeyValue<String, String> apply(String key, String value) {
                    return new KeyValue<String, String>(value, value);
                }
            }).countByKey("Counts").toStream();

        wordCounts.to(stringSerde, longSerde, DEFAULT_OUTPUT_TOPIC);

        // Remove any state from previous test runs
        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        //
        // Step 2: Produce some input data to the input topic.
        //
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers());
        producerConfig.put(ProducerConfig.ACKS_CONFIG, "all");
        producerConfig.put(ProducerConfig.RETRIES_CONFIG, 0);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        IntegrationTestUtils.produceValuesSynchronously(DEFAULT_INPUT_TOPIC, inputValues, producerConfig);

        //
        // Step 3: Verify the state changelog topics are compact
        //
        streams.close();
        assertEquals(isUsingCompactionForStateChangelogTopics(), true);
    }
}
