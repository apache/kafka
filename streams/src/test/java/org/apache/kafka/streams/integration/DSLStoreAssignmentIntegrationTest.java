/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.integration;

import kafka.utils.MockTime;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.test.TestUtils;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.Collections;
import java.util.Properties;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Tests if state stores get assigned to the correct processors within DSL.
 * For example, a KTable might not be materialized and thus a parent state store must be assigned correctly.
 */
public class DSLStoreAssignmentIntegrationTest {
    private static final int NUM_BROKERS = 1;
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);
    private final MockTime mockTime = CLUSTER.time;

    private static final String APP_ID = "dsl-store-assignment-integration-test";
    private static final String INPUT_TOPIC_1 = "inputTopic1";
    private static final String INPUT_TOPIC_2 = "inputTopic2";
    private static final String INPUT_TOPIC_3 = "inputTopic3";
    private static final String OUTPUT_TOPIC = "outputTopic";

    @BeforeClass
    public static void startKafkaCluster() throws Exception {
        CLUSTER.createTopic(INPUT_TOPIC_1);
        CLUSTER.createTopic(INPUT_TOPIC_2);
        CLUSTER.createTopic(INPUT_TOPIC_3);
        CLUSTER.createTopic(OUTPUT_TOPIC);
    }

    @Test
    public void testDSLStoreAssignment() throws Exception {
        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, APP_ID);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, CLUSTER.zKConnectString());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath());
        streamsConfiguration.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        IntegrationTestUtils.purgeLocalStreamsState(streamsConfiguration);

        final Properties producerConfig = TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class);
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_1, Collections.singleton(new KeyValue<>("A", "1")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_2, Collections.singleton(new KeyValue<>("A", "2")), producerConfig, mockTime.milliseconds());
        mockTime.sleep(10);
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(INPUT_TOPIC_3, Collections.singleton(new KeyValue<>("A", "3")), producerConfig, mockTime.milliseconds());


        final KStreamBuilder builder = new KStreamBuilder();

        final KTable<String, String> table1 = builder.table(INPUT_TOPIC_1, "store1");
        final KTable<String, String> table2 = builder.table(INPUT_TOPIC_2, "store2");
        final KStream<String, String> stream1 = builder.stream(INPUT_TOPIC_3);

        final KTable<String, String> table3 = table1.join(table2, new ValueJoiner<String, String, String>() {
            @Override
            public String apply(String value1, String value2) {
                return value1 + "-" + value2;
            }
        });
        stream1
            .leftJoin(table3, new ValueJoiner<String, String, String>() {
                @Override
                public String apply(String value1, String value2) {
                    return value1 + "-" + value2;
                }
            })
            .to(OUTPUT_TOPIC);


        KafkaStreams streams = new KafkaStreams(builder, streamsConfiguration);
        streams.start();

        final Properties resultTopicConsumerConfig = TestUtils.consumerConfig(
            CLUSTER.bootstrapServers(),
            OUTPUT_TOPIC,
            StringDeserializer.class,
            StringDeserializer.class);

        final KeyValue result = IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(
            resultTopicConsumerConfig,
            OUTPUT_TOPIC,
            1,
            60000
        ).get(0);

        streams.close();

        assertThat((String) result.key, equalTo("A"));
        assertThat((String) result.value, equalTo("3-1-2"));
    }

}
