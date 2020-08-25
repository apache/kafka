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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@Category({IntegrationTest.class})
public class HandlingSourceTopicDeletionTest {

    private static final int NUM_BROKERS = 1;
    private static final int NUM_THREADS = 2;
    private static final long TIMEOUT = 60000;


    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(NUM_BROKERS);

    // topic names
    private static final String INPUT_TOPIC = "inputTopic";
    private static final String OUTPUT_TOPIC = "outputTopic";

    @Rule
    public TestName testName = new TestName();

    private final StreamsBuilder builder = new StreamsBuilder();
    private Properties streamsConfiguration;
    private KafkaStreams kafkaStreams;

    @Before
    public void before() throws InterruptedException {
        CLUSTER.createTopics(INPUT_TOPIC, OUTPUT_TOPIC);

        final String safeTestName = safeUniqueTestName(getClass(), testName);
        final String appId = "app-" + safeTestName;

        streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        streamsConfiguration.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, NUM_THREADS);
        streamsConfiguration.put(StreamsConfig.METADATA_MAX_AGE_CONFIG, 2000);
    }

    @After
    public void after() throws InterruptedException {
        CLUSTER.deleteTopics(INPUT_TOPIC, OUTPUT_TOPIC);
    }

    @Test
    public void shouldShutdownAfterSourceTopicDeleted() throws InterruptedException, ExecutionException {
        builder.stream(INPUT_TOPIC, Consumed.with(Serdes.Integer(), Serdes.String()))
            .to(OUTPUT_TOPIC, Produced.with(Serdes.Integer(), Serdes.String()));
        startApplication();
    }

    private void startApplication() throws InterruptedException {
        final Topology topology = builder.build();
        kafkaStreams = new KafkaStreams(topology, streamsConfiguration);

        final AtomicBoolean calledUncaughtExceptionHandler = new AtomicBoolean(false);
        kafkaStreams.setUncaughtExceptionHandler((thread, exception) -> calledUncaughtExceptionHandler.set(true));
        kafkaStreams.start();
        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.RUNNING,
            TIMEOUT,
            () -> "Kafka Streams application did not reach state RUNNING"
        );

        CLUSTER.deleteTopicAndWait(INPUT_TOPIC);

        TestUtils.waitForCondition(
            () -> kafkaStreams.state() == State.ERROR,
            TIMEOUT,
            () -> "Kafka Streams application did not reach state ERROR"
        );

        assertThat(calledUncaughtExceptionHandler.get(), is(true));
    }
}
