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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Named;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_APPLICATION;
import static org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.SHUTDOWN_CLIENT;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;


@Category(IntegrationTest.class)
public class StreamsHandlerIntegrationTest {
    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @Rule
    public TestName testName = new TestName();

    String inputTopic;
    StreamsBuilder builder;
    Properties properties;
    List<String> processorValueCollector;
    String idempotentTopic = "idempotentTopic";
    String appId = "";

    @Before
    public void setup() {
        final String testId = safeUniqueTestName(getClass(), testName);
        appId = "appId_" + testId;
        inputTopic = "input" + testId;
        cleanStateBeforeTest(CLUSTER, idempotentTopic, inputTopic);


        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic);

        builder  = new StreamsBuilder();

        processorValueCollector = new ArrayList<>();

        final KStream<String, String> stream = builder.stream(inputTopic);
        stream.process(() -> new ShutdownProcessor(processorValueCollector), Named.as("process"));

        properties  = mkObjectProperties(
                mkMap(
                        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                        mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                        mkEntry(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "5"),
                        mkEntry(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "6"),
                        mkEntry(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, "7"),
                        mkEntry(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, "480000"),
                        mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2),
                        mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                        mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class)
                )
        );
    }

    @After
    public void teardown() throws IOException {
        purgeLocalStreamsState(properties);
    }

    @Test
    public void shouldShutdownThreadUsingOldHandler() throws Exception {
        //
        //
        // Also note that this is an integration test because so many components have to come together to
        // ensure these configurations wind up where they belong, and any number of future code changes
        // could break this change.

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            final CountDownLatch latch = new CountDownLatch(1);
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            final AtomicBoolean flag = new AtomicBoolean(false);
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> flag.set(true));

            kafkaStreams.start();

            produceMessages(0L, inputTopic, "A");
            latch.await(15, TimeUnit.SECONDS);

            assertThat(processorValueCollector.size(), equalTo(2));
            assertThat(kafkaStreams.state(), equalTo(KafkaStreams.State.ERROR));
            assertThat("handler was called", flag.get());
        }
    }

    @Test
    public void shouldShutdownClient() throws Exception {
        //
        //
        // Also note that this is an integration test because so many components have to come together to
        // ensure these configurations wind up where they belong, and any number of future code changes
        // could break this change.

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            final CountDownLatch latch = new CountDownLatch(1);
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));

            kafkaStreams.setUncaughtExceptionHandler(exception -> SHUTDOWN_CLIENT);

            kafkaStreams.start();

            produceMessages(0L, inputTopic, "A");
            latch.await(15, TimeUnit.SECONDS);

            assertThat(processorValueCollector.size(), equalTo(1));
            assertThat(kafkaStreams.state(), equalTo(KafkaStreams.State.NOT_RUNNING));
        }
    }

    @Test
    public void shouldShutdownApplication() throws Exception {
        //
        //
        // Also note that this is an integration test because so many components have to come together to
        // ensure these configurations wind up where they belong, and any number of future code changes
        // could break this change.

        builder.stream(idempotentTopic).filter((k, v) -> true);

        final Topology topology = builder.build();

        try (final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties)) {
            final KafkaStreams kafkaStreams1 = new KafkaStreams(topology, properties);
            final CountDownLatch latch = new CountDownLatch(1);
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams1.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams.setUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION);
            kafkaStreams1.setUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION);


            kafkaStreams.start();
            kafkaStreams1.start();

            produceMessages(0L, idempotentTopic, "B");
            produceMessages(0L, inputTopic, "A");
            latch.await(30, TimeUnit.SECONDS);

            assertThat(processorValueCollector.size(), equalTo(1));
            assertThat(kafkaStreams.state(), equalTo(KafkaStreams.State.ERROR));
            assertThat(kafkaStreams1.state(), equalTo(KafkaStreams.State.ERROR));
//            kafkaStreams.close(Duration.ZERO);
//            kafkaStreams1.close(Duration.ZERO);
        }
    }

    @Test
    public void shouldShutdownSingleThreadApplication() throws Exception {
        //
        //
        // Also note that this is an integration test because so many components have to come together to
        // ensure these configurations wind up where they belong, and any number of future code changes
        // could break this change.

        final Properties properties  = mkObjectProperties(
                mkMap(
                        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                        mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                        mkEntry(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "5"),
                        mkEntry(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "6"),
                        mkEntry(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, "7"),
                        mkEntry(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, "480000"),
                        mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1),
                        mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                        mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class)
                )
        );

        builder.stream(idempotentTopic).filter((k, v) -> true);

        final Topology topology = builder.build();

        try (final KafkaStreams kafkaStreams = new KafkaStreams(topology, properties)) {
            final KafkaStreams kafkaStreams1 = new KafkaStreams(topology, properties);
            final CountDownLatch latch = new CountDownLatch(1);
            kafkaStreams.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams1.setUncaughtExceptionHandler((t, e) -> fail("should not hit old handler"));
            kafkaStreams.setUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION);
            kafkaStreams1.setUncaughtExceptionHandler(exception -> SHUTDOWN_APPLICATION);

            kafkaStreams.start();
            kafkaStreams1.start();

            produceMessages(0L, idempotentTopic, "B");
            produceMessages(0L, inputTopic, "A");
            latch.await(30, TimeUnit.SECONDS);

            assertThat(processorValueCollector.size(), equalTo(1));
            assertThat(kafkaStreams.state(), equalTo(KafkaStreams.State.ERROR));
            assertThat(kafkaStreams1.state(), equalTo(KafkaStreams.State.ERROR));
        }
    }



    private void produceMessages(final long timestamp, final String streamOneInput, final String msg) {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
                streamOneInput,
                Collections.singletonList(new KeyValue<>("1", msg)),
                TestUtils.producerConfig(
                        CLUSTER.bootstrapServers(),
                        StringSerializer.class,
                        StringSerializer.class,
                        new Properties()),
                timestamp);
    }



    private static class ShutdownProcessor extends AbstractProcessor<String, String> {

        final List<String> valueList;

        ShutdownProcessor(final List<String> valueList) {
            this.valueList = valueList;
        }

        @Override
        public void process(final String key, final String value) {
            valueList.add(value + " " + context.taskId());
            throw new StreamsException(Thread.currentThread().getName());
        }
    }
}


