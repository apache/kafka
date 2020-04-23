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
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.StateDirectory;
import org.apache.kafka.streams.state.internals.OffsetCheckpoint;
import org.apache.kafka.test.TestUtils;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertTrue;

/**
 * An integration test to verify the conversion of a dirty-closed EOS
 * task towards a standby task is safe across restarts of the application.
 */
@RunWith(Parameterized.class)
public class StandbyTaskEOSIntegrationTest {

    @Parameterized.Parameters(name = "{0}")
    public static Collection<String[]> data() {
        return Arrays.asList(new String[][] {
            {StreamsConfig.EXACTLY_ONCE},
            {StreamsConfig.EXACTLY_ONCE_BETA}
        });
    }

    @Parameterized.Parameter
    public String eosConfig;

    private final String appId = "eos-test-app";
    private final String inputTopic = "input";

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(3);

    @Rule
    public TestName testName = new TestName();

    @Before
    public void createTopics() throws Exception {
        CLUSTER.deleteTopicsAndWait(inputTopic, appId + "-KSTREAM-AGGREGATE-STATE-STORE-0000000001-changelog");
        CLUSTER.createTopic(inputTopic, 1, 3);
    }

    @Test
    public void surviveWithOneTaskAsStandby() throws ExecutionException, InterruptedException, IOException {
        IntegrationTestUtils.produceKeyValuesSynchronouslyWithTimestamp(
            inputTopic,
            Collections.singletonList(
                new KeyValue<>(0, 0)),
            TestUtils.producerConfig(
                CLUSTER.bootstrapServers(),
                IntegerSerializer.class,
                IntegerSerializer.class,
                new Properties()),
            10L);

        final String stateDirPath = TestUtils.tempDirectory(appId).getPath();

        final CountDownLatch instanceLatch = new CountDownLatch(1);

        try (
            final KafkaStreams streamInstanceOne = buildStreamWithDirtyStateDir(stateDirPath + "/" + appId + "-1/", instanceLatch);
            final KafkaStreams streamInstanceTwo = buildStreamWithDirtyStateDir(stateDirPath + "/" + appId + "-2/", instanceLatch);
        ) {


            streamInstanceOne.start();

            streamInstanceTwo.start();

            // Wait for the record to be processed
            assertTrue(instanceLatch.await(15, TimeUnit.SECONDS));

            waitForCondition(() -> streamInstanceOne.state().equals(KafkaStreams.State.RUNNING),
                             "Stream instance one should be up and running by now");
            waitForCondition(() -> streamInstanceTwo.state().equals(KafkaStreams.State.RUNNING),
                             "Stream instance two should be up and running by now");

            streamInstanceOne.close(Duration.ZERO);
            streamInstanceTwo.close(Duration.ZERO);

            streamInstanceOne.cleanUp();
            streamInstanceTwo.cleanUp();
        }
    }

    private KafkaStreams buildStreamWithDirtyStateDir(final String stateDirPath,
                                                      final CountDownLatch recordProcessLatch) throws IOException {

        final StreamsBuilder builder = new StreamsBuilder();
        final TaskId taskId = new TaskId(0, 0);

        final Properties props = props(stateDirPath);

        final StateDirectory stateDirectory = new StateDirectory(
            new StreamsConfig(props), new MockTime(), true);

        new OffsetCheckpoint(new File(stateDirectory.directoryForTask(taskId), ".checkpoint"))
            .write(Collections.singletonMap(new TopicPartition("unknown-topic", 0), 5L));

        assertTrue(new File(stateDirectory.directoryForTask(taskId),
                            "rocksdb/KSTREAM-AGGREGATE-STATE-STORE-0000000001").mkdirs());

        builder.stream(inputTopic,
                       Consumed.with(Serdes.Integer(), Serdes.Integer()))
               .groupByKey()
               .count()
               .toStream()
               .peek((key, value) -> recordProcessLatch.countDown());

        return new KafkaStreams(builder.build(), props);
    }

    private Properties props(final String stateDirPath) {
        final Properties streamsConfiguration = new Properties();
        final String suffix = testName.getMethodName()
            .replace('[', '_')
            .replace(']', '_');
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, appId + suffix);
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers());
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDirPath);
        streamsConfiguration.put(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);
        streamsConfiguration.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, eosConfig);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Integer().getClass());
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return streamsConfiguration;
    }
}
