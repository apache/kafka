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

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.common.utils.Utils.mkSet;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.cleanStateBeforeTest;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.quietlyCleanStateAfterTest;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.IntegrationTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

@Category(IntegrationTest.class)
public class ProcessorContextIntegrationTest {

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(
        1,
        mkProperties(mkMap()),
        0L
    );

    @Rule
    public final TestName testName = new TestName();

    @BeforeClass
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterClass
    public static void closeCluster() {
        CLUSTER.stop();
    }

    @Test
    public void shouldReturnPositionAndTime() throws InterruptedException {
        // set up the test itself.
        final String testId = IntegrationTestUtils.safeUniqueTestName(getClass(), testName);
        final String input1 = "input1-" + testId;
        final String input2 = "input2-" + testId;
        cleanStateBeforeTest(CLUSTER, 2, input1, input2);

        // We're creating two partitions, so these will be the two tasks:
        final TaskId task00 = new TaskId(0, 0);
        final TaskId task01 = new TaskId(0, 1);

        final long now = System.currentTimeMillis();

        // create the input data
        final List<KeyValueTimestamp<String, String>> input1Records = asList(
            new KeyValueTimestamp<>("a", "v1", now + 1),
            new KeyValueTimestamp<>("b", "v2", now + 2),
            new KeyValueTimestamp<>("c", "v1", now + 3),
            new KeyValueTimestamp<>("d", "x", now + 4)
        );

        produceSynchronously(input1, input1Records);

        final List<KeyValueTimestamp<String, String>> input2Records = asList(
            new KeyValueTimestamp<>("e", "v1", now + 5),
            new KeyValueTimestamp<>("f", "v2", now + 6),
            new KeyValueTimestamp<>("g", "v1", now + 7)
        );

        produceSynchronously(input2, input2Records);

        // create the collectors for the observed properties
        final ConcurrentHashMap<TaskId, Long> systemTimeOnPunctuate = new ConcurrentHashMap<>();
        final ConcurrentHashMap<TaskId, Long> streamTimeOnPunctuate = new ConcurrentHashMap<>();
        final ConcurrentHashMap<TaskId, Map<TopicPartition, Long>> positionOnPunctuate =
            new ConcurrentHashMap<>();
        final ConcurrentHashMap<TaskId, Long> systemTimeOnProcess = new ConcurrentHashMap<>();
        final ConcurrentHashMap<TaskId, Long> streamTimeOnProcess = new ConcurrentHashMap<>();
        final ConcurrentHashMap<TaskId, Map<TopicPartition, Long>> positionOnProcess =
            new ConcurrentHashMap<>();

        // create the latches so we can delay evaluation until we've processed everything
        final CountDownLatch processLatch = new CountDownLatch(
            input1Records.size() + input2Records.size());
        // we just need to punctuate at least for each task
        final ConcurrentHashMap<TaskId, CountDownLatch> punctuateLatches = new ConcurrentHashMap<>();

        punctuateLatches.put(task00, new CountDownLatch(1));
        punctuateLatches.put(task01, new CountDownLatch(1));

        // create the streams app, which simply subscribes to both input topics and captures the
        // desired properties during processing and punctuation
        final Topology topology = new Topology();

        topology.addSource("source1", new StringDeserializer(), new StringDeserializer(), input1);
        topology.addSource("source2", new StringDeserializer(), new StringDeserializer(), input2);
        topology.addProcessor("processor", () -> new Processor<Object, Object, Object, Object>() {
            private ProcessorContext<Object, Object> context;

            @Override
            public void init(final ProcessorContext<Object, Object> context) {
                this.context = context;
                context.schedule(
                    Duration.ofMillis(1),
                    PunctuationType.WALL_CLOCK_TIME,
                    timestamp -> {
                        final TaskId taskId = context.taskId();
                        positionOnPunctuate.put(taskId, context.currentPositions());
                        streamTimeOnPunctuate.put(taskId, context.currentStreamTimeMs());
                        systemTimeOnPunctuate.put(taskId, this.context.currentSystemTimeMs());
                        punctuateLatches.get(taskId).countDown();
                    }
                );
            }

            @Override
            public void process(final Record<Object, Object> record1) {
                final TaskId taskId = context.taskId();
                final Map<TopicPartition, Long> positions = context.currentPositions();
                positionOnProcess.put(taskId, positions);
                streamTimeOnProcess.put(taskId, context.currentStreamTimeMs());
                systemTimeOnProcess.put(taskId, context.currentSystemTimeMs());
                processLatch.countDown();
            }
        }, "source1", "source2");

        final Properties streamsConfig = getStreamsConfig(testId);

        // start Streams
        final KafkaStreams driver =
            IntegrationTestUtils.getRunningStreams(streamsConfig, topology, true);
        try {

            // wait until we've processed all the inputs and punctuated both tasks
            processLatch.await(2, TimeUnit.MINUTES);
            for (final CountDownLatch value : punctuateLatches.values()) {
                value.await(2, TimeUnit.MINUTES);
            }

            // relying on the default partitioning of our test keys to be stable for this assertion
            // based on that one assumption, and the fact that we waited for all the inputs to be
            // processed, we can assert the exact position that the processors should have seen by now.
            assertThat(
                positionOnProcess,
                equalTo(
                    mkMap(
                        mkEntry(
                            task00,
                            mkMap(
                                mkEntry(new TopicPartition(input1, 0), 3L),
                                mkEntry(new TopicPartition(input2, 0), 1L)
                            )
                        ),
                        mkEntry(
                            task01,
                            mkMap(
                                mkEntry(new TopicPartition(input1, 1), 1L),
                                mkEntry(new TopicPartition(input2, 1), 2L)
                            )
                        )
                    )
                )
            );

            assertThat(
                streamTimeOnProcess,
                equalTo(
                    mkMap(
                        mkEntry(task00, now + 5),
                        mkEntry(task01, now + 7)
                    )
                )
            );

            // for the rest of the properties, there's not much that we can reliably assert.
            final Set<TaskId> tasks = mkSet(task00, task01);
            assertThat(systemTimeOnProcess.keySet(), equalTo(tasks));
            assertThat(systemTimeOnPunctuate.keySet(), equalTo(tasks));
            assertThat(streamTimeOnPunctuate.keySet(), equalTo(tasks));
            assertThat(positionOnPunctuate.keySet(), equalTo(tasks));
            for (final TaskId task : tasks) {
                assertThat(systemTimeOnProcess.get(task), greaterThanOrEqualTo(now));
                assertThat(streamTimeOnProcess.get(task), greaterThanOrEqualTo(now));
                assertThat(systemTimeOnPunctuate.get(task), greaterThanOrEqualTo(now));
                final Map<TopicPartition, Long> positions = positionOnPunctuate.get(task);
                final Set<TopicPartition> topicPartitions = mkSet(
                    new TopicPartition(input1, task.partition()),
                    new TopicPartition(input2, task.partition())
                );
                assertThat(positions.keySet(), equalTo(topicPartitions));
                for (final TopicPartition partition : topicPartitions) {
                    assertThat(positions.get(partition), greaterThanOrEqualTo(0L));
                }
            }
        } finally {
            driver.close();
            quietlyCleanStateAfterTest(CLUSTER, driver);
        }
    }

    private Properties getStreamsConfig(final String testId) {
        return mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, testId),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
    }

    private static void produceSynchronously(final String topic,
        final List<KeyValueTimestamp<String, String>> toProduce) {
        final Properties producerConfig = mkProperties(mkMap(
            mkEntry(ProducerConfig.CLIENT_ID_CONFIG, "anything"),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName()),
            mkEntry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers())
        ));
        IntegrationTestUtils.produceSynchronously(producerConfig, false, topic, Optional.empty(),
            toProduce);
    }
}
