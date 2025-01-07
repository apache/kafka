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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TaskMetadata;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.api.ContextualProcessor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

@Tag("integration")
@Timeout(600)
public class TaskMetadataIntegrationTest {

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1, new Properties(), Collections.emptyMap(), 0L, 0L);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    public TestInfo testInfo;

    private String inputTopic;
    private static StreamsBuilder builder;
    private static Properties properties;
    private static final String APP_ID_PREFIX = "TaskMetadataTest_";
    private static String appId;
    private AtomicBoolean process;
    private AtomicBoolean commit;

    @BeforeEach
    public void setup(final TestInfo testInfo) {
        final String testId = safeUniqueTestName(testInfo);
        appId = APP_ID_PREFIX + testId;
        inputTopic = "input" + testId;
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic);

        builder  = new StreamsBuilder();

        process = new AtomicBoolean(true);
        commit = new AtomicBoolean(true);

        final KStream<String, String> stream = builder.stream(inputTopic);
        stream.process(PauseProcessor::new);

        properties  = mkObjectProperties(
                mkMap(
                        mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                        mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                        mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                        mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2),
                        mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                        mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                        mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1L)
                )
        );
    }

    @Test
    public void shouldReportCorrectCommittedOffsetInformation() {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);
            final TaskMetadata taskMetadata = getTaskMetadata(kafkaStreams);
            assertThat(taskMetadata.committedOffsets().size(), equalTo(1));
            final TopicPartition topicPartition = new TopicPartition(inputTopic, 0);

            produceMessages(0L, inputTopic, "test");
            TestUtils.waitForCondition(() -> !process.get(), "The record was not processed");
            TestUtils.waitForCondition(() -> taskMetadata.committedOffsets().get(topicPartition) == 1L, "the record was processed");
            process.set(true);

            produceMessages(0L, inputTopic, "test1");
            TestUtils.waitForCondition(() -> !process.get(), "The record was not processed");
            TestUtils.waitForCondition(() -> taskMetadata.committedOffsets().get(topicPartition) == 2L, "the record was processed");
            process.set(true);

            produceMessages(0L, inputTopic, "test1");
            TestUtils.waitForCondition(() -> !process.get(), "The record was not processed");
            TestUtils.waitForCondition(() -> taskMetadata.committedOffsets().get(topicPartition) == 3L, "the record was processed");
        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void shouldReportCorrectEndOffsetInformation() {
        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            IntegrationTestUtils.startApplicationAndWaitUntilRunning(kafkaStreams);
            final TaskMetadata taskMetadata = getTaskMetadata(kafkaStreams);
            assertThat(taskMetadata.endOffsets().size(), equalTo(1));
            final TopicPartition topicPartition = new TopicPartition(inputTopic, 0);
            commit.set(false);

            for (int i = 0; i < 10; i++) {
                produceMessages(0L, inputTopic, "test");
                TestUtils.waitForCondition(() -> !process.get(), "The record was not processed");
                process.set(true);
            }
            assertThat(taskMetadata.endOffsets().get(topicPartition), equalTo(9L));

        } catch (final Exception e) {
            e.printStackTrace();
        }
    }

    private TaskMetadata getTaskMetadata(final KafkaStreams kafkaStreams) throws InterruptedException {
        final AtomicReference<List<TaskMetadata>> taskMetadataList = new AtomicReference<>();
        TestUtils.waitForCondition(() -> {
            taskMetadataList.set(kafkaStreams.metadataForLocalThreads().stream().flatMap(t -> t.activeTasks().stream()).collect(Collectors.toList()));
            return taskMetadataList.get().size() == 1;
        }, "The number of active tasks returned in the allotted time was not one.");
        return taskMetadataList.get().get(0);
    }

    @AfterEach
    public void teardown() throws IOException {
        purgeLocalStreamsState(properties);
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

    private class PauseProcessor extends ContextualProcessor<String, String, Void, Void> {
        @Override
        public void process(final Record<String, String> record) {
            while (!process.get()) {
                try {
                    wait(100);
                } catch (final InterruptedException e) {
                }
            }
            if (commit.get()) {
                context().commit();
            }
            process.set(false);
        }
    }
}
