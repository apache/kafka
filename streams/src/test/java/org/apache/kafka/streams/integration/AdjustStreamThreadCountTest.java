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
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.ThreadMetadata;
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
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.purgeLocalStreamsState;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;

@Category(IntegrationTest.class)
public class AdjustStreamThreadCountTest {

    @ClassRule
    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @Rule
    public TestName testName = new TestName();

    private final List<KafkaStreams.State> stateToTransitions = new ArrayList<>();
    private KafkaStreams kafkaStreams;
    private static String inputTopic;
    private static StreamsBuilder builder;
    private static Properties properties;
    private static String appId = "";
    public static final Duration DEFAULT_DURATION = Duration.ofSeconds(30);

    @Before
    public void setup() throws InterruptedException {
        final String testId = safeUniqueTestName(getClass(), testName);
        appId = "appId_" + testId;
        inputTopic = "input" + testId;
        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic);

        builder  = new StreamsBuilder();
        builder.stream(inputTopic);

        properties  = mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class)
            )
        );

        createAndRunStream();
    }

    @After
    public void teardown() throws IOException {
        if (kafkaStreams != null) {
            kafkaStreams.close();
        }
        purgeLocalStreamsState(properties);
    }

    private void addStreamStateChangeListener(final KafkaStreams kafkaStreams) {
        // we store each new state in state transition so that we won't miss any state change
        kafkaStreams.setStateListener(
            (newState, oldState) -> stateToTransitions.add(newState)
        );
    }

    private void waitForStateTransition(final KafkaStreams.State expected) throws InterruptedException {
        waitForCondition(
            () -> !stateToTransitions.isEmpty() && stateToTransitions.contains(expected),
            DEFAULT_DURATION.toMillis(),
            () -> String.format("Client did not change to the %s state in time. Observed new state transitions: %s",
                expected, stateToTransitions)
        );
    }

    private void createAndRunStream() throws InterruptedException {
        kafkaStreams = new KafkaStreams(builder.build(), properties);
        addStreamStateChangeListener(kafkaStreams);
        kafkaStreams.start();
        waitForStateTransition(KafkaStreams.State.RUNNING);
    }

    @Test
    public void shouldAddStreamThread() throws Exception {
        final int oldThreadCount = kafkaStreams.localThreadsMetadata().size();
        assertThat(kafkaStreams.localThreadsMetadata().stream().map(t -> t.threadName().split("-StreamThread-")[1]).sorted().toArray(), equalTo(new String[] {"1", "2"}));

        stateToTransitions.clear();
        final Optional<String> name = kafkaStreams.addStreamThread();

        assertThat(name, not(Optional.empty()));
        TestUtils.waitForCondition(
            () -> kafkaStreams.localThreadsMetadata().stream().sequential()
                    .map(ThreadMetadata::threadName).anyMatch(t -> t.equals(name.orElse(""))),
            "Wait for the thread to be added"
        );
        assertThat(kafkaStreams.localThreadsMetadata().size(), equalTo(oldThreadCount + 1));
        assertThat(
            kafkaStreams
                .localThreadsMetadata()
                .stream()
                .map(t -> t.threadName().split("-StreamThread-")[1])
                .sorted().toArray(),
            equalTo(new String[] {"1", "2", "3"})
        );

        waitForStateTransition(KafkaStreams.State.REBALANCING);
        waitForStateTransition(KafkaStreams.State.RUNNING);
    }

    @Test
    public void shouldRemoveStreamThread() throws Exception {
        final int oldThreadCount = kafkaStreams.localThreadsMetadata().size();
        stateToTransitions.clear();
        assertThat(kafkaStreams.removeStreamThread().get().split("-")[0], equalTo(appId));
        assertThat(kafkaStreams.localThreadsMetadata().size(), equalTo(oldThreadCount - 1));

        waitForStateTransition(KafkaStreams.State.REBALANCING);
        waitForStateTransition(KafkaStreams.State.RUNNING);

    }

    @Test
    public void shouldAddAndRemoveThreadsMultipleTimes() throws InterruptedException {
        final int oldThreadCount = kafkaStreams.localThreadsMetadata().size();
        stateToTransitions.clear();

        final CountDownLatch latch = new CountDownLatch(2);
        final Thread one = adjustCountHelperThread(kafkaStreams, 4, latch);
        final Thread two = adjustCountHelperThread(kafkaStreams, 6, latch);
        two.start();
        one.start();
        latch.await(30, TimeUnit.SECONDS);
        assertThat(kafkaStreams.localThreadsMetadata().size(), equalTo(oldThreadCount));

        waitForStateTransition(KafkaStreams.State.REBALANCING);
        waitForStateTransition(KafkaStreams.State.RUNNING);
    }

    private Thread adjustCountHelperThread(final KafkaStreams kafkaStreams, final int count, final CountDownLatch latch) {
        return new Thread(() -> {
            for (int i = 0; i < count; i++) {
                kafkaStreams.addStreamThread();
                kafkaStreams.removeStreamThread();
            }
            latch.countDown();
        });
    }

    @Test
    public void shouldAddAndRemoveStreamThreadsWhileKeepingNamesCorrect() throws Exception {
        int oldThreadCount = kafkaStreams.localThreadsMetadata().size();
        stateToTransitions.clear();

        assertThat(
            kafkaStreams.localThreadsMetadata()
                .stream()
                .map(t -> t.threadName().split("-StreamThread-")[1])
                .sorted()
                .toArray(),
            equalTo(new String[] {"1", "2"})
        );

        // add a new thread
        final Optional<String> name = kafkaStreams.addStreamThread();

        assertThat("New thread has index 3", "3".equals(name.get().split("-StreamThread-")[1]));
        TestUtils.waitForCondition(
            () -> kafkaStreams
                .localThreadsMetadata()
                .stream().sequential()
                .map(ThreadMetadata::threadName)
                .anyMatch(t -> t.equals(name.get())),
            "Stream thread has not been added"
        );
        assertThat(kafkaStreams.localThreadsMetadata().size(), equalTo(oldThreadCount + 1));
        assertThat(
            kafkaStreams
                .localThreadsMetadata()
                .stream()
                .map(t -> t.threadName().split("-StreamThread-")[1])
                .sorted()
                .toArray(),
            equalTo(new String[] {"1", "2", "3"})
        );
        waitForStateTransition(KafkaStreams.State.REBALANCING);
        waitForStateTransition(KafkaStreams.State.RUNNING);

        oldThreadCount = kafkaStreams.localThreadsMetadata().size();
        stateToTransitions.clear();

        // remove a thread
        final Optional<String> removedThread = kafkaStreams.removeStreamThread();

        assertThat(removedThread, not(Optional.empty()));
        assertThat(kafkaStreams.localThreadsMetadata().size(), equalTo(oldThreadCount - 1));
        waitForStateTransition(KafkaStreams.State.REBALANCING);
        waitForStateTransition(KafkaStreams.State.RUNNING);

        stateToTransitions.clear();

        // add a new thread again
        final Optional<String> name2 = kafkaStreams.addStreamThread();

        assertThat(name2, not(Optional.empty()));
        TestUtils.waitForCondition(
            () -> kafkaStreams.localThreadsMetadata().stream().sequential()
                    .map(ThreadMetadata::threadName).anyMatch(t -> t.equals(name2.orElse(""))),
            "Wait for the thread to be added"
        );
        assertThat(kafkaStreams.localThreadsMetadata().size(), equalTo(oldThreadCount));
        assertThat(
            kafkaStreams
                .localThreadsMetadata()
                .stream()
                .map(t -> t.threadName().split("-StreamThread-")[1])
                .sorted()
                .toArray(),
            equalTo(new String[] {"1", "2", "3"})
        );

        assertThat("the new thread should have received the old threads name", name2.equals(removedThread));
        waitForStateTransition(KafkaStreams.State.REBALANCING);
        waitForStateTransition(KafkaStreams.State.RUNNING);
    }
}
