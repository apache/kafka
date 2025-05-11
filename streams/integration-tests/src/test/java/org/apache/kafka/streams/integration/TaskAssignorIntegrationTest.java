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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.ClassicKafkaConsumer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentListener;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor;
import org.apache.kafka.streams.processor.internals.assignment.LegacyTaskAssignor;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

@Tag("integration")
@Timeout(600)
public class TaskAssignorIntegrationTest {

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1);

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
    }

    @AfterAll
    public static void closeCluster() {
        CLUSTER.stop();
    }

    // Just a dummy implementation so we can check the config
    public static final class MyLegacyTaskAssignor extends HighAvailabilityTaskAssignor implements
        LegacyTaskAssignor { }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldProperlyConfigureTheAssignor(final TestInfo testInfo) throws NoSuchFieldException, IllegalAccessException {
        // This test uses reflection to check and make sure that all the expected configurations really
        // make it all the way to configure the task assignor. There's no other use case for being able
        // to extract all these fields, so reflection is a good choice until we find that the maintenance
        // burden is too high.
        //
        // Also note that this is an integration test because so many components have to come together to
        // ensure these configurations wind up where they belong, and any number of future code changes
        // could break this change.

        final String testId = safeUniqueTestName(testInfo);
        final String appId = "appId_" + testId;
        final String inputTopic = "input" + testId;

        IntegrationTestUtils.cleanStateBeforeTest(CLUSTER, inputTopic);

        // Maybe I'm paranoid, but I don't want the compiler deciding that my lambdas are equal to the identity
        // function and defeating my identity check
        final AtomicInteger compilerDefeatingReference = new AtomicInteger(0);

        // the implementation doesn't matter, we're just going to verify the reference.
        final AssignmentListener configuredAssignmentListener =
            stable -> compilerDefeatingReference.incrementAndGet();

        final Properties properties = mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "5"),
                mkEntry(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "6"),
                mkEntry(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, "7"),
                mkEntry(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, "480000"),
                mkEntry(StreamsConfig.InternalConfig.ASSIGNMENT_LISTENER, configuredAssignmentListener),
                mkEntry(StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, MyLegacyTaskAssignor.class.getName())
            )
        );

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.start();

            final Field threads = KafkaStreams.class.getDeclaredField("threads");
            threads.setAccessible(true);
            final  List<StreamThread> streamThreads = (List<StreamThread>) threads.get(kafkaStreams);
            final StreamThread streamThread = streamThreads.get(0);

            final Field mainConsumer = StreamThread.class.getDeclaredField("mainConsumer");
            mainConsumer.setAccessible(true);
            final KafkaConsumer<?, ?> parentConsumer = (KafkaConsumer<?, ?>) mainConsumer.get(streamThread);

            final Field delegate = KafkaConsumer.class.getDeclaredField("delegate");
            delegate.setAccessible(true);
            final Consumer<?, ?> consumer = (Consumer<?, ?>)  delegate.get(parentConsumer);
            assertThat(consumer, instanceOf(ClassicKafkaConsumer.class));

            final Field assignors = ClassicKafkaConsumer.class.getDeclaredField("assignors");
            assignors.setAccessible(true);
            final List<ConsumerPartitionAssignor> consumerPartitionAssignors = (List<ConsumerPartitionAssignor>) assignors.get(consumer);
            final StreamsPartitionAssignor streamsPartitionAssignor = (StreamsPartitionAssignor) consumerPartitionAssignors.get(0);

            final Field assignmentConfigs = StreamsPartitionAssignor.class.getDeclaredField("assignmentConfigs");
            assignmentConfigs.setAccessible(true);
            final AssignmentConfigs configs = (AssignmentConfigs) assignmentConfigs.get(streamsPartitionAssignor);

            final Field assignmentListenerField = StreamsPartitionAssignor.class.getDeclaredField("assignmentListener");
            assignmentListenerField.setAccessible(true);
            final AssignmentListener actualAssignmentListener = (AssignmentListener) assignmentListenerField.get(streamsPartitionAssignor);

            final Field taskAssignorSupplierField = StreamsPartitionAssignor.class.getDeclaredField("legacyTaskAssignorSupplier");
            taskAssignorSupplierField.setAccessible(true);
            final Supplier<LegacyTaskAssignor> taskAssignorSupplier =
                (Supplier<LegacyTaskAssignor>) taskAssignorSupplierField.get(streamsPartitionAssignor);
            final LegacyTaskAssignor taskAssignor = taskAssignorSupplier.get();

            assertThat(configs.numStandbyReplicas(), is(5));
            assertThat(configs.acceptableRecoveryLag(), is(6L));
            assertThat(configs.maxWarmupReplicas(), is(7));
            assertThat(configs.probingRebalanceIntervalMs(), is(480000L));
            assertThat(actualAssignmentListener, sameInstance(configuredAssignmentListener));
            assertThat(taskAssignor, instanceOf(MyLegacyTaskAssignor.class));
        }
    }
}
