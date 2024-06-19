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

import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupAssignment;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.GroupSubscription;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.internals.LegacyKafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.KeyValueTimestamp;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.processor.assignment.ApplicationState;
import org.apache.kafka.streams.processor.assignment.AssignmentConfigs;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsAssignment.AssignedTask.Type;
import org.apache.kafka.streams.processor.assignment.KafkaStreamsState;
import org.apache.kafka.streams.processor.assignment.ProcessId;
import org.apache.kafka.streams.processor.assignment.TaskAssignor;
import org.apache.kafka.streams.processor.assignment.assignors.StickyTaskAssignor;
import org.apache.kafka.streams.processor.internals.StreamThread;
import org.apache.kafka.streams.processor.internals.StreamsPartitionAssignor;
import org.apache.kafka.streams.processor.internals.assignment.AssignorConfiguration.AssignmentListener;
import org.apache.kafka.streams.processor.internals.assignment.HighAvailabilityTaskAssignor;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.junit.rules.Timeout;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.produceSynchronously;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.readExactNumRecordsFromOffset;
import static org.apache.kafka.streams.integration.utils.IntegrationTestUtils.safeUniqueTestName;
import static org.apache.kafka.streams.processor.internals.assignment.AssignmentTestUtils.RACK_AWARE_ASSIGNMENT_TAG_KEYS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

@Category(IntegrationTest.class)
public class TaskAssignorIntegrationTest {
    @Rule
    public Timeout globalTimeout = Timeout.seconds(600);

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

    private static final int NUM_RECORDS_PER_PARTITION = 5;

    private static final int NUM_INPUT_PARTITIONS = 3;
    private static final int NUM_OUTPUT_PARTITIONS = 1;

    private static final int PARTITION_0 = 0;
    private static final int PARTITION_1 = 1;
    private static final int POISON_PARTITION = 2; // "poison pill" partition

    private static final TaskId PARTITION_0_TASK = new TaskId(0, PARTITION_0);
    private static final TaskId PARTITION_1_TASK = new TaskId(0, PARTITION_1);
    private static final TaskId POISON_PARTITION_TASK = new TaskId(0, POISON_PARTITION);

    private static final String CONTAINER_CONFIG = "task.assignor.test.container";

    private final Map<String, Object> configMap = new HashMap<>();

    private String inputTopic;
    private String outputTopic;

    private AssignmentListener configuredAssignmentListener;

    @Before
    public void setup() {
        final String testId = safeUniqueTestName(testName);
        final String appId = "appId_" + testId;

        inputTopic = "input" + testId;
        outputTopic = "output" + testId;

        try {
            CLUSTER.deleteAllTopicsAndWait(15 * 1000L);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }

        // Maybe I'm paranoid, but I don't want the compiler deciding that my lambdas are equal to the identity
        // function and defeating my identity check
        final AtomicInteger compilerDefeatingReference = new AtomicInteger(0);

        // the implementation doesn't matter, we're just going to verify the reference.
        configuredAssignmentListener = stable -> compilerDefeatingReference.incrementAndGet();

        configMap.putAll(basicConfigs(appId, configuredAssignmentListener));
    }

    private static class ConfigContainer {
        final AtomicBoolean assignPartition0 = new AtomicBoolean(false);
        final AtomicBoolean assignPartition1 = new AtomicBoolean(false);
        final AtomicBoolean assignPoisonPartition2 = new AtomicBoolean(false);

        final AtomicBoolean taskAssignorConfigured = new AtomicBoolean(false);
        final AtomicBoolean taskAssignorReceivedFinalAssignment = new AtomicBoolean(false);
        final AtomicBoolean taskAssignorErrored = new AtomicBoolean(false);
    }

    public static class CustomTaskAssignor implements TaskAssignor {

        AtomicBoolean assignPartition0;
        AtomicBoolean assignPartition1;
        AtomicBoolean assignPoisonPartition;

        AtomicBoolean taskAssignorConfigured;
        AtomicBoolean taskAssignorReceivedFinalAssignment;
        AtomicBoolean taskAssignorErrored;

        @Override
        public TaskAssignment assign(final ApplicationState applicationState) {
            if (!taskAssignorConfigured.get()) {
                throw new AssertionError("Failed because TaskAssignor was not configured before assignment");
            }

            final Map<ProcessId, KafkaStreamsState> clientStates = applicationState.kafkaStreamsStates(false);
            if (clientStates.size() != 1) {
                throw new IllegalStateException("Should be only one KafkaStreams client but there was " + clientStates.size());
            }

            final Set<AssignedTask> tasksToAssign = new HashSet<>();
            if (assignPartition0.get()) {
                tasksToAssign.add(new AssignedTask(PARTITION_0_TASK, Type.ACTIVE));
            }
            if (assignPartition1.get()) {
                tasksToAssign.add(new AssignedTask(PARTITION_1_TASK, Type.ACTIVE));
            }
            if (assignPoisonPartition.get()) {
                tasksToAssign.add(new AssignedTask(POISON_PARTITION_TASK, Type.ACTIVE));
            }

            return new TaskAssignment(Collections.singleton(
                KafkaStreamsAssignment.of(
                    clientStates.keySet().iterator().next(),
                    tasksToAssign
                )
            ));
        }

        @Override
        public void onAssignmentComputed(final GroupAssignment assignment, final GroupSubscription subscription, final AssignmentError error) {
            taskAssignorReceivedFinalAssignment.set(true);
            if (error != AssignmentError.NONE) {
                taskAssignorErrored.set(true);
                throw new AssertionError("Failed because task assignment was invalid due to error: " + error);
            }
        }

        @Override
        public void configure(final Map<String, ?> configs) {
            final ConfigContainer configContainer = (ConfigContainer) configs.get(CONTAINER_CONFIG);

            this.assignPartition0 = configContainer.assignPartition0;
            this.assignPartition1 = configContainer.assignPartition1;
            this.assignPoisonPartition = configContainer.assignPoisonPartition2;

            this.taskAssignorConfigured = configContainer.taskAssignorConfigured;
            this.taskAssignorReceivedFinalAssignment = configContainer.taskAssignorReceivedFinalAssignment;
            this.taskAssignorErrored = configContainer.taskAssignorErrored;

            taskAssignorConfigured.set(true);
        }
    }

    @Test
    public void shouldProperlyConfigureNewPublicTaskAssignorAndFollowCustomAssignmentLogic() throws NoSuchFieldException, IllegalAccessException, InterruptedException {
        final Class<CustomTaskAssignor> taskAssignorClassToUse = CustomTaskAssignor.class;
        final ConfigContainer assignorConfigs = new ConfigContainer();

        // Should choose to use the new assignor if both configs are set
        configMap.put(StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, MyLegacyTaskAssignor.class.getName());
        configMap.put(StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG, taskAssignorClassToUse.getName());

        configMap.put(CONTAINER_CONFIG, assignorConfigs);
        final Properties properties = mkObjectProperties(configMap);

        // Create output with only 1 partition so we can read all the outputs together and ignore key partitioning
        CLUSTER.createTopic(outputTopic, NUM_OUTPUT_PARTITIONS, 1);

        // Create input topic with 3 partitions and fill in all 3 with initial data
        CLUSTER.createTopic(inputTopic, NUM_INPUT_PARTITIONS, 1);

        produceSynchronously(properties, false, inputTopic, Optional.of(PARTITION_0), inputRecordsForPartition(String.valueOf(PARTITION_0)));
        produceSynchronously(properties, false, inputTopic, Optional.of(PARTITION_1), inputRecordsForPartition(String.valueOf(PARTITION_1)));
        produceSynchronously(properties, false, inputTopic, Optional.of(POISON_PARTITION), inputRecordsForPartition(String.valueOf(POISON_PARTITION)));

        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, String> input = builder.stream(inputTopic);

        input
            .processValues(() -> new FixedKeyProcessor<String, String, String>() {
                int partition;
                FixedKeyProcessorContext<String, String> context;

                @Override
                public void init(final FixedKeyProcessorContext<String, String> context) {
                    this.partition = context.taskId().partition();
                    this.context = context;
                }

                @Override
                public void process(final FixedKeyRecord<String, String> record) {
                    final FixedKeyRecord<String, String> recordToForward;
                    if (partition == PARTITION_0 && record.key().equals(String.valueOf(PARTITION_0))) {
                        recordToForward = record.withValue(String.valueOf(PARTITION_0));
                    } else if (partition == PARTITION_1 && record.key().equals(String.valueOf(PARTITION_1))) {
                        recordToForward = record.withValue(String.valueOf(PARTITION_1));
                    } else if (partition == POISON_PARTITION && record.key().equals(String.valueOf(POISON_PARTITION))) {
                        throw new RuntimeException("Poison partition record");
                    } else {
                        // this should not happen but instead of throwing an exception we just return without
                        // forwarding anything to distinguish from the poison pill case
                        return;
                    }
                    context.forward(recordToForward);
                }
            })
            .to(outputTopic);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            // only assign partition 0 at the start, so we should only get data from task 0_0 at first
            assignorConfigs.assignPartition0.set(true);

            kafkaStreams.start();

            validateConfiguration(taskAssignorClassToUse, configuredAssignmentListener, kafkaStreams);

            waitForStreamsState(kafkaStreams, State.RUNNING);

            assertThat(assignorConfigs.taskAssignorConfigured.get(), is(true));
            assertThat(assignorConfigs.taskAssignorReceivedFinalAssignment.get(), is(true));
            assertThat(assignorConfigs.taskAssignorErrored.get(), is(false));

            verifyOutputRecordsForInputPartition(String.valueOf(PARTITION_0), 0L);

            // allow partition 1 to be assigned and verify we now get output for partition 1
            assignorConfigs.assignPartition1.set(true);
            triggerRebalance(kafkaStreams);
            waitForStreamsState(kafkaStreams, State.RUNNING);

            verifyOutputRecordsForInputPartition(String.valueOf(PARTITION_1), NUM_RECORDS_PER_PARTITION);

            // assign no tasks at all and make sure it can reach RUNNING
            assignorConfigs.assignPartition0.set(false);
            assignorConfigs.assignPartition1.set(false);
            triggerRebalance(kafkaStreams);
            waitForStreamsState(kafkaStreams, State.RUNNING);

            // finally allow the "poison pill" partition 2 to be assigned and verify Streams goes into ERROR
            assignorConfigs.assignPoisonPartition2.set(true);
            triggerRebalance(kafkaStreams);
            waitForStreamsState(kafkaStreams, State.ERROR);
        }
    }

    @Test
    public void shouldUseStickyTaskAssignor() throws Exception {
        final Class<StickyTaskAssignor> taskAssignorClass = StickyTaskAssignor.class;

        final int numPartitions = 20;
        CLUSTER.createTopic(inputTopic, numPartitions, 1);

        // Should use the new assignor if it's configured and the internal legacy assignor config is not
        configMap.put(StreamsConfig.TASK_ASSIGNOR_CLASS_CONFIG, taskAssignorClass.getName());
        final Properties properties = mkObjectProperties(configMap);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.start();

            validateConfiguration(taskAssignorClass, configuredAssignmentListener, kafkaStreams);

            waitForStreamsState(kafkaStreams, State.RUNNING);
        }
    }

    // Just a dummy implementation so we can check the config
    public static final class MyLegacyTaskAssignor extends HighAvailabilityTaskAssignor { }

    @Test
    public void shouldProperlyConfigureLegacyTaskAssignor() throws Exception {
        final Class<MyLegacyTaskAssignor> taskAssignorClass = MyLegacyTaskAssignor.class;

        final int numPartitions = 20;
        CLUSTER.createTopic(inputTopic, numPartitions, 1);

        // Should use the legacy assignor if it's configured while the new public assignor config is not
        configMap.put(StreamsConfig.InternalConfig.INTERNAL_TASK_ASSIGNOR_CLASS, taskAssignorClass.getName());
        final Properties properties = mkObjectProperties(configMap);

        final StreamsBuilder builder = new StreamsBuilder();
        builder.stream(inputTopic);

        try (final KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), properties)) {
            kafkaStreams.start();

            validateConfiguration(taskAssignorClass, configuredAssignmentListener, kafkaStreams);

            waitForStreamsState(kafkaStreams, State.RUNNING);
        }
    }

    // This check uses reflection to check and make sure that all the expected configurations really
    // make it all the way to configure the task assignor. There's no other use case for being able
    // to extract all these fields, so reflection is a good choice until we find that the maintenance
    // burden is too high.
    //
    // Also note that this  has to be done in an integration test because so many components have to come together
    // to ensure these configurations wind up where they belong, and any number of future code changes
    // could break this change.
    @SuppressWarnings("unchecked")
    private void validateConfiguration(final Class<?> taskAssignorClass,
                                       final AssignmentListener configuredAssignmentListener,
                                       final KafkaStreams kafkaStreams) throws NoSuchFieldException, IllegalAccessException {
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
        assertThat(consumer, instanceOf(LegacyKafkaConsumer.class));

        final Field assignors = LegacyKafkaConsumer.class.getDeclaredField("assignors");
        assignors.setAccessible(true);
        final List<ConsumerPartitionAssignor> consumerPartitionAssignors = (List<ConsumerPartitionAssignor>) assignors.get(consumer);
        final StreamsPartitionAssignor streamsPartitionAssignor = (StreamsPartitionAssignor) consumerPartitionAssignors.get(0);

        final Field assignmentConfigs = StreamsPartitionAssignor.class.getDeclaredField("assignmentConfigs");
        assignmentConfigs.setAccessible(true);
        final AssignmentConfigs configs = (AssignmentConfigs) assignmentConfigs.get(streamsPartitionAssignor);

        final Field assignmentListenerField = StreamsPartitionAssignor.class.getDeclaredField("assignmentListener");
        assignmentListenerField.setAccessible(true);
        final AssignmentListener actualAssignmentListener = (AssignmentListener) assignmentListenerField.get(streamsPartitionAssignor);

        assertThat(configs.numStandbyReplicas(), is(5));
        assertThat(configs.acceptableRecoveryLag(), is(6L));
        assertThat(configs.maxWarmupReplicas(), is(7));
        assertThat(configs.probingRebalanceIntervalMs(), is(480000L));
        assertThat(configs.rackAwareTrafficCost(), is(OptionalInt.of(11)));
        assertThat(configs.rackAwareNonOverlapCost(), is(OptionalInt.of(12)));
        assertThat(configs.rackAwareAssignmentStrategy(), is(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC));
        assertThat(configs.rackAwareAssignmentTags(), is(RACK_AWARE_ASSIGNMENT_TAG_KEYS));
        assertThat(actualAssignmentListener, sameInstance(configuredAssignmentListener));

        if (MyLegacyTaskAssignor.class.isAssignableFrom(taskAssignorClass)) {
            final Field legacytaskAssignorSupplierField = StreamsPartitionAssignor.class.getDeclaredField("legacyTaskAssignorSupplier");
            legacytaskAssignorSupplierField.setAccessible(true);
            final Supplier<MyLegacyTaskAssignor> legacyTaskAssignorSupplier =
                (Supplier<MyLegacyTaskAssignor>) legacytaskAssignorSupplierField.get(streamsPartitionAssignor);
            final MyLegacyTaskAssignor legacyTaskAssignor = legacyTaskAssignorSupplier.get();

            assertThat(legacyTaskAssignor, instanceOf(taskAssignorClass));
        } else if (TaskAssignor.class.isAssignableFrom(taskAssignorClass)) {
            // New/public task assignor interface
            final Field taskAssignorSupplierField = StreamsPartitionAssignor.class.getDeclaredField("customTaskAssignorSupplier");
            taskAssignorSupplierField.setAccessible(true);
            final Supplier<Optional<TaskAssignor>> taskAssignorSupplier =
                (Supplier<Optional<TaskAssignor>>) taskAssignorSupplierField.get(streamsPartitionAssignor);
            final Optional<TaskAssignor> newTaskAssignor = taskAssignorSupplier.get();

            assertThat(newTaskAssignor.isPresent(), is(true));
            assertThat(newTaskAssignor.get(), instanceOf(taskAssignorClass));
        } else {
            throw new AssertionError("Passed-in task assignor class does not implement any known TaskAssignor interface");
        }
    }

    /**
     * Verify that there are exactly the expected number of records in the output topic from the startOffset to the end, and that
     * the key and value both match the input partition that these records should have come from
     */
    private void verifyOutputRecordsForInputPartition(final String inputPartition, final long startOffset) {
        final List<KeyValue<String, String>> output =
            readExactNumRecordsFromOffset(outputTopic, 0, startOffset, NUM_RECORDS_PER_PARTITION, configMap);

        assertThat(output.size(), equalTo(NUM_RECORDS_PER_PARTITION));

        for (final KeyValue<String, String> record : output) {
            // Check keys to make sure we only processed records from the given input partition
            assertThat(record.key, equalTo(inputPartition));

            // Check values to make sure we fully processed these records
            assertThat(record.value, equalTo(inputPartition));

            if (!record.key.equals(inputPartition)) {
                throw new AssertionError(String.format("Output record key did not match expected partition: key=%s and partition=%s", record.key, inputPartition));
            } else if (!record.value.equals(inputPartition)) {
                throw new AssertionError(String.format("Output record value did not match: value=%s and partition=%s", record.value, inputPartition));
            }
        }
    }

    private static List<KeyValueTimestamp<String, String>> inputRecordsForPartition(final String partition) {
        return asList(
            new KeyValueTimestamp<>(partition, "ignored", 1L),
            new KeyValueTimestamp<>(partition, "ignored", 2L),
            new KeyValueTimestamp<>(partition, "ignored", 3L),
            new KeyValueTimestamp<>(partition, "ignored", 4L),
            new KeyValueTimestamp<>(partition, "ignored", 5L)
        );
    }

    private static Map<String, Object> basicConfigs(final String appId, final AssignmentListener configuredAssignmentListener) {
        final Map<String, Object> configMap = mkMap(
            mkEntry(StreamsConfig.InternalConfig.ASSIGNMENT_LISTENER, configuredAssignmentListener),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
            mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),

            mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
            mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class),
            mkEntry(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
            mkEntry(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class),
            mkEntry(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class),
            mkEntry(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class),

            // assignor configs
            mkEntry(StreamsConfig.NUM_STANDBY_REPLICAS_CONFIG, "5"),
            mkEntry(StreamsConfig.ACCEPTABLE_RECOVERY_LAG_CONFIG, "6"),
            mkEntry(StreamsConfig.MAX_WARMUP_REPLICAS_CONFIG, "7"),
            mkEntry(StreamsConfig.PROBING_REBALANCE_INTERVAL_MS_CONFIG, "480000"),
            mkEntry(StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_CONFIG, StreamsConfig.RACK_AWARE_ASSIGNMENT_STRATEGY_MIN_TRAFFIC),
            mkEntry(StreamsConfig.RACK_AWARE_ASSIGNMENT_TRAFFIC_COST_CONFIG, 11),
            mkEntry(StreamsConfig.RACK_AWARE_ASSIGNMENT_NON_OVERLAP_COST_CONFIG, 12),
            mkEntry(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, "480000"),
            mkEntry(StreamsConfig.RACK_AWARE_ASSIGNMENT_TAGS_CONFIG, String.join(",", RACK_AWARE_ASSIGNMENT_TAG_KEYS)),
            mkEntry(ConsumerConfig.CLIENT_RACK_CONFIG, "rack1")
        );
        RACK_AWARE_ASSIGNMENT_TAG_KEYS.forEach(key -> configMap.put(StreamsConfig.clientTagPrefix(key), "dummy"));

        return configMap;
    }

    private static void waitForStreamsState(final KafkaStreams kafkaStreams, final State state) throws InterruptedException {
        IntegrationTestUtils.waitForApplicationState(Collections.singletonList(kafkaStreams), state, Duration.ofSeconds(30));
    }

    private static void triggerRebalance(final KafkaStreams kafkaStreams) {
        kafkaStreams.removeStreamThread();
        kafkaStreams.addStreamThread();
    }

}
