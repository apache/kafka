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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.streams.AssignmentRefinerImpl;
import org.apache.kafka.streams.GroupProtocol;
import org.apache.kafka.streams.KafkaClientSupplier;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.apache.kafka.streams.integration.utils.IntegrationTestUtils;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.processor.StandbyUpdateListener;
import org.apache.kafka.streams.processor.StateRestoreListener;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.DefaultKafkaClientSupplier;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.Timeout;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.apache.kafka.common.utils.Utils.mkObjectProperties;
import static org.apache.kafka.common.utils.Utils.mkProperties;
import static org.apache.kafka.streams.utils.TestUtils.safeUniqueTestName;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the warm-up task lifecycle under the streams group protocol (KIP-1071) end to end, with real Kafka Streams
 * clients against the broker-side {@link AssignmentRefinerImpl}: the warm-up budget, a warm-up task given up because
 * the target assignment no longer wants it, the offset reporting that drives promotion, the promotion gate on
 * {@code acceptable.recovery.lag}, and warm-up tasks being disabled. The scale-out case itself is covered by
 * {@link StreamsGroupWarmupTaskIntegrationTest}.
 *
 * <p>All stores are in memory, so a task that is closed rather than recycled has to restore its changelog from
 * scratch, which the restore listeners record. Where a test has to hold a warm-up task behind, a {@link RestoreGate}
 * withholds the records of the instance's restore consumer. A gate that is closed from the start would leave the
 * warm-up task with nothing restored and hence no offset to report, so a gate lets a small budget of records through
 * first.
 */
@Timeout(600)
@Tag("integration")
public class StreamsGroupWarmupTaskLifecycleIntegrationTest {
    private static final Properties BROKER_CONFIG = mkProperties(mkMap(
        mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_REFINER_CLASS_CONFIG, AssignmentRefinerImpl.class.getName()),
        mkEntry(GroupCoordinatorConfig.STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG_CONFIG, "0")
    ));

    public static final EmbeddedKafkaCluster CLUSTER = new EmbeddedKafkaCluster(1, BROKER_CONFIG);

    private static final String SUBTOPOLOGY_ID = "0";
    private static final int HEARTBEAT_INTERVAL_MS = 500;
    private static final long WAIT_MS = 60_000L;
    private static final int RECORDS_PER_PARTITION = 200;
    private static final int RESTORE_BUDGET = 20;

    private static Admin admin;

    private final List<Instance> instances = new ArrayList<>();
    private String appId;
    private String inputTopic;
    private String storeName;
    private String changelogTopic;
    private int numPartitions;

    @BeforeAll
    public static void startCluster() throws IOException {
        CLUSTER.start();
        admin = Admin.create(Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()));
    }

    @AfterAll
    public static void closeCluster() {
        admin.close();
        CLUSTER.stop();
    }

    @AfterEach
    public void tearDown() {
        for (final Instance instance : instances) {
            instance.gate.open();
            instance.streams.close(Duration.ofSeconds(30));
        }
        instances.clear();
    }

    @Test
    public void shouldReportTheOffsetsOfAWarmupTaskAsSoonAsItIsCaughtUp(final TestInfo testInfo) throws Exception {
        setUp(testInfo, 2);
        final Instance a = startOwnerOfAllTasks();
        // Far longer than the test waits, so the interval never elapses and cannot be what reports the offsets.
        CLUSTER.setGroupTaskOffsetInterval(appId, 300_000);

        final Instance b = startInstance("b", RESTORE_BUDGET);
        final int task = awaitStagedMigration(a, b);

        // The warm-up task catches up only now, long after the offsets it reported when it was assigned. Only the
        // client reporting a caught-up warm-up task on the next heartbeat lets the broker promote it in time.
        b.gate.open();
        waitForGroup("the warm-up task " + task + " to be promoted to active on b", group ->
            activeTasks(group, b).equals(Set.of(task)) && warmupTasks(group, b).isEmpty());
    }

    @Test
    public void shouldGateWarmupPromotionOnAcceptableRecoveryLag(final TestInfo testInfo) throws Exception {
        setUp(testInfo, 2);
        final long acceptableRecoveryLag = RESTORE_BUDGET / 4;
        CLUSTER.setGroupAcceptableRecoveryLag(appId, acceptableRecoveryLag);
        final Instance a = startOwnerOfAllTasks();

        final Instance b = startInstance("b", RESTORE_BUDGET);
        final int task = awaitStagedMigration(a, b);
        // The client reports Long.MAX_VALUE for an offset it does not know yet, which the broker treats as an unknown
        // lag. The task would be withheld for that reason alone, so wait for both offsets to be real.
        waitForGroup("the broker to learn how far behind the warm-up task " + task + " is", group -> {
            final StreamsGroupMemberDescription member = member(group, b);
            if (member == null) {
                return false;
            }
            final long offset = reportedOffset(member.taskOffsets(), task);
            final long endOffset = reportedOffset(member.taskEndOffsets(), task);
            return offset >= RESTORE_BUDGET && offset != Long.MAX_VALUE
                && endOffset != -1L && endOffset != Long.MAX_VALUE
                && endOffset - offset > acceptableRecoveryLag;
        });

        // The lag is known and above the threshold, so the task stays where it is.
        assertHolds("the warm-up task " + task + " to be withheld from b", Duration.ofSeconds(5), group ->
            warmupTasks(group, b).equals(Set.of(task)) && activeTasks(group, a).contains(task));

        // The gate still holds the warm-up task at the same lag, so it is only the raised threshold that promotes it.
        CLUSTER.setGroupAcceptableRecoveryLag(appId, 1_000_000L);
        waitForGroup("the warm-up task " + task + " to be promoted to active on b", group ->
            activeTasks(group, b).equals(Set.of(task)) && !activeTasks(group, a).contains(task));
    }

    @Test
    public void shouldWarmUpOneMigrationAtATimeWhenTwoMembersJoinAtOnce(final TestInfo testInfo) throws Exception {
        setUp(testInfo, 3);
        CLUSTER.setGroupNumWarmupReplicas(appId, 1);
        final Instance a = startOwnerOfAllTasks();

        final Instance b = startInstance("b", RESTORE_BUDGET);
        final Instance c = startInstance("c", RESTORE_BUDGET);
        // Both joiners are to take over a task, but with a budget of one warm-up task only one of the migrations is
        // warmed up, and the other waits with its task still running on a.
        waitForGroup("one of the two migrations to be warmed up, and the other to wait", group -> isStable(group, 3)
            && targetActiveTasks(group, b).size() == 1
            && targetActiveTasks(group, c).size() == 1
            && activeTasks(group, a).size() == 3
            && totalWarmupTasks(group) == 1);
        assertHolds("the waiting migration not to be warmed up while the budget is spent", Duration.ofSeconds(5), group ->
            totalWarmupTasks(group) == 1 && activeTasks(group, a).size() == 3);

        b.gate.open();
        c.gate.open();
        final AtomicInteger maxWarmupTasksWhenStable = new AtomicInteger();
        waitForGroup("both migrations to complete", group -> {
            if (isStable(group, 3)) {
                maxWarmupTasksWhenStable.accumulateAndGet(totalWarmupTasks(group), Math::max);
            }
            return isStable(group, 3)
                && activeTasks(group, a).size() == 1
                && activeTasks(group, b).equals(targetActiveTasks(group, b))
                && activeTasks(group, c).equals(targetActiveTasks(group, c))
                && totalWarmupTasks(group) == 0;
        });
        assertTrue(maxWarmupTasksWhenStable.get() <= 1, "the group ran " + maxWarmupTasksWhenStable.get() + " warm-up tasks at once");
        // Neither joiner restored an active task, so both took over a warmed-up task: the waiting migration was warmed
        // up once the first one freed the budget, rather than moving cold. The group is stable as soon as the joiners
        // own their tasks, before a cold restore would even begin, so wait for each restore to end before checking.
        final StreamsGroupDescription converged = describeGroup();
        awaitNoActiveRestore(b, single(activeTasks(converged, b)));
        awaitNoActiveRestore(c, single(activeTasks(converged, c)));
    }

    @Test
    public void shouldGiveUpAWarmupTaskTheTargetAssignmentNoLongerWantsOnScaleIn(final TestInfo testInfo) throws Exception {
        setUp(testInfo, 4);
        final Instance a = startInstance("a");
        final Instance b = startInstance("b");
        waitForGroup("each instance to own two active tasks", group -> isStable(group, 2)
            && activeTasks(group, a).size() == 2 && activeTasks(group, b).size() == 2 && totalWarmupTasks(group) == 0);
        produceAndAwaitChangelog();
        awaitNoActiveTaskReportedAsRestoring();

        final Instance c = startInstance("c", RESTORE_BUDGET);
        final AtomicReference<Integer> warmupTask = new AtomicReference<>();
        waitForGroup("c to warm up one task", group -> {
            if (!isStable(group, 3) || !activeTasks(group, c).isEmpty() || warmupTasks(group, c).size() != 1) {
                return false;
            }
            warmupTask.set(single(warmupTasks(group, c)));
            return true;
        });
        final int task = warmupTask.get();
        final StreamsGroupDescription beforeScaleIn = describeGroup();
        final Instance owner = activeTasks(beforeScaleIn, a).contains(task) ? a : b;
        final Instance leaver = owner == a ? b : a;
        final Set<Integer> tasksOfOwner = activeTasks(beforeScaleIn, owner);
        final Set<Integer> tasksOfLeaver = activeTasks(beforeScaleIn, leaver);
        final int restoresByOwner = owner.restoreEnds(task);

        // With the leaver gone, the owner is no longer over its share and keeps the task it was to hand to c, while c
        // takes over the leaver's tasks instead. So the warm-up task on c is given up rather than promoted.
        leaver.streams.close(Duration.ofSeconds(30));
        instances.remove(leaver);
        waitForGroup("c to give up the warm-up task " + task + " and take over the leaver's tasks", group -> isStable(group, 2)
            && targetActiveTasks(group, c).equals(tasksOfLeaver)
            && activeTasks(group, c).equals(tasksOfLeaver)
            && activeTasks(group, owner).equals(tasksOfOwner)
            && totalWarmupTasks(group) == 0);
        // Observed through the standby update listener rather than the thread metadata, which a stream thread only
        // refreshes once all its active tasks are restored -- and c's gate holds back the restore of its new tasks.
        TestUtils.waitForCondition(
            () -> c.standbySuspensionsByTask.containsKey(task),
            WAIT_MS,
            () -> "c never closed the warm-up task " + task
        );
        assertEquals(StandbyUpdateListener.SuspendReason.MIGRATED, c.standbySuspensionsByTask.get(task),
            "c should have given up the warm-up task " + task + " rather than promoted it");
        assertEquals(restoresByOwner, owner.restoreEnds(task), "the owner should have kept running " + task + " throughout");
    }

    @Test
    public void shouldMoveTasksColdWhenWarmupTasksAreDisabled(final TestInfo testInfo) throws Exception {
        setUp(testInfo, 2);
        CLUSTER.setGroupNumWarmupReplicas(appId, 0);
        final Instance a = startOwnerOfAllTasks();

        final Instance b = startInstance("b");
        final AtomicBoolean sawWarmupTask = new AtomicBoolean();
        waitForGroup("b to take over a task", group -> {
            sawWarmupTask.compareAndSet(false, totalWarmupTasks(group) > 0);
            return isStable(group, 2) && activeTasks(group, a).size() == 1 && activeTasks(group, b).size() == 1;
        });

        assertFalse(sawWarmupTask.get(), "no warm-up task should be assigned with warm-up tasks disabled");
        final int movedTask = single(activeTasks(describeGroup(), b));
        awaitRestoreEnd(b, movedTask, 1);
        assertEquals(changelogSizes().get(movedTask), b.totalRestoredByTask.get(movedTask),
            "b should have taken over " + movedTask + " cold, restoring all of it from the changelog");
    }

    /**
     * Waits until the broker no longer takes any active task to be restoring. A client reports the changelog offsets of
     * an active task while it restores it, but only reports that the restore has finished with its next offset report,
     * which may be up to the task offset interval later. Until then the refiner sees no caught-up state on the owner to
     * protect and moves the task without warming it up.
     */
    private void awaitNoActiveTaskReportedAsRestoring() throws InterruptedException {
        waitForGroup("the broker to learn that all active tasks are running", group -> group.members().stream()
            .allMatch(member -> {
                final Set<Integer> reported = member.taskOffsets().stream()
                    .map(StreamsGroupMemberDescription.TaskOffset::partition)
                    .collect(Collectors.toSet());
                reported.retainAll(partitions(member.assignment().activeTasks()));
                return reported.isEmpty();
            }));
    }

    /**
     * Waits for the refiner to stage the migration of one of the owner's tasks to the joiner behind a warm-up task, and
     * for the joiner's gate to hold the warm-up task behind, and returns that task.
     */
    private int awaitStagedMigration(final Instance owner, final Instance joiner) throws InterruptedException {
        final AtomicReference<Integer> warmupTask = new AtomicReference<>();
        waitForGroup("a warm-up task to be assigned to " + joiner.name, group -> {
            if (!isStable(group, 2) || !activeTasks(group, joiner).isEmpty() || warmupTasks(group, joiner).size() != 1) {
                return false;
            }
            warmupTask.set(single(warmupTasks(group, joiner)));
            return activeTasks(group, owner).contains(warmupTask.get());
        });
        TestUtils.waitForCondition(
            () -> joiner.gate.delivered.get() >= RESTORE_BUDGET,
            WAIT_MS,
            () -> joiner.name + " restored only " + joiner.gate.delivered.get() + " records before the gate closed"
        );
        return warmupTask.get();
    }

    /**
     * Waits for the instance to finish restoring the active task, and asserts that it restored nothing, ie, that it
     * took over the state of a warm-up task. Kafka Streams ends the restore of a recycled task with nothing to restore
     * too, so the wait always ends.
     */
    private static void awaitNoActiveRestore(final Instance instance, final int task) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> instance.totalRestoredByTask.containsKey(task),
            WAIT_MS,
            () -> instance.name + " never finished restoring its active task " + task
        );
        assertEquals(0L, instance.totalRestoredByTask.get(task), instance.name + " should have taken over a warmed-up task " + task);
    }

    /**
     * Waits until the instance's restores of the active task have ended the given number of times.
     */
    private static void awaitRestoreEnd(final Instance instance, final int task, final int restoreEnds) throws InterruptedException {
        TestUtils.waitForCondition(
            () -> instance.restoreEnds(task) >= restoreEnds,
            WAIT_MS,
            () -> instance.name + " finished restoring its active task " + task + " only " + instance.restoreEnds(task)
                + " times, expected " + restoreEnds
        );
    }

    /**
     * Starts the instance "a", waits until it owns all tasks, and fills the changelog. Waits also until the broker no
     * longer takes any of the tasks to be restoring, so that a later migration of them is staged behind a warm-up task.
     */
    private Instance startOwnerOfAllTasks() throws Exception {
        final Instance a = startInstance("a");
        waitForGroup("a to own all tasks", group -> isStable(group, 1) && activeTasks(group, a).size() == numPartitions);
        produceAndAwaitChangelog();
        awaitNoActiveTaskReportedAsRestoring();
        return a;
    }

    private void setUp(final TestInfo testInfo, final int numPartitions) throws InterruptedException {
        final String testId = safeUniqueTestName(testInfo);
        this.numPartitions = numPartitions;
        appId = "appId_" + System.currentTimeMillis() + "_" + testId;
        inputTopic = "input" + testId;
        storeName = "store" + testId;
        changelogTopic = appId + "-" + storeName + "-changelog";

        CLUSTER.createTopic(inputTopic, numPartitions, 1);
        CLUSTER.createTopic(changelogTopic, numPartitions, 1, Map.of(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT));
        // Refinement steps only advance on heartbeats, so short heartbeats keep the tests fast.
        CLUSTER.setGroupHeartbeatInterval(appId, HEARTBEAT_INTERVAL_MS);
        // The shortest interval allowed, so that the broker learns quickly that a restore has finished (see
        // awaitNoActiveTaskReportedAsRestoring).
        CLUSTER.setGroupTaskOffsetInterval(appId, GroupCoordinatorConfig.STREAMS_GROUP_MIN_TASK_OFFSET_INTERVAL_MS_DEFAULT);
        CLUSTER.setGroupStreamsInitialRebalanceDelay(appId, 0);
    }

    private Instance startInstance(final String name) {
        return startInstance(name, Long.MAX_VALUE);
    }

    private Instance startInstance(final String name, final long restoreBudget) {
        final Instance instance = new Instance(name, restoreBudget);
        instances.add(instance);
        instance.streams.start();
        return instance;
    }

    private Properties streamsProperties(final String name) {
        return mkObjectProperties(
            mkMap(
                mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, CLUSTER.bootstrapServers()),
                mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, appId),
                mkEntry(StreamsConfig.CLIENT_ID_CONFIG, clientIdPrefix(name)),
                mkEntry(StreamsConfig.STATE_DIR_CONFIG, TestUtils.tempDirectory().getPath()),
                mkEntry(StreamsConfig.GROUP_PROTOCOL_CONFIG, GroupProtocol.STREAMS.name().toLowerCase(Locale.getDefault())),
                mkEntry(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100L),
                mkEntry(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 1),
                mkEntry(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName()),
                mkEntry(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName())
            )
        );
    }

    private String clientIdPrefix(final String name) {
        return appId + "-" + name;
    }

    private void produceAndAwaitChangelog() throws Exception {
        final int numRecords = numPartitions * RECORDS_PER_PARTITION;
        final List<KeyValue<String, String>> records = IntStream.range(0, numRecords)
            .mapToObj(i -> KeyValue.pair(String.valueOf(i), "value" + i))
            .collect(Collectors.toList());
        IntegrationTestUtils.produceKeyValuesSynchronously(
            inputTopic,
            records,
            TestUtils.producerConfig(CLUSTER.bootstrapServers(), StringSerializer.class, StringSerializer.class),
            CLUSTER.time
        );

        final Map<TopicPartition, OffsetSpec> latest = IntStream.range(0, numPartitions).boxed()
            .collect(Collectors.toMap(p -> new TopicPartition(changelogTopic, p), p -> OffsetSpec.latest()));
        final AtomicLong changelogSize = new AtomicLong();
        TestUtils.waitForCondition(
            () -> {
                changelogSize.set(admin.listOffsets(latest).all().get().values().stream()
                    .mapToLong(info -> info.offset())
                    .sum());
                return changelogSize.get() == numRecords;
            },
            WAIT_MS,
            () -> "The changelog holds " + changelogSize.get() + " of " + numRecords + " records"
        );
    }

    /**
     * The number of records in each partition of the changelog, by task.
     */
    private Map<Integer, Long> changelogSizes() throws Exception {
        final Map<TopicPartition, OffsetSpec> latest = IntStream.range(0, numPartitions).boxed()
            .collect(Collectors.toMap(p -> new TopicPartition(changelogTopic, p), p -> OffsetSpec.latest()));
        return admin.listOffsets(latest).all().get().entrySet().stream()
            .collect(Collectors.toMap(entry -> entry.getKey().partition(), entry -> entry.getValue().offset()));
    }

    private StreamsGroupDescription describeGroup() throws InterruptedException {
        try {
            return admin.describeStreamsGroups(List.of(appId)).describedGroups().get(appId).get();
        } catch (final ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private void waitForGroup(final String description, final Predicate<StreamsGroupDescription> condition) throws InterruptedException {
        final AtomicReference<StreamsGroupDescription> lastDescribed = new AtomicReference<>();
        TestUtils.waitForCondition(
            () -> {
                final StreamsGroupDescription group = describeGroup();
                lastDescribed.set(group);
                return condition.test(group);
            },
            WAIT_MS,
            () -> "Timed out waiting for " + description + ". Last described group: " + lastDescribed.get()
        );
    }

    private void assertHolds(final String description,
                             final Duration duration,
                             final Predicate<StreamsGroupDescription> condition) throws InterruptedException {
        final long deadline = System.currentTimeMillis() + duration.toMillis();
        while (System.currentTimeMillis() < deadline) {
            final StreamsGroupDescription group = describeGroup();
            assertTrue(condition.test(group), "Expected " + description + ", but the group is " + group);
            Utils.sleep(HEARTBEAT_INTERVAL_MS / 2);
        }
    }

    private static boolean isStable(final StreamsGroupDescription group, final int numMembers) {
        return group.groupState() == GroupState.STABLE && group.members().size() == numMembers;
    }

    private StreamsGroupMemberDescription member(final StreamsGroupDescription group, final Instance instance) {
        final String prefix = clientIdPrefix(instance.name) + "-";
        return group.members().stream()
            .filter(member -> member.clientId().startsWith(prefix))
            .findFirst()
            .orElse(null);
    }

    private List<StreamsGroupMemberDescription> members(final StreamsGroupDescription group, final Instance instance) {
        final String prefix = clientIdPrefix(instance.name) + "-";
        return group.members().stream()
            .filter(member -> member.clientId().startsWith(prefix))
            .collect(Collectors.toList());
    }

    private Set<Integer> activeTasks(final StreamsGroupDescription group, final Instance instance) {
        return tasks(group, instance, member -> member.assignment().activeTasks());
    }

    private Set<Integer> warmupTasks(final StreamsGroupDescription group, final Instance instance) {
        return tasks(group, instance, member -> member.assignment().warmupTasks());
    }

    private Set<Integer> targetActiveTasks(final StreamsGroupDescription group, final Instance instance) {
        return tasks(group, instance, member -> member.targetAssignment().activeTasks());
    }

    private Set<Integer> tasks(final StreamsGroupDescription group,
                               final Instance instance,
                               final Function<StreamsGroupMemberDescription, List<StreamsGroupMemberAssignment.TaskIds>> tasksOfMember) {
        final Set<Integer> tasks = new HashSet<>();
        members(group, instance).forEach(member -> tasks.addAll(partitions(tasksOfMember.apply(member))));
        return tasks;
    }

    private static int totalWarmupTasks(final StreamsGroupDescription group) {
        return group.members().stream()
            .mapToInt(member -> partitions(member.assignment().warmupTasks()).size())
            .sum();
    }

    private static Set<Integer> partitions(final List<StreamsGroupMemberAssignment.TaskIds> taskIds) {
        final Set<Integer> partitions = new HashSet<>();
        for (final StreamsGroupMemberAssignment.TaskIds ids : taskIds) {
            assertEquals(SUBTOPOLOGY_ID, ids.subtopologyId());
            partitions.addAll(ids.partitions());
        }
        return partitions;
    }

    private static long reportedOffset(final List<StreamsGroupMemberDescription.TaskOffset> offsets, final int task) {
        return offsets.stream()
            .filter(offset -> offset.subtopologyId().equals(SUBTOPOLOGY_ID) && offset.partition() == task)
            .mapToLong(StreamsGroupMemberDescription.TaskOffset::offset)
            .findFirst()
            .orElse(-1L);
    }

    private static int single(final Set<Integer> tasks) {
        assertEquals(1, tasks.size(), "Expected exactly one task, got " + tasks);
        return tasks.iterator().next();
    }

    private final class Instance {
        private final String name;
        private final KafkaStreams streams;
        private final RestoreGate gate;
        // The total an active task's restore ended with, by task. A task's changelog partition is the task's partition.
        private final Map<Integer, Long> totalRestoredByTask = new ConcurrentHashMap<>();
        // How many restores of an active task have ended, by task.
        private final Map<Integer, AtomicInteger> restoreEndsByTask = new ConcurrentHashMap<>();
        // Why a standby or warm-up task last stopped being updated, by task.
        private final Map<Integer, StandbyUpdateListener.SuspendReason> standbySuspensionsByTask = new ConcurrentHashMap<>();

        private Instance(final String name, final long restoreBudget) {
            this.name = name;
            this.gate = new RestoreGate(restoreBudget);
            final StreamsBuilder builder = new StreamsBuilder();
            builder.table(inputTopic, Materialized.as(Stores.inMemoryKeyValueStore(storeName)));
            this.streams = new KafkaStreams(builder.build(), streamsProperties(name), new GatedRestoreClientSupplier(gate));
            streams.setGlobalStateRestoreListener(new StateRestoreListener() {
                @Override
                public void onRestoreStart(final TopicPartition topicPartition,
                                           final String storeName,
                                           final long startingOffset,
                                           final long endingOffset) {
                }

                @Override
                public void onBatchRestored(final TopicPartition topicPartition,
                                            final String storeName,
                                            final long batchEndOffset,
                                            final long numRestored) {
                }

                @Override
                public void onRestoreEnd(final TopicPartition topicPartition,
                                         final String storeName,
                                         final long totalRestored) {
                    totalRestoredByTask.put(topicPartition.partition(), totalRestored);
                    restoreEndsByTask.computeIfAbsent(topicPartition.partition(), __ -> new AtomicInteger()).incrementAndGet();
                }
            });
            streams.setStandbyUpdateListener(new StandbyUpdateListener() {
                @Override
                public void onUpdateStart(final TopicPartition topicPartition,
                                          final String storeName,
                                          final long startingOffset) {
                }

                @Override
                public void onBatchLoaded(final TopicPartition topicPartition,
                                          final String storeName,
                                          final TaskId taskId,
                                          final long batchEndOffset,
                                          final long batchSize,
                                          final long currentEndOffset) {
                }

                @Override
                public void onUpdateSuspended(final TopicPartition topicPartition,
                                              final String storeName,
                                              final long storeOffset,
                                              final long currentEndOffset,
                                              final SuspendReason reason) {
                    standbySuspensionsByTask.put(topicPartition.partition(), reason);
                }
            });
        }

        private int restoreEnds(final int task) {
            return restoreEndsByTask.getOrDefault(task, new AtomicInteger()).get();
        }
    }

    /**
     * Lets a budget of records through an instance's restore consumer and withholds the rest until opened.
     */
    private static final class RestoreGate {
        private final AtomicLong remaining;
        private final AtomicLong delivered = new AtomicLong();

        private RestoreGate(final long budget) {
            this.remaining = new AtomicLong(budget);
        }

        private void open() {
            remaining.set(Long.MAX_VALUE);
        }
    }

    /**
     * Supplies a restore consumer that honours the instance's {@link RestoreGate}. The restore consumer, unlike the
     * main consumer under the streams group protocol, is taken from the client supplier.
     */
    private static final class GatedRestoreClientSupplier implements KafkaClientSupplier {
        private final KafkaClientSupplier delegate = new DefaultKafkaClientSupplier();
        private final RestoreGate gate;

        private GatedRestoreClientSupplier(final RestoreGate gate) {
            this.gate = gate;
        }

        @Override
        public Admin getAdmin(final Map<String, Object> config) {
            return delegate.getAdmin(config);
        }

        @Override
        public Producer<byte[], byte[]> getProducer(final Map<String, Object> config) {
            return delegate.getProducer(config);
        }

        @Override
        public Consumer<byte[], byte[]> getConsumer(final Map<String, Object> config) {
            return delegate.getConsumer(config);
        }

        @Override
        public Consumer<byte[], byte[]> getRestoreConsumer(final Map<String, Object> config) {
            final Map<String, Object> gatedConfig = new HashMap<>(config);
            // One record per poll, so that a poll never overshoots the gate's budget.
            gatedConfig.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1);
            return new GatedRestoreConsumer(gatedConfig, gate);
        }

        @Override
        public Consumer<byte[], byte[]> getGlobalConsumer(final Map<String, Object> config) {
            return delegate.getGlobalConsumer(config);
        }
    }

    /**
     * Withholds records by not polling at all, rather than by pausing partitions, which the changelog reader resumes on
     * its own.
     */
    private static final class GatedRestoreConsumer extends KafkaConsumer<byte[], byte[]> {
        private final RestoreGate gate;

        private GatedRestoreConsumer(final Map<String, Object> config, final RestoreGate gate) {
            super(config, new ByteArrayDeserializer(), new ByteArrayDeserializer());
            this.gate = gate;
        }

        @Override
        public ConsumerRecords<byte[], byte[]> poll(final Duration timeout) {
            if (gate.remaining.get() <= 0) {
                Utils.sleep(Math.min(timeout.toMillis(), 10L));
                return ConsumerRecords.empty();
            }
            final ConsumerRecords<byte[], byte[]> records = super.poll(timeout);
            gate.remaining.addAndGet(-records.count());
            gate.delivered.addAndGet(records.count());
            return records;
        }
    }
}
