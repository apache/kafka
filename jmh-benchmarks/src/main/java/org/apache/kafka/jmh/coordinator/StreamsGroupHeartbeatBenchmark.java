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
package org.apache.kafka.jmh.coordinator;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.network.ListenerName;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.record.internal.RecordBatch;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.requests.RequestHeader;
import org.apache.kafka.common.security.auth.KafkaPrincipal;
import org.apache.kafka.common.security.auth.SecurityProtocol;
import org.apache.kafka.common.utils.MockTime;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.common.runtime.CoordinatorTimer;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorShard;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.group.streams.AssignmentRefinerImpl;
import org.apache.kafka.coordinator.group.streams.NoOpAssignmentRefiner;
import org.apache.kafka.coordinator.group.streams.StreamsGroup;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.timeline.SnapshotRegistry;

import com.yammer.metrics.core.MetricsRegistry;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Benchmarks how the group coordinator processes streams group heartbeats, and how it replays the records of a group.
 * <ul>
 *     <li>{@code stableHeartbeat}: a heartbeat of a member of a stable group that has nothing to report.</li>
 *     <li>{@code taskOffsetsHeartbeat}: heartbeats of the members of a stable group, one in every
 *     {@code heartbeatsPerTaskOffsetsReport} of which reports the offsets of the member's standby tasks, as a client
 *     does every {@code group.streams.task.offset.interval.ms}. {@code 1} reports on every heartbeat; {@code 12} is
 *     the default ratio of that interval (60 s) to {@code group.streams.heartbeat.interval.ms} (5 s). The members
 *     report in different rounds, like clients whose timers are not aligned. A member reports no offsets for an
 *     active task it processes, so without standby replicas the report is empty.</li>
 *     <li>{@code replayGroupRecords}: replaying all records that built the stable group into a new coordinator, as
 *     when the coordinator loads the group.</li>
 * </ul>
 * The group is built through the coordinator itself: every member joins with a heartbeat, the initial rebalance
 * computes the target assignment for all of them at once, and the members heartbeat, acknowledging the tasks they are
 * assigned, until a whole round of heartbeats writes no records. There is one member per process, and the topology is
 * one stateful subtopology with {@code tasksPerMember} active tasks per member and {@code standbyReplicas} standby
 * replicas of each. {@code refiner} selects the assignment refiner: {@code noop}, the default of the coordinator, or
 * {@code warmup}, the {@link AssignmentRefinerImpl}.
 */
@Fork(value = 1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Threads(1)
public class StreamsGroupHeartbeatBenchmark {

    private static final String GROUP_ID = "group";
    private static final String SUBTOPOLOGY_ID = "0";
    private static final String SOURCE_TOPIC = "input";
    private static final String CHANGELOG_TOPIC = "group-store-changelog";
    private static final int MAX_SETTLING_ROUNDS = 20;

    @State(Scope.Benchmark)
    public static class StableGroup {

        @Param({"100", "1000", "4000"})
        int members;

        @Param({"5"})
        int tasksPerMember;

        @Param({"0", "1", "2", "5"})
        int standbyReplicas;

        @Param({"noop"})
        String refiner;

        private Coordinator coordinator;
        private List<StreamsGroupHeartbeatRequestData> heartbeats;
        private List<StreamsGroupHeartbeatRequestData> taskOffsetsHeartbeats;
        private int next;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            coordinator = new Coordinator(new GroupShape(members, tasksPerMember, standbyReplicas, refiner));
            List<SimulatedMember> settled = coordinator.settleGroup();

            heartbeats = new ArrayList<>(settled.size());
            taskOffsetsHeartbeats = new ArrayList<>(settled.size());
            for (SimulatedMember member : settled) {
                heartbeats.add(coordinator.heartbeatWritingNoRecords(member.emptyHeartbeat()));
                taskOffsetsHeartbeats.add(coordinator.heartbeatWritingNoRecords(member.taskOffsetsHeartbeat()));
            }
        }

        private StreamsGroupHeartbeatRequestData nextHeartbeat() {
            StreamsGroupHeartbeatRequestData heartbeat = heartbeats.get(next);
            next = (next + 1) % heartbeats.size();
            return heartbeat;
        }
    }

    @State(Scope.Benchmark)
    public static class TaskOffsetsReports {

        @Param({"1", "12"})
        int heartbeatsPerTaskOffsetsReport;

        private long numHeartbeats;

        /**
         * The next heartbeat of the members of the group, in rounds over all members. A member reports its task
         * offsets in every {@code heartbeatsPerTaskOffsetsReport}-th round, shifted by its index.
         */
        private StreamsGroupHeartbeatRequestData next(StableGroup group) {
            int member = (int) (numHeartbeats % group.members);
            long round = numHeartbeats / group.members;
            numHeartbeats++;
            return (round + member) % heartbeatsPerTaskOffsetsReport == 0
                ? group.taskOffsetsHeartbeats.get(member)
                : group.heartbeats.get(member);
        }
    }

    @State(Scope.Benchmark)
    public static class GroupRecords {

        @Param({"100", "1000", "4000"})
        int members;

        @Param({"5"})
        int tasksPerMember;

        @Param({"0", "1", "2", "5"})
        int standbyReplicas;

        private GroupShape shape;
        private List<CoordinatorRecord> records;
        private GroupCoordinatorShard shard;

        @Setup(Level.Trial)
        public void setup() throws Exception {
            shape = new GroupShape(members, tasksPerMember, standbyReplicas, "noop");
            Coordinator coordinator = new Coordinator(shape);
            coordinator.settleGroup();
            records = coordinator.replayedRecords;
        }

        @Setup(Level.Invocation)
        public void setupInvocation() {
            shard = newShard(shape, new MockTime(), new MapCoordinatorTimer());
        }
    }

    @Benchmark
    public CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> stableHeartbeat(StableGroup state) {
        return state.coordinator.heartbeat(state.nextHeartbeat());
    }

    @Benchmark
    public CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> taskOffsetsHeartbeat(
        StableGroup group,
        TaskOffsetsReports reports
    ) {
        return group.coordinator.heartbeat(reports.next(group));
    }

    @Benchmark
    public GroupCoordinatorShard replayGroupRecords(GroupRecords state) {
        long offset = 0L;
        for (CoordinatorRecord record : state.records) {
            state.shard.replay(offset++, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, record);
        }
        return state.shard;
    }

    /**
     * The shape of the benchmarked group.
     *
     * @param members         The number of members, each in its own process.
     * @param tasksPerMember  The number of active tasks per member.
     * @param standbyReplicas The number of standby replicas of each task.
     * @param refiner         The assignment refiner, {@code noop} or {@code warmup}.
     */
    private record GroupShape(int members, int tasksPerMember, int standbyReplicas, String refiner) {

        int numTasks() {
            return members * tasksPerMember;
        }

        int numStandbyTasks() {
            return numTasks() * Math.min(standbyReplicas, members - 1);
        }

        Class<?> refinerClass() {
            return switch (refiner) {
                case "noop" -> NoOpAssignmentRefiner.class;
                case "warmup" -> AssignmentRefinerImpl.class;
                default -> throw new IllegalArgumentException("Unknown refiner " + refiner + ".");
            };
        }
    }

    private static GroupCoordinatorShard newShard(GroupShape shape, MockTime time, MapCoordinatorTimer timer) {
        Map<String, Object> props = new HashMap<>();
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, shape.standbyReplicas());
        props.put(
            GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_CONFIG,
            Math.max(shape.standbyReplicas(), GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_DEFAULT)
        );
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_REFINER_CLASS_CONFIG, shape.refinerClass().getName());
        GroupCoordinatorConfig config = GroupCoordinatorConfig.fromProps(props);
        GroupConfigManager configManager = new GroupConfigManager(config, ShareGroupConfig.fromProps(props));
        LogContext logContext = new LogContext();
        return new GroupCoordinatorShard.Builder(config, configManager)
            .withAuthorizerPlugin(Optional.empty())
            .withLogContext(logContext)
            .withSnapshotRegistry(new SnapshotRegistry(logContext))
            .withTime(time)
            .withTimer(timer)
            .withExecutor(new MockCoordinatorExecutor<>())
            .withCoordinatorMetrics(new GroupCoordinatorMetrics(new MetricsRegistry(), new Metrics()))
            .withTopicPartition(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0))
            .build();
    }

    /**
     * A group coordinator shard that replays the records of every request it handles, the way the coordinator
     * runtime does once they are written.
     */
    private static final class Coordinator {

        private final MockTime time = new MockTime();
        private final MapCoordinatorTimer timer = new MapCoordinatorTimer();
        private final GroupShape shape;
        private final GroupCoordinatorShard shard;
        private final RequestContext context = new RequestContext(
            new RequestHeader(ApiKeys.STREAMS_GROUP_HEARTBEAT, ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion(), "client", 0),
            "1",
            InetAddress.getLoopbackAddress(),
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );
        private final List<CoordinatorRecord> replayedRecords = new ArrayList<>();
        private long nextOffset = 0L;

        Coordinator(GroupShape shape) {
            this.shape = shape;
            this.shard = newShard(shape, time, timer);
            CoordinatorMetadataImage image = new MetadataImageBuilder()
                .addTopic(Uuid.randomUuid(), SOURCE_TOPIC, shape.numTasks())
                .addTopic(Uuid.randomUuid(), CHANGELOG_TOPIC, shape.numTasks())
                .buildCoordinatorMetadataImage();
            shard.onLoaded(image);
        }

        CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> heartbeat(
            StreamsGroupHeartbeatRequestData request
        ) {
            CoordinatorResult<StreamsGroupHeartbeatResult, CoordinatorRecord> result =
                shard.streamsGroupHeartbeat(context, request);
            replay(result.records());
            return result;
        }

        StreamsGroupHeartbeatRequestData heartbeatWritingNoRecords(StreamsGroupHeartbeatRequestData request) {
            if (!heartbeat(request).records().isEmpty()) {
                throw new IllegalStateException("A heartbeat of the settled group wrote records.");
            }
            return request;
        }

        private void replay(List<CoordinatorRecord> records) {
            for (CoordinatorRecord record : records) {
                shard.replay(nextOffset++, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, record);
                replayedRecords.add(record);
            }
        }

        /**
         * Joins the members, runs the initial rebalance, and lets the members heartbeat until a whole round of
         * heartbeats writes no records.
         *
         * @return The members of the settled group.
         */
        List<SimulatedMember> settleGroup() {
            StreamsGroupHeartbeatRequestData.Topology topology = new StreamsGroupHeartbeatRequestData.Topology()
                .setEpoch(0)
                .setSubtopologies(List.of(new StreamsGroupHeartbeatRequestData.Subtopology()
                    .setSubtopologyId(SUBTOPOLOGY_ID)
                    .setSourceTopics(List.of(SOURCE_TOPIC))
                    .setStateChangelogTopics(List.of(new StreamsGroupHeartbeatRequestData.TopicInfo()
                        .setName(CHANGELOG_TOPIC)))));

            List<SimulatedMember> members = new ArrayList<>(shape.members());
            for (int i = 0; i < shape.members(); i++) {
                SimulatedMember member = new SimulatedMember(Uuid.randomUuid().toString());
                member.handle(heartbeat(new StreamsGroupHeartbeatRequestData()
                    .setGroupId(GROUP_ID)
                    .setMemberId(member.memberId)
                    .setMemberEpoch(0)
                    .setRebalanceTimeoutMs(60_000)
                    .setProcessId(Uuid.randomUuid().toString())
                    .setTopology(topology)
                    .setActiveTasks(List.of())
                    .setStandbyTasks(List.of())
                    .setWarmupTasks(List.of())).response().data());
                members.add(member);
            }

            // The joins are coalesced by the initial rebalance delay, so the assignor runs once for all members.
            time.sleep(GroupCoordinatorConfig.STREAMS_GROUP_INITIAL_REBALANCE_DELAY_MS_DEFAULT);
            replay(timer.fire(StreamsGroup.initialRebalanceTimeoutKey(GROUP_ID)));

            for (int round = 0; round < MAX_SETTLING_ROUNDS; round++) {
                int recordsBefore = replayedRecords.size();
                for (SimulatedMember member : members) {
                    member.handle(heartbeat(member.heartbeat()).response().data());
                }
                if (replayedRecords.size() == recordsBefore) {
                    int assignedActiveTasks = 0;
                    int assignedStandbyTasks = 0;
                    for (SimulatedMember member : members) {
                        assignedActiveTasks += numTasks(member.activeTasks);
                        assignedStandbyTasks += numTasks(member.standbyTasks);
                    }
                    if (assignedActiveTasks != shape.numTasks() || assignedStandbyTasks != shape.numStandbyTasks()) {
                        throw new IllegalStateException("The settled group was assigned " + assignedActiveTasks +
                            " active and " + assignedStandbyTasks + " standby tasks, but expected " + shape.numTasks() +
                            " and " + shape.numStandbyTasks() + ".");
                    }
                    String groupState = shard.streamsGroupDescribe(List.of(GROUP_ID), SnapshotRegistry.LATEST_EPOCH)
                        .describedGroups().get(0).groupState();
                    if (!groupState.equals(StreamsGroup.StreamsGroupState.STABLE.toString())) {
                        throw new IllegalStateException("The settled group is " + groupState + ", not stable.");
                    }
                    return members;
                }
            }
            throw new IllegalStateException("The group did not settle within " + MAX_SETTLING_ROUNDS + " rounds.");
        }
    }

    private static int numTasks(List<StreamsGroupHeartbeatRequestData.TaskIds> taskIds) {
        int numTasks = 0;
        for (StreamsGroupHeartbeatRequestData.TaskIds ids : taskIds) {
            numTasks += ids.partitions().size();
        }
        return numTasks;
    }

    /**
     * A member that owns exactly the tasks of the latest assignment it was sent, so it acknowledges a revocation with
     * its next heartbeat.
     */
    private static final class SimulatedMember {

        private final String memberId;
        private int memberEpoch;
        private List<StreamsGroupHeartbeatRequestData.TaskIds> activeTasks = List.of();
        private List<StreamsGroupHeartbeatRequestData.TaskIds> standbyTasks = List.of();
        private List<StreamsGroupHeartbeatRequestData.TaskIds> warmupTasks = List.of();

        SimulatedMember(String memberId) {
            this.memberId = memberId;
        }

        /**
         * A heartbeat with nothing to report, as a client sends it between rebalances.
         */
        StreamsGroupHeartbeatRequestData emptyHeartbeat() {
            return new StreamsGroupHeartbeatRequestData()
                .setGroupId(GROUP_ID)
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch);
        }

        /**
         * A heartbeat that reports the changelog offsets and end offsets of the member's standby tasks, each lagging
         * behind its end offset.
         */
        StreamsGroupHeartbeatRequestData taskOffsetsHeartbeat() {
            List<StreamsGroupHeartbeatRequestData.TaskOffset> taskOffsets = new ArrayList<>();
            List<StreamsGroupHeartbeatRequestData.TaskOffset> taskEndOffsets = new ArrayList<>();
            for (StreamsGroupHeartbeatRequestData.TaskIds taskIds : standbyTasks) {
                for (int partition : taskIds.partitions()) {
                    taskOffsets.add(new StreamsGroupHeartbeatRequestData.TaskOffset()
                        .setSubtopologyId(taskIds.subtopologyId())
                        .setPartition(partition)
                        .setOffset(1_000L));
                    taskEndOffsets.add(new StreamsGroupHeartbeatRequestData.TaskOffset()
                        .setSubtopologyId(taskIds.subtopologyId())
                        .setPartition(partition)
                        .setOffset(2_000L));
                }
            }
            return emptyHeartbeat()
                .setTaskOffsets(taskOffsets)
                .setTaskEndOffsets(taskEndOffsets);
        }

        /**
         * A heartbeat that reports the tasks the member owns.
         */
        StreamsGroupHeartbeatRequestData heartbeat() {
            return new StreamsGroupHeartbeatRequestData()
                .setGroupId(GROUP_ID)
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setActiveTasks(activeTasks)
                .setStandbyTasks(standbyTasks)
                .setWarmupTasks(warmupTasks);
        }

        void handle(StreamsGroupHeartbeatResponseData response) {
            memberEpoch = response.memberEpoch();
            if (response.activeTasks() != null) {
                activeTasks = toRequestTaskIds(response.activeTasks());
                standbyTasks = toRequestTaskIds(response.standbyTasks());
                warmupTasks = toRequestTaskIds(response.warmupTasks());
            }
        }

        private static List<StreamsGroupHeartbeatRequestData.TaskIds> toRequestTaskIds(
            List<StreamsGroupHeartbeatResponseData.TaskIds> taskIds
        ) {
            List<StreamsGroupHeartbeatRequestData.TaskIds> requestTaskIds = new ArrayList<>(taskIds.size());
            for (StreamsGroupHeartbeatResponseData.TaskIds ids : taskIds) {
                requestTaskIds.add(new StreamsGroupHeartbeatRequestData.TaskIds()
                    .setSubtopologyId(ids.subtopologyId())
                    .setPartitions(ids.partitions()));
            }
            return requestTaskIds;
        }
    }

    /**
     * A coordinator timer that only keeps the scheduled operations by key, so that rescheduling a member's session
     * timeout on every heartbeat costs the same for any group size. Operations only run when {@link #fire} is called.
     */
    private static final class MapCoordinatorTimer implements CoordinatorTimer<CoordinatorRecord> {

        private final Map<String, TimeoutOperation<CoordinatorRecord>> operations = new HashMap<>();

        @Override
        public void schedule(
            String key,
            long delay,
            TimeUnit unit,
            boolean retry,
            TimeoutOperation<CoordinatorRecord> operation
        ) {
            operations.put(key, operation);
        }

        @Override
        public void schedule(
            String key,
            long delay,
            TimeUnit unit,
            boolean retry,
            long retryBackoff,
            TimeoutOperation<CoordinatorRecord> operation
        ) {
            operations.put(key, operation);
        }

        @Override
        public void scheduleIfAbsent(
            String key,
            long delay,
            TimeUnit unit,
            boolean retry,
            TimeoutOperation<CoordinatorRecord> operation
        ) {
            operations.putIfAbsent(key, operation);
        }

        @Override
        public void cancel(String key) {
            operations.remove(key);
        }

        @Override
        public boolean isScheduled(String key) {
            return operations.containsKey(key);
        }

        List<CoordinatorRecord> fire(String key) {
            TimeoutOperation<CoordinatorRecord> operation = operations.remove(key);
            if (operation == null) {
                throw new IllegalStateException("No operation is scheduled for " + key + ".");
            }
            return operation.generateRecords().records();
        }
    }
}
