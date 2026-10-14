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
import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
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
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorExecutor;
import org.apache.kafka.coordinator.common.runtime.MockCoordinatorTimer;
import org.apache.kafka.coordinator.group.GroupConfigManager;
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig;
import org.apache.kafka.coordinator.group.GroupCoordinatorShard;
import org.apache.kafka.coordinator.group.Utils;
import org.apache.kafka.coordinator.group.api.streams.assignor.GroupAssignment;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.modern.share.ShareGroupConfig;
import org.apache.kafka.coordinator.group.streams.AssignmentRefiner;
import org.apache.kafka.coordinator.group.streams.AssignmentRefinerImpl;
import org.apache.kafka.coordinator.group.streams.MemberTaskOffsets;
import org.apache.kafka.coordinator.group.streams.NoOpAssignmentRefiner;
import org.apache.kafka.coordinator.group.streams.StreamsCoordinatorRecordHelpers;
import org.apache.kafka.coordinator.group.streams.StreamsGroup.StreamsGroupState;
import org.apache.kafka.coordinator.group.streams.StreamsGroupMember;
import org.apache.kafka.coordinator.group.streams.StreamsTopology;
import org.apache.kafka.coordinator.group.streams.TasksTuple;
import org.apache.kafka.coordinator.group.streams.TopologyMetadata;
import org.apache.kafka.coordinator.group.streams.assignor.AssignmentConfigsImpl;
import org.apache.kafka.coordinator.group.streams.assignor.StickyTaskAssignor;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.apache.kafka.jmh.assignor.AssignorBenchmarkUtils;
import org.apache.kafka.jmh.assignor.StreamsAssignorBenchmarkUtils;
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
import org.openjdk.jmh.infra.Blackhole;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * Measures one streams group heartbeat as the group coordinator handles it: everything the coordinator shard runs for
 * a heartbeat of an existing member, from looking up the group to building the response and the records to write.
 * This covers the refiner and the reconciler together with the work around them, such as the metadata hash, the
 * comparison of the member's assignment with its intermediate assignment, and the records of a group epoch bump.
 * <p>
 * The group is loaded into the shard by replaying its records, then every member sends one heartbeat reporting its
 * restore progress as the lag picture says. The current assignment is set up as in AssignmentRefinerBenchmark: the
 * target assignment comes from the sticky assignor and a fraction of the stateful active tasks still runs on a member
 * of another process. In a STABLE group every member is reconciled to the target assignment epoch, so a heartbeat
 * derives the intermediate assignment afresh. In a RECONCILING group the target assignment epoch is one ahead of the
 * members, so a heartbeat reuses the intermediate assignment of that epoch, which the setup heartbeats derived, and
 * reconciles the member toward it.
 * <p>
 * Each invocation is a heartbeat of the next member, round-robin, without task offsets or owned tasks, so a score is
 * the mean over the members. The records a heartbeat returns are not replayed, so every heartbeat finds the group in
 * the same state; a heartbeat that bumps the group epoch does so on every round. Since the group has not bumped its
 * epoch since it was loaded, every heartbeat also recomputes the metadata hash, as after a coordinator failover.
 * <p>
 * The full grid is large; narrow it with -p, for example -p memberCount=1000 -p groupState=STABLE.
 */
@State(Scope.Benchmark)
@Fork(value = 1)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
public class StreamsGroupHeartbeatBenchmark {

    private static final String GROUP_ID = "streams-group";

    private static final String CLIENT_ID = "streams-client";

    private static final InetAddress CLIENT_ADDRESS = InetAddress.getLoopbackAddress();

    private static final int TOPOLOGY_EPOCH = 0;

    /**
     * The epoch of the members, which also is the group epoch and target assignment epoch of a STABLE group.
     */
    private static final int MEMBER_EPOCH = 10;

    /**
     * The default of acceptable.recovery.lag.
     */
    private static final long ACCEPTABLE_RECOVERY_LAG = 10_000L;

    public enum Refiner {
        NO_OP(NoOpAssignmentRefiner.class),
        IMPL(AssignmentRefinerImpl.class);

        private final Class<? extends AssignmentRefiner> refinerClass;

        Refiner(Class<? extends AssignmentRefiner> refinerClass) {
            this.refinerClass = refinerClass;
        }
    }

    @Param({"100", "1000"})
    private int memberCount;

    @Param({"1", "50"})
    private int membersPerProcess;

    @Param({"10", "100"})
    private int subtopologyCount;

    @Param({"100"})
    private int partitionCount;

    @Param({"1"})
    private int standbyReplicas;

    @Param({"2"})
    private int numWarmupReplicas;

    @Param({"0.0", "0.01", "0.5", "1.0"})
    private double migratingTaskFraction;

    @Param({"MIXED"})
    private StreamsAssignorBenchmarkUtils.LagPicture lagPicture;

    @Param({"STABLE", "RECONCILING"})
    private StreamsGroupState groupState;

    @Param({"NO_OP", "IMPL"})
    private Refiner refiner;

    private GroupCoordinatorShard coordinatorShard;

    private RequestContext context;

    private List<StreamsGroupHeartbeatRequestData> heartbeats;

    private int nextHeartbeat;

    @Setup(Level.Trial)
    public void setup() {
        if (groupState != StreamsGroupState.STABLE && groupState != StreamsGroupState.RECONCILING) {
            throw new IllegalArgumentException("Unsupported group state " + groupState + ".");
        }
        int assignmentEpoch = groupState == StreamsGroupState.STABLE ? MEMBER_EPOCH : MEMBER_EPOCH + 1;
        AssignmentConfigsImpl assignmentConfigs = AssignmentConfigsImpl.DEFAULT.withNumStandbyReplicas(standbyReplicas);

        List<String> sourceTopicNames = AssignorBenchmarkUtils.createTopicNames(subtopologyCount);
        SortedMap<String, ConfiguredSubtopology> subtopologyMap =
            StreamsAssignorBenchmarkUtils.createSubtopologyMap(partitionCount, sourceTopicNames);
        StreamsGroupTopologyValue topology = createTopology(subtopologyMap);
        List<String> allTopicNames = new ArrayList<>(StreamsTopology.fromRecord(topology).requiredTopics());
        CoordinatorMetadataImage metadataImage = AssignorBenchmarkUtils.createMetadataImage(allTopicNames, partitionCount);

        context = new RequestContext(
            new RequestHeader(ApiKeys.STREAMS_GROUP_HEARTBEAT, ApiKeys.STREAMS_GROUP_HEARTBEAT.latestVersion(), CLIENT_ID, 0),
            "1",
            CLIENT_ADDRESS,
            KafkaPrincipal.ANONYMOUS,
            ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT),
            SecurityProtocol.PLAINTEXT,
            ClientInformation.EMPTY,
            false
        );

        // The members as their heartbeats leave them, so that a heartbeat does not change their metadata.
        Map<String, StreamsGroupMember> newMembers = new HashMap<>();
        StreamsAssignorBenchmarkUtils.createStreamsMembers(memberCount, membersPerProcess).forEach((memberId, member) ->
            newMembers.put(memberId, new StreamsGroupMember.Builder(member)
                .setMemberEpoch(MEMBER_EPOCH)
                .setPreviousMemberEpoch(MEMBER_EPOCH - 1)
                .setTopologyEpoch(TOPOLOGY_EPOCH)
                .setClientId(CLIENT_ID)
                .setClientHost(CLIENT_ADDRESS.toString())
                .build()));

        GroupAssignment groupAssignment = new StickyTaskAssignor().assign(
            StreamsAssignorBenchmarkUtils.createGroupSpec(newMembers, assignmentConfigs, Map.of()),
            new TopologyMetadata(metadataImage, subtopologyMap)
        );
        Map<String, TasksTuple> targetAssignment = new HashMap<>();
        groupAssignment.members().forEach((memberId, memberAssignment) -> targetAssignment.put(
            memberId,
            new TasksTuple(memberAssignment.activeTasks(), memberAssignment.standbyTasks(), Map.of())
        ));

        Map<String, StreamsGroupMember> members = StreamsAssignorBenchmarkUtils.divergeAssignment(
            newMembers,
            targetAssignment,
            subtopologyMap,
            migratingTaskFraction,
            numWarmupReplicas
        );
        Map<String, MemberTaskOffsets> taskOffsets = StreamsAssignorBenchmarkUtils.createMemberTaskOffsets(
            members,
            targetAssignment,
            subtopologyMap,
            lagPicture,
            ACCEPTABLE_RECOVERY_LAG
        );

        Map<String, Long> topicHashes = new HashMap<>();
        allTopicNames.forEach(topicName -> topicHashes.put(topicName, Utils.computeTopicHash(topicName, metadataImage)));

        List<CoordinatorRecord> records = new ArrayList<>();
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTopologyRecord(GROUP_ID, topology));
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupMetadataRecord(
            GROUP_ID,
            assignmentEpoch,
            Utils.computeGroupHash(topicHashes),
            TOPOLOGY_EPOCH,
            assignmentConfigs,
            -1,
            -1
        ));
        members.values().forEach(member ->
            records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupMemberRecord(GROUP_ID, member)));
        targetAssignment.forEach((memberId, assignment) ->
            records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentRecord(GROUP_ID, memberId, assignment)));
        records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupTargetAssignmentMetadataRecord(GROUP_ID, assignmentEpoch, 0L));
        members.values().forEach(member ->
            records.add(StreamsCoordinatorRecordHelpers.newStreamsGroupCurrentAssignmentRecord(GROUP_ID, member)));

        coordinatorShard = createCoordinatorShard();
        for (int offset = 0; offset < records.size(); offset++) {
            coordinatorShard.replay(offset, RecordBatch.NO_PRODUCER_ID, RecordBatch.NO_PRODUCER_EPOCH, records.get(offset));
        }
        coordinatorShard.onLoaded(metadataImage);

        // Every member reports its restore progress once, which the coordinator keeps in memory.
        List<String> memberIds = new ArrayList<>(members.keySet());
        memberIds.sort(null);
        for (String memberId : memberIds) {
            MemberTaskOffsets offsets = taskOffsets.get(memberId);
            StreamsGroupHeartbeatRequestData heartbeat = createHeartbeat(memberId);
            if (offsets != null) {
                heartbeat
                    .setTaskOffsets(toTaskOffsets(offsets.taskOffsets()))
                    .setTaskEndOffsets(toTaskOffsets(offsets.taskEndOffsets()));
            }
            coordinatorShard.streamsGroupHeartbeat(context, heartbeat);
        }

        heartbeats = new ArrayList<>();
        memberIds.forEach(memberId -> heartbeats.add(createHeartbeat(memberId)));
        nextHeartbeat = 0;
    }

    @Benchmark
    @Threads(1)
    public void heartbeat(Blackhole blackhole) {
        StreamsGroupHeartbeatRequestData heartbeat = heartbeats.get(nextHeartbeat);
        nextHeartbeat = (nextHeartbeat + 1) % heartbeats.size();
        blackhole.consume(coordinatorShard.streamsGroupHeartbeat(context, heartbeat));
    }

    private GroupCoordinatorShard createCoordinatorShard() {
        Map<String, Object> props = new HashMap<>();
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_ASSIGNMENT_REFINER_CLASS_CONFIG, refiner.refinerClass.getName());
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_CONFIG, standbyReplicas);
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_CONFIG,
            Math.max(standbyReplicas, GroupCoordinatorConfig.STREAMS_GROUP_MAX_STANDBY_REPLICAS_DEFAULT));
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_NUM_WARMUP_REPLICAS_CONFIG, numWarmupReplicas);
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_MAX_WARMUP_REPLICAS_CONFIG,
            Math.max(numWarmupReplicas, GroupCoordinatorConfig.STREAMS_GROUP_MAX_WARMUP_REPLICAS_DEFAULT));
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_ACCEPTABLE_RECOVERY_LAG_CONFIG, ACCEPTABLE_RECOVERY_LAG);
        props.put(GroupCoordinatorConfig.STREAMS_GROUP_MAX_SIZE_CONFIG, Integer.MAX_VALUE);
        GroupCoordinatorConfig config = GroupCoordinatorConfig.fromProps(props);
        GroupConfigManager configManager = new GroupConfigManager(config, ShareGroupConfig.fromProps(props));

        MockTime time = new MockTime();
        LogContext logContext = new LogContext();
        return new GroupCoordinatorShard.Builder(config, configManager)
            .withAuthorizerPlugin(Optional.empty())
            .withLogContext(logContext)
            .withSnapshotRegistry(new SnapshotRegistry(logContext))
            .withTime(time)
            .withTimer(new MockCoordinatorTimer<>(time))
            .withExecutor(new MockCoordinatorExecutor<>())
            .withCoordinatorMetrics(new GroupCoordinatorMetrics(new MetricsRegistry(), new Metrics()))
            .withTopicPartition(new TopicPartition(Topic.GROUP_METADATA_TOPIC_NAME, 0))
            .build();
    }

    /**
     * Creates the topology of the subtopologies: each one reads its source topic, and a stateful one writes its
     * changelog topic.
     */
    private static StreamsGroupTopologyValue createTopology(SortedMap<String, ConfiguredSubtopology> subtopologyMap) {
        List<StreamsGroupTopologyValue.Subtopology> subtopologies = new ArrayList<>();
        subtopologyMap.forEach((subtopologyId, subtopology) -> subtopologies.add(
            new StreamsGroupTopologyValue.Subtopology()
                .setSubtopologyId(subtopologyId)
                .setSourceTopics(new ArrayList<>(subtopology.sourceTopics()))
                .setStateChangelogTopics(subtopology.stateChangelogTopics().keySet().stream()
                    .map(changelogTopic -> new StreamsGroupTopologyValue.TopicInfo().setName(changelogTopic))
                    .toList())));
        return new StreamsGroupTopologyValue()
            .setEpoch(TOPOLOGY_EPOCH)
            .setSubtopologies(subtopologies);
    }

    private static StreamsGroupHeartbeatRequestData createHeartbeat(String memberId) {
        return new StreamsGroupHeartbeatRequestData()
            .setGroupId(GROUP_ID)
            .setMemberId(memberId)
            .setMemberEpoch(MEMBER_EPOCH);
    }

    private static List<StreamsGroupHeartbeatRequestData.TaskOffset> toTaskOffsets(Map<String, Map<Integer, Long>> offsets) {
        List<StreamsGroupHeartbeatRequestData.TaskOffset> taskOffsets = new ArrayList<>();
        offsets.forEach((subtopologyId, partitions) -> partitions.forEach((partition, offset) -> taskOffsets.add(
            new StreamsGroupHeartbeatRequestData.TaskOffset()
                .setSubtopologyId(subtopologyId)
                .setPartition(partition)
                .setOffset(offset))));
        return taskOffsets;
    }
}
