package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatResponseData;
import org.apache.kafka.common.requests.StreamsGroupHeartbeatRequest;
import org.apache.kafka.coordinator.common.runtime.CoordinatorMetadataImage;
import org.apache.kafka.coordinator.common.runtime.MetadataImageBuilder;
import org.apache.kafka.coordinator.group.streams.*;

import java.util.*;

import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.*;
import static org.apache.kafka.coordinator.group.GroupMetadataManagerTestContext.DEFAULT_PROCESS_ID;
import static org.apache.kafka.coordinator.group.Utils.computeGroupHash;
import static org.apache.kafka.coordinator.group.Utils.computeTopicHash;
import static org.apache.kafka.coordinator.group.streams.TaskAssignmentTestUtil.mkTasksTupleWithCommonEpoch;

class StreamsGroupTestUtil {

    static StreamsGroupMember.Builder streamsGroupMemberBuilderWithDefaults(String memberId) {
        return streamsGroupMemberBuilderWithDefaults(memberId, null);
    }

    static StreamsGroupMember.Builder streamsGroupMemberBuilderWithDefaults(String memberId, String instanceId) {
        return new StreamsGroupMember.Builder(memberId)
                .setMemberEpoch(1)
                .setPreviousMemberEpoch(0)
                .setState(org.apache.kafka.coordinator.group.streams.MemberState.STABLE)
                .setRackId(null)
                .setInstanceId(instanceId)
                .setRebalanceTimeoutMs(1500)
                .setAssignedTasks(TasksTupleWithEpochs.EMPTY)
                .setTasksPendingRevocation(TasksTupleWithEpochs.EMPTY)
                .setTopologyEpoch(0)
                .setClientTags(Map.of())
                .setClientId(DEFAULT_CLIENT_ID)
                .setClientHost(DEFAULT_CLIENT_ADDRESS.toString())
                .setProcessId(DEFAULT_PROCESS_ID)
                .setUserEndpoint(null);
    }

    /**
     * Returns the default assignment configurations that would be used by the system.
     * This matches what streamsGroupAssignmentConfigs() would return.
     */
    static Map<String, String> getDefaultAssignmentConfigs() {
        // Use the same default value as GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT
        return new TreeMap<>(Map.of(
                "num.standby.replicas", String.valueOf(GroupCoordinatorConfig.STREAMS_GROUP_NUM_STANDBY_REPLICAS_DEFAULT)
        ));
    }

    static List<StreamsGroupHeartbeatResponseData.TaskIds> mkResponseTasks(
            String subtopologyId,
            Integer... partitions
    ) {
        return List.of(
                new StreamsGroupHeartbeatResponseData.TaskIds()
                        .setSubtopologyId(subtopologyId)
                        .setPartitions(Arrays.asList(partitions))
        );
    }

    static final int DEFAULT_REBALANCE_TIMEOUT_MS = 1500;

    static StreamsTopicFixture streamsTopicFixture(String subtopologyId, String topicName, int partitions) {
        return new StreamsTopicFixture(subtopologyId, topicName, partitions);
    }

    static StreamsGroupHeartbeatRequestData staticHeartbeat(String groupId, String memberId, String instanceId, int memberEpoch) {
        return new StreamsGroupHeartbeatRequestData()
                .setGroupId(groupId)
                .setInstanceId(instanceId)
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch);
    }

    static StreamsGroupHeartbeatRequestData staticJoinHeartbeat(String groupId, String memberId, String instanceId, StreamsTopicFixture topic) {
        return staticHeartbeat(groupId, memberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setProcessId(DEFAULT_PROCESS_ID)
                .setRebalanceTimeoutMs(DEFAULT_REBALANCE_TIMEOUT_MS)
                .setTopology(topic.topology)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of());
    }

    static StreamsGroupHeartbeatResponseData staticLeaveResponse(String memberId, int leaveEpoch) {
        return new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(leaveEpoch)
                .setStatus(List.of());
    }


    static StreamsGroupHeartbeatResponseData staticLeaveResponseWithNullTasks(String memberId, int leaveEpoch) {
        return staticLeaveResponse(memberId, leaveEpoch)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null);
    }


    static class StreamsTopicFixture {
        private final String subtopologyId;
        private final String topicName;
        private final Uuid topicId;
        private final StreamsGroupHeartbeatRequestData.Topology topology;
        private final CoordinatorMetadataImage metadataImage;
        private final long metadataHash;

        private StreamsTopicFixture(
                String subtopologyId,
                String topicName,
                int partitions
        ) {
            this.subtopologyId = subtopologyId;
            this.topicName = topicName;
            this.topicId = Uuid.randomUuid();
            this.topology = new StreamsGroupHeartbeatRequestData.Topology()
                    .setSubtopologies(List.of(
                            new StreamsGroupHeartbeatRequestData.Subtopology()
                                    .setSubtopologyId(subtopologyId)
                                    .setSourceTopics(List.of(topicName))
                    ));
            this.metadataImage = new MetadataImageBuilder()
                    .addTopic(topicId, topicName, partitions)
                    .buildCoordinatorMetadataImage();
            this.metadataHash = computeGroupHash(Map.of(
                    topicName,
                    computeTopicHash(topicName, metadataImage)
            ));
        }

        public Map.Entry<String, Set<Integer>> tasks(Integer... partitions) {
            return TaskAssignmentTestUtil.mkTasks(subtopologyId, partitions);
        }

        public TasksTuple targetAssignment(Integer... partitions) {
            return TaskAssignmentTestUtil.mkTasksTuple(
                    TaskAssignmentTestUtil.TaskRole.ACTIVE,
                    tasks(partitions)
            );
        }

        public TasksTupleWithEpochs assignedTasks(
                int epoch,
                Integer... partitions
        ) {
            return mkTasksTupleWithCommonEpoch(
                    TaskAssignmentTestUtil.TaskRole.ACTIVE,
                    epoch,
                    tasks(partitions)
            );
        }
        
        public CoordinatorMetadataImage metadataImage() {
            return metadataImage;
        }
        
        public long metadataHash() {
            return metadataHash;
        }
        
        public StreamsGroupHeartbeatRequestData.Topology topology() {
            return topology;
        }

        public List<StreamsGroupHeartbeatResponseData.TaskIds> responseTasks(Integer... partitions) {
            return mkResponseTasks(subtopologyId, partitions);
        }

        public List<StreamsGroupHeartbeatRequestData.TaskIds> requestTasks(List<Integer> partitions) {
            return List.of(
                    new StreamsGroupHeartbeatRequestData.TaskIds()
                            .setSubtopologyId(subtopologyId)
                            .setPartitions(partitions)
            );
        }
    }

    static GroupMetadataManagerTestContext contextWithStreamsGroup(
            String groupId,
            int groupEpoch,
            StreamsTopicFixture topic,
            java.util.function.UnaryOperator<StreamsGroupBuilder> configureGroup
    ) {
        return contextWithStreamsGroup(
                groupId,
                groupEpoch,
                topic,
                new MockTaskAssignor("sticky"),
                configureGroup
        );
    }

    static GroupMetadataManagerTestContext contextWithStreamsGroup(
            String groupId,
            int groupEpoch,
            StreamsTopicFixture topic,
            MockTaskAssignor assignor,
            java.util.function.UnaryOperator<StreamsGroupBuilder> configureGroup
    ) {
        StreamsGroupBuilder group = new StreamsGroupBuilder(groupId, groupEpoch)
                .withTargetAssignmentEpoch(groupEpoch)
                .withTopology(StreamsTopology.fromHeartbeatRequest(topic.topology()))
                .withValidatedTopologyEpoch(0)
                .withMetadataHash(topic.metadataHash())
                .withLastAssignmentConfigs(getDefaultAssignmentConfigs());

        return new GroupMetadataManagerTestContext.Builder()
                .withStreamsGroupTaskAssignors(List.of(assignor))
                .withMetadataImage(topic.metadataImage())
                .withStreamsGroup(configureGroup.apply(group))
                .build();
    }

    static StreamsGroupHeartbeatResponseData heartbeatResponseWithActiveTasks(
            String memberId,
            int memberEpoch,
            StreamsTopicFixture topic,
            Integer... activeTasks
    ) {
        return heartbeatResponseWithActiveTasks(
                memberId,
                memberEpoch,
                topic.responseTasks(activeTasks)
        );
    }
    
    static StreamsGroupHeartbeatResponseData heartbeatResponseWithActiveTasks(
            String memberId,
            int memberEpoch,
            List<StreamsGroupHeartbeatResponseData.TaskIds> activeTasks
    ) {
        return new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setActiveTasks(activeTasks)
                .setWarmupTasks(List.of())
                .setStandbyTasks(List.of());
    }

    static StreamsGroupHeartbeatResponseData heartbeatResponseWithActiveTasks(
            String memberId,
            int memberEpoch,
            String subtopologyId,
            Integer... activeTasks
    ) {
        return heartbeatResponseWithActiveTasks(
                memberId,
                memberEpoch,
                mkResponseTasks(subtopologyId, activeTasks)
        );
    }


    static StreamsGroupHeartbeatResponseData heartbeatResponseWithNullTasks(
            String memberId,
            int memberEpoch
    ) {
        return new StreamsGroupHeartbeatResponseData()
                .setMemberId(memberId)
                .setMemberEpoch(memberEpoch)
                .setHeartbeatIntervalMs(5000)
                .setTaskOffsetIntervalMs(60000)
                .setActiveTasks(null)
                .setWarmupTasks(null)
                .setStandbyTasks(null);
    }

    static StreamsGroupHeartbeatRequestData staticJoinHeartbeat(
            String groupId,
            String memberId,
            String instanceId,
            String processId
    ) {
        return staticHeartbeat(groupId, memberId, instanceId, StreamsGroupHeartbeatRequest.JOIN_GROUP_MEMBER_EPOCH)
                .setProcessId(processId)
                .setActiveTasks(List.of())
                .setStandbyTasks(List.of())
                .setWarmupTasks(List.of());
    }
    
}