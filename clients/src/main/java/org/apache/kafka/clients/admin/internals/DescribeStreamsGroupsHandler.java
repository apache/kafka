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
package org.apache.kafka.clients.admin.internals;

import org.apache.kafka.clients.admin.StreamsGroupDescription;
import org.apache.kafka.clients.admin.StreamsGroupMemberAssignment;
import org.apache.kafka.clients.admin.StreamsGroupMemberDescription;
import org.apache.kafka.clients.admin.StreamsGroupSubtopologyDescription;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.StreamsGroupDescribeRequestData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.requests.StreamsGroupDescribeRequest;
import org.apache.kafka.common.requests.StreamsGroupDescribeResponse;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.admin.internals.AdminUtils.validAclOperations;

public class DescribeStreamsGroupsHandler extends AdminApiHandler.Batched<CoordinatorKey, StreamsGroupDescription> {

    private final boolean includeAuthorizedOperations;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public DescribeStreamsGroupsHandler(
          boolean includeAuthorizedOperations,
          LogContext logContext) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        this.log = logContext.logger(DescribeStreamsGroupsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    private static Set<CoordinatorKey> buildKeySet(Collection<String> groupIds) {
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, StreamsGroupDescription> newFuture(Collection<String> groupIds) {
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    @Override
    public String apiName() {
        return "describeStreamsGroups";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public StreamsGroupDescribeRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        List<String> groupIds = keys.stream().map(key -> {
            if (key.type != CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeStreamsGroups` request");
            }
            return key.idValue;
        }).collect(Collectors.toList());
        StreamsGroupDescribeRequestData data = new StreamsGroupDescribeRequestData()
            .setGroupIds(groupIds)
            .setIncludeAuthorizedOperations(includeAuthorizedOperations);
        return new StreamsGroupDescribeRequest.Builder(data, true);
    }

    @Override
    public ApiResult<CoordinatorKey, StreamsGroupDescription> handleResponse(
            Node coordinator,
            Set<CoordinatorKey> groupIds,
            AbstractResponse abstractResponse) {
        final StreamsGroupDescribeResponse response = (StreamsGroupDescribeResponse) abstractResponse;
        final Map<CoordinatorKey, StreamsGroupDescription> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        for (StreamsGroupDescribeResponseData.DescribedGroup describedGroup : response.data().groups()) {
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            Errors error = Errors.forCode(describedGroup.errorCode());
            if (error != Errors.NONE) {
                handleError(groupIdKey, describedGroup, coordinator, error, describedGroup.errorMessage(), completed, failed, groupsToUnmap);
                continue;
            }
            if (describedGroup.topology() == null) {
                log.error("`DescribeStreamsGroups` response for group id {} is missing the topology information", groupIdKey.idValue);
                failed.put(groupIdKey, new IllegalStateException("Topology information is missing"));
                continue;
            }

            final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());

            final StreamsGroupDescription streamsGroupDescription = new StreamsGroupDescription(
                    describedGroup.groupId(),
                    describedGroup.groupEpoch(),
                    describedGroup.assignmentEpoch(),
                    describedGroup.topology().epoch(),
                    convertSubtopologies(describedGroup.topology().subtopologies()),
                    convertMembers(describedGroup.members()),
                    GroupState.parse(describedGroup.groupState()),
                    coordinator,
                    authorizedOperations
            );
            completed.put(groupIdKey, streamsGroupDescription);
        }

        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    private Collection<StreamsGroupMemberDescription> convertMembers(final List<StreamsGroupDescribeResponseData.Member> members) {
        final List<StreamsGroupMemberDescription> memberDescriptions = new ArrayList<>(members.size());
        members.forEach(groupMember ->
            memberDescriptions.add(new StreamsGroupMemberDescription(
                groupMember.memberId(),
                groupMember.memberEpoch(),
                Optional.ofNullable(groupMember.instanceId()),
                Optional.ofNullable(groupMember.rackId()),
                groupMember.clientId(),
                groupMember.clientHost(),
                groupMember.topologyEpoch(),
                groupMember.processId(),
                Optional.ofNullable(groupMember.userEndpoint()).map(this::convertEndpoint),
                convertClientTags(groupMember.clientTags()),
                convertTaskOffsets(groupMember.taskOffsets()),
                convertTaskOffsets(groupMember.taskEndOffsets()),
                convertAssignment(groupMember.assignment()),
                convertAssignment(groupMember.targetAssignment()),
                groupMember.isClassic()
            ))
        );
        return memberDescriptions;
    }

    private Collection<StreamsGroupSubtopologyDescription> convertSubtopologies(final List<StreamsGroupDescribeResponseData.Subtopology> subtopologies) {
        final List<StreamsGroupSubtopologyDescription> subtopologyDescriptions = new ArrayList<>(subtopologies.size());
        subtopologies.forEach(subtopology ->
            subtopologyDescriptions.add(new StreamsGroupSubtopologyDescription(
                subtopology.subtopologyId(),
                subtopology.sourceTopics(),
                subtopology.repartitionSinkTopics(),
                convertTopicInfos(subtopology.stateChangelogTopics()),
                convertTopicInfos(subtopology.repartitionSourceTopics())
            ))
        );
        return subtopologyDescriptions;
    }

    private Map<String, StreamsGroupSubtopologyDescription.TopicInfo> convertTopicInfos(final List<StreamsGroupDescribeResponseData.TopicInfo> topicInfos) {
        return topicInfos.stream().collect(Collectors.toMap(
            StreamsGroupDescribeResponseData.TopicInfo::name,
            topicInfo -> new StreamsGroupSubtopologyDescription.TopicInfo(
                topicInfo.partitions(),
                topicInfo.replicationFactor(),
                topicInfo.topicConfigs().stream().collect(Collectors.toMap(
                    StreamsGroupDescribeResponseData.KeyValue::key,
                    StreamsGroupDescribeResponseData.KeyValue::value
                ))
            )
        ));
    }

    private StreamsGroupMemberAssignment.TaskIds convertTaskIds(final StreamsGroupDescribeResponseData.TaskIds taskIds) {
        return new StreamsGroupMemberAssignment.TaskIds(
            taskIds.subtopologyId(),
            taskIds.partitions()
        );
    }

    private StreamsGroupMemberAssignment convertAssignment(final StreamsGroupDescribeResponseData.Assignment assignment) {
        return new StreamsGroupMemberAssignment(
            assignment.activeTasks().stream().map(this::convertTaskIds).collect(Collectors.toList()),
            assignment.standbyTasks().stream().map(this::convertTaskIds).collect(Collectors.toList()),
            assignment.warmupTasks().stream().map(this::convertTaskIds).collect(Collectors.toList())
        );
    }

    private List<StreamsGroupMemberDescription.TaskOffset> convertTaskOffsets(final List<StreamsGroupDescribeResponseData.TaskOffset> taskOffsets) {
        return taskOffsets.stream().map(taskOffset ->
            new StreamsGroupMemberDescription.TaskOffset(
                taskOffset.subtopologyId(),
                taskOffset.partition(),
                taskOffset.offset()
            )
        ).collect(Collectors.toList());
    }

    private Map<String, String> convertClientTags(final List<StreamsGroupDescribeResponseData.KeyValue> keyValues) {
        return keyValues.stream().collect(Collectors.toMap(
            StreamsGroupDescribeResponseData.KeyValue::key,
            StreamsGroupDescribeResponseData.KeyValue::value
        ));
    }

    private StreamsGroupMemberDescription.Endpoint convertEndpoint(final StreamsGroupDescribeResponseData.Endpoint endpoint) {
        return new StreamsGroupMemberDescription.Endpoint(endpoint.host(), endpoint.port());
    }


    private void handleError(
            CoordinatorKey groupId,
            StreamsGroupDescribeResponseData.DescribedGroup describedGroup,
            Node coordinator,
            Errors error,
            String errorMsg,
            Map<CoordinatorKey, StreamsGroupDescription> completed,
            Map<CoordinatorKey, Throwable> failed,
            Set<CoordinatorKey> groupsToUnmap) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("`DescribeStreamsGroups` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`DescribeStreamsGroups` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`DescribeStreamsGroups` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            case GROUP_ID_NOT_FOUND:
                // In order to maintain compatibility with describeConsumerGroups, an unknown group ID is
                // reported as a DEAD streams group, and the admin client operation did not fail
                log.debug("`DescribeStreamsGroups` request for group id {} failed because the group does not exist. {}",
                    groupId.idValue, errorMsg != null ? errorMsg : "");
                final StreamsGroupDescription streamsGroupDescription =
                    new StreamsGroupDescription(
                        groupId.idValue,
                        -1,
                        -1,
                        -1,
                        Collections.emptySet(),
                        Collections.emptySet(),
                        GroupState.DEAD,
                        coordinator,
                        validAclOperations(describedGroup.authorizedOperations()));
                completed.put(groupId, streamsGroupDescription);
                break;

            default:
                log.error("`DescribeStreamsGroups` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
        }
    }
}
