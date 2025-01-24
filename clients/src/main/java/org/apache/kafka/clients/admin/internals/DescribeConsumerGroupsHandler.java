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

import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.GroupState;
import org.apache.kafka.common.GroupType;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ConsumerGroupDescribeRequestData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroup;
import org.apache.kafka.common.message.DescribeGroupsResponseData.DescribedGroupMember;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.ConsumerGroupDescribeRequest;
import org.apache.kafka.common.requests.ConsumerGroupDescribeResponse;
import org.apache.kafka.common.requests.DescribeGroupsRequest;
import org.apache.kafka.common.requests.DescribeGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;

import org.slf4j.Logger;

import java.nio.ByteBuffer;
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

public class DescribeConsumerGroupsHandler implements AdminApiHandler<CoordinatorKey, ConsumerGroupDescription> {

    private final boolean includeAuthorizedOperations;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;
    private final Set<String> useClassicGroupApi;
    private final Map<String, String> groupIdNotFoundErrorMessages;

    public DescribeConsumerGroupsHandler(
        boolean includeAuthorizedOperations,
        LogContext logContext
    ) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        this.log = logContext.logger(DescribeConsumerGroupsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
        this.useClassicGroupApi = new HashSet<>();
        this.groupIdNotFoundErrorMessages = new HashMap<>();
    }

    private static Set<CoordinatorKey> buildKeySet(Collection<String> groupIds) {
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ConsumerGroupDescription> newFuture(
        Collection<String> groupIds
    ) {
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    @Override
    public String apiName() {
        return "describeConsumerGroups";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public Collection<RequestAndKeys<CoordinatorKey>> buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        Set<CoordinatorKey> newConsumerGroupKeys = new HashSet<>();
        Set<CoordinatorKey> oldConsumerGroupKeys = new HashSet<>();
        List<String> newConsumerGroupIds = new ArrayList<>();
        List<String> oldConsumerGroupIds = new ArrayList<>();

        keys.forEach(key -> {
            if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeGroups` request");
            }

            // By default, we always try using the new consumer group describe API.
            // If it fails, we fail back to using the classic group API.
            if (useClassicGroupApi.contains(key.idValue)) {
                oldConsumerGroupKeys.add(key);
                oldConsumerGroupIds.add(key.idValue);
            } else {
                newConsumerGroupKeys.add(key);
                newConsumerGroupIds.add(key.idValue);
            }
        });

        List<RequestAndKeys<CoordinatorKey>> requests = new ArrayList<>();
        if (!newConsumerGroupKeys.isEmpty()) {
            ConsumerGroupDescribeRequestData data = new ConsumerGroupDescribeRequestData()
                .setGroupIds(newConsumerGroupIds)
                .setIncludeAuthorizedOperations(includeAuthorizedOperations);
            requests.add(new RequestAndKeys<>(new ConsumerGroupDescribeRequest.Builder(data), newConsumerGroupKeys));
        }

        if (!oldConsumerGroupKeys.isEmpty()) {
            DescribeGroupsRequestData data = new DescribeGroupsRequestData()
                .setGroups(oldConsumerGroupIds)
                .setIncludeAuthorizedOperations(includeAuthorizedOperations);
            requests.add(new RequestAndKeys<>(new DescribeGroupsRequest.Builder(data), oldConsumerGroupKeys));
        }

        return requests;
    }

    @Override
    public ApiResult<CoordinatorKey, ConsumerGroupDescription> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        final Map<CoordinatorKey, ConsumerGroupDescription> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        if (abstractResponse instanceof DescribeGroupsResponse) {
            return handledClassicGroupResponse(
                coordinator,
                completed,
                failed,
                groupsToUnmap,
                (DescribeGroupsResponse) abstractResponse
            );
        } else if (abstractResponse instanceof ConsumerGroupDescribeResponse) {
            return handledConsumerGroupResponse(
                coordinator,
                completed,
                failed,
                groupsToUnmap,
                (ConsumerGroupDescribeResponse) abstractResponse
            );
        } else {
            throw new IllegalArgumentException("Received an unexpected response type.");
        }
    }

    @Override
    public Map<CoordinatorKey, Throwable> handleUnsupportedVersionException(
        int coordinator,
        UnsupportedVersionException exception,
        Set<CoordinatorKey> keys
    ) {
        Map<CoordinatorKey, Throwable> errors = new HashMap<>();

        keys.forEach(key -> {
            if (!useClassicGroupApi.add(key.idValue)) {
                // We already tried with the classic group API so we need to fail now.
                errors.put(key, exception);
            }
        });

        return errors;
    }

    private ApiResult<CoordinatorKey, ConsumerGroupDescription> handledConsumerGroupResponse(
        Node coordinator,
        Map<CoordinatorKey, ConsumerGroupDescription> completed,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        ConsumerGroupDescribeResponse response
    ) {
        for (ConsumerGroupDescribeResponseData.DescribedGroup describedGroup : response.data().groups()) {
            final CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            final Errors error = Errors.forCode(describedGroup.errorCode());
            if (error != Errors.NONE) {
                handleError(
                    groupIdKey,
                    error,
                    describedGroup.errorMessage(),
                    failed,
                    groupsToUnmap,
                    true
                );
                continue;
            }

            final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());
            final List<MemberDescription> memberDescriptions = new ArrayList<>(describedGroup.members().size());

            describedGroup.members().forEach(groupMember ->
                memberDescriptions.add(new MemberDescription(
                    groupMember.memberId(),
                    Optional.ofNullable(groupMember.instanceId()),
                    groupMember.clientId(),
                    groupMember.clientHost(),
                    new MemberAssignment(convertAssignment(groupMember.assignment())),
                    Optional.of(new MemberAssignment(convertAssignment(groupMember.targetAssignment()))),
                    Optional.of(groupMember.memberEpoch()),
                    groupMember.memberType() == -1 ? Optional.empty() : Optional.of(groupMember.memberType() == 1)
                ))
            );

            final ConsumerGroupDescription consumerGroupDescription =
                new ConsumerGroupDescription(
                    groupIdKey.idValue,
                    false,
                    memberDescriptions,
                    describedGroup.assignorName(),
                    GroupType.CONSUMER,
                    GroupState.parse(describedGroup.groupState()),
                    coordinator,
                    authorizedOperations,
                    Optional.of(describedGroup.groupEpoch()),
                    Optional.of(describedGroup.assignmentEpoch())
                );
            completed.put(groupIdKey, consumerGroupDescription);
        }

        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    private ApiResult<CoordinatorKey, ConsumerGroupDescription> handledClassicGroupResponse(
        Node coordinator,
        Map<CoordinatorKey, ConsumerGroupDescription> completed,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        DescribeGroupsResponse response
    ) {
        for (DescribedGroup describedGroup : response.data().groups()) {
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            Errors error = Errors.forCode(describedGroup.errorCode());
            if (error != Errors.NONE) {
                handleError(
                    groupIdKey,
                    error,
                    describedGroup.errorMessage(),
                    failed,
                    groupsToUnmap,
                    false
                );
                continue;
            }
            final String protocolType = describedGroup.protocolType();
            if (protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty()) {
                final List<DescribedGroupMember> members = describedGroup.members();
                final List<MemberDescription> memberDescriptions = new ArrayList<>(members.size());
                final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());
                for (DescribedGroupMember groupMember : members) {
                    Set<TopicPartition> partitions = Collections.emptySet();
                    if (groupMember.memberAssignment().length > 0) {
                        final Assignment assignment = ConsumerProtocol.
                            deserializeAssignment(ByteBuffer.wrap(groupMember.memberAssignment()));
                        partitions = new HashSet<>(assignment.partitions());
                    }
                    memberDescriptions.add(new MemberDescription(
                        groupMember.memberId(),
                        Optional.ofNullable(groupMember.groupInstanceId()),
                        groupMember.clientId(),
                        groupMember.clientHost(),
                        new MemberAssignment(partitions),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()));
                }
                final ConsumerGroupDescription consumerGroupDescription =
                    new ConsumerGroupDescription(groupIdKey.idValue, protocolType.isEmpty(),
                        memberDescriptions,
                        describedGroup.protocolData(),
                        GroupType.CLASSIC,
                        GroupState.parse(describedGroup.groupState()),
                        coordinator,
                        authorizedOperations,
                        Optional.empty(),
                        Optional.empty());
                completed.put(groupIdKey, consumerGroupDescription);
            } else {
                failed.put(groupIdKey, new IllegalArgumentException(
                    String.format("GroupId %s is not a consumer group (%s).",
                        groupIdKey.idValue, protocolType)));
            }
        }

        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    private Set<TopicPartition> convertAssignment(ConsumerGroupDescribeResponseData.Assignment assignment) {
        return assignment.topicPartitions().stream().flatMap(topic ->
            topic.partitions().stream().map(partition ->
                new TopicPartition(topic.topicName(), partition)
            )
        ).collect(Collectors.toSet());
    }

    private void handleError(
        CoordinatorKey groupId,
        Errors error,
        String errorMsg,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        boolean isConsumerGroupResponse
    ) {
        String apiName = isConsumerGroupResponse ? "ConsumerGroupDescribe" : "DescribeGroups";

        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("`{}` request for group id {} failed due to error {}.", apiName, groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`{}` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry.", apiName, groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`{}` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", apiName, groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            case UNSUPPORTED_VERSION:
                if (isConsumerGroupResponse) {
                    log.debug("`{}` request for group id {} failed because the API is not " +
                        "supported. Will retry with `DescribeGroups` API.", apiName, groupId.idValue);
                    useClassicGroupApi.add(groupId.idValue);
                } else {
                    log.error("`{}` request for group id {} failed because the `ConsumerGroupDescribe` API is not supported.",
                        apiName, groupId.idValue);
                    failed.put(groupId, error.exception(errorMsg));
                }
                break;

            case GROUP_ID_NOT_FOUND:
                if (isConsumerGroupResponse) {
                    log.debug("`{}` request for group id {} failed because the group is not " +
                        "a new consumer group. Will retry with `DescribeGroups` API. {}",
                        apiName, groupId.idValue, errorMsg != null ? errorMsg : "");
                    useClassicGroupApi.add(groupId.idValue);

                    // The error message from the ConsumerGroupDescribe API is more informative to the user
                    // than the error message from the classic group API. Capture it and use it if we get the
                    // same error code for the classic group API also.
                    groupIdNotFoundErrorMessages.put(groupId.idValue, errorMsg);
                } else {
                    log.debug("`{}` request for group id {} failed because the group does not exist. {}",
                        apiName, groupId.idValue, errorMsg != null ? errorMsg : "");
                    failed.put(groupId, error.exception(groupIdNotFoundErrorMessages.getOrDefault(groupId.idValue, errorMsg)));
                }
                break;

            default:
                log.error("`{}` request for group id {} failed due to unexpected error {}.", apiName, groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
        }
    }
}
