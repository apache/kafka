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

import org.apache.kafka.clients.admin.ClassicGroupDescription;
import org.apache.kafka.clients.admin.MemberAssignment;
import org.apache.kafka.clients.admin.MemberDescription;
import org.apache.kafka.clients.consumer.ConsumerPartitionAssignor.Assignment;
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol;
import org.apache.kafka.common.ClassicGroupState;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.acl.AclOperation;
import org.apache.kafka.common.message.DescribeGroupsRequestData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
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

public class DescribeClassicGroupsHandler extends AdminApiHandler.Batched<CoordinatorKey, ClassicGroupDescription> {

    private final boolean includeAuthorizedOperations;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public DescribeClassicGroupsHandler(
        boolean includeAuthorizedOperations,
        LogContext logContext
    ) {
        this.includeAuthorizedOperations = includeAuthorizedOperations;
        this.log = logContext.logger(DescribeConsumerGroupsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    private static Set<CoordinatorKey> buildKeySet(Collection<String> groupIds) {
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, ClassicGroupDescription> newFuture(Collection<String> groupIds) {
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    @Override
    public String apiName() {
        return "describeClassicGroups";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    @Override
    public DescribeGroupsRequest.Builder buildBatchedRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        List<String> groupIds = keys.stream().map(key -> {
            if (key.type != FindCoordinatorRequest.CoordinatorType.GROUP) {
                throw new IllegalArgumentException("Invalid group coordinator key " + key +
                    " when building `DescribeGroups` request");
            }
            return key.idValue;
        }).collect(Collectors.toList());
        DescribeGroupsRequestData data = new DescribeGroupsRequestData()
            .setGroups(groupIds)
            .setIncludeAuthorizedOperations(includeAuthorizedOperations);
        return new DescribeGroupsRequest.Builder(data);
    }

    @Override
    public ApiResult<CoordinatorKey, ClassicGroupDescription> handleResponse(
            Node coordinator,
            Set<CoordinatorKey> groupIds,
            AbstractResponse abstractResponse) {
        final DescribeGroupsResponse response = (DescribeGroupsResponse) abstractResponse;
        final Map<CoordinatorKey, ClassicGroupDescription> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        for (DescribeGroupsResponseData.DescribedGroup describedGroup : response.data().groups()) {
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(describedGroup.groupId());
            Errors error = Errors.forCode(describedGroup.errorCode());
            if (error != Errors.NONE) {
                handleError(groupIdKey, error, error.message(), failed, groupsToUnmap);
                continue;
            }

            final List<MemberDescription> memberDescriptions = new ArrayList<>(describedGroup.members().size());
            final Set<AclOperation> authorizedOperations = validAclOperations(describedGroup.authorizedOperations());

            final String protocolType = describedGroup.protocolType();
            final boolean isConsumerGroup = protocolType.equals(ConsumerProtocol.PROTOCOL_TYPE) || protocolType.isEmpty();
            describedGroup.members().forEach(groupMember -> {
                Set<TopicPartition> partitions = Collections.emptySet();
                if (isConsumerGroup && groupMember.memberAssignment().length > 0) {
                    // We can only deserialize the assignment for a classic consumer group
                    final Assignment assignment = ConsumerProtocol.deserializeAssignment(ByteBuffer.wrap(groupMember.memberAssignment()));
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
            });

            final ClassicGroupDescription classicGroupDescription =
                new ClassicGroupDescription(
                    groupIdKey.idValue,
                    protocolType,
                    describedGroup.protocolData(),
                    memberDescriptions,
                    ClassicGroupState.parse(describedGroup.groupState()),
                    coordinator,
                    authorizedOperations);
            completed.put(groupIdKey, classicGroupDescription);
        }

        return new ApiResult<>(completed, failed, List.copyOf(groupsToUnmap));
    }

    private void handleError(
            CoordinatorKey groupId,
            Errors error,
            String errorMsg,
            Map<CoordinatorKey, Throwable> failed,
            Set<CoordinatorKey> groupsToUnmap) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("`DescribeGroups` request for group id {} failed due to error {}.", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`DescribeGroups` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry.", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`DescribeGroups` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry.", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                log.error("`DescribeGroups` request for group id {} failed due to unexpected error {}.", groupId.idValue, error);
                failed.put(groupId, error.exception(errorMsg));
        }
    }
}
