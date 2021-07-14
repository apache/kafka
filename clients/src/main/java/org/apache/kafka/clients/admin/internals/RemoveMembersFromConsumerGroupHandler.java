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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.LeaveGroupRequestData.MemberIdentity;
import org.apache.kafka.common.message.LeaveGroupResponseData.MemberResponse;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.LeaveGroupRequest;
import org.apache.kafka.common.requests.LeaveGroupResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class RemoveMembersFromConsumerGroupHandler implements AdminApiHandler<CoordinatorKey, Map<MemberIdentity, Errors>> {

    private final CoordinatorKey groupId;
    private final List<MemberIdentity> members;
    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public RemoveMembersFromConsumerGroupHandler(
        String groupId,
        List<MemberIdentity> members,
        LogContext logContext
    ) {
        this.groupId = CoordinatorKey.byGroupId(groupId);
        this.members = members;
        this.log = logContext.logger(RemoveMembersFromConsumerGroupHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    @Override
    public String apiName() {
        return "leaveGroup";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Map<MemberIdentity, Errors>> newFuture(
        String groupId
    ) {
        return AdminApiFuture.forKeys(Collections.singleton(CoordinatorKey.byGroupId(groupId)));
    }

    private void validateKeys(
        Set<CoordinatorKey> groupIds
    ) {
        if (!groupIds.equals(Collections.singleton(groupId))) {
            throw new IllegalArgumentException("Received unexpected group ids " + groupIds +
                " (expected only " + Collections.singleton(groupId) + ")");
        }
    }

    @Override
    public LeaveGroupRequest.Builder buildRequest(int coordinatorId, Set<CoordinatorKey> groupIds) {
        validateKeys(groupIds);
        return new LeaveGroupRequest.Builder(groupId.idValue, members);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        validateKeys(groupIds);

        final LeaveGroupResponse response = (LeaveGroupResponse) abstractResponse;
        final Map<CoordinatorKey, Map<MemberIdentity, Errors>> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();
        final Set<CoordinatorKey> groupsToRetry = new HashSet<>();

        final Errors error = response.topLevelError();
        if (error != Errors.NONE) {
            handleGroupError(groupId, error, failed, groupsToUnmap, groupsToRetry);
        } else {
            final Map<MemberIdentity, Errors> memberErrors = new HashMap<>();
            for (MemberResponse memberResponse : response.memberResponses()) {
                Errors memberError = Errors.forCode(memberResponse.errorCode());
                String memberId = memberResponse.memberId();

                memberErrors.put(new MemberIdentity()
                                     .setMemberId(memberId)
                                     .setGroupInstanceId(memberResponse.groupInstanceId()),
                    memberError);

            }
            completed.put(groupId, memberErrors);
        }

        if (groupsToUnmap.isEmpty() && groupsToRetry.isEmpty()) {
            return new ApiResult<>(
                completed,
                failed,
                Collections.emptyList()
            );
        } else {
            // retry the request, so don't send completed/failed results back
            return new ApiResult<>(
                Collections.emptyMap(),
                Collections.emptyMap(),
                new ArrayList<>(groupsToUnmap)
            );
        }
    }

    private void handleGroupError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap,
        Set<CoordinatorKey> groupsToRetry
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.debug("`LeaveGroup` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`LeaveGroup` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                groupsToRetry.add(groupId);
                break;
            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`LeaveGroup` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                final String unexpectedErrorMsg =
                    String.format("`LeaveGroup` request for group id %s failed due to unexpected error %s", groupId.idValue, error);
                log.error(unexpectedErrorMsg);
                failed.put(groupId, error.exception(unexpectedErrorMsg));
        }
    }

}