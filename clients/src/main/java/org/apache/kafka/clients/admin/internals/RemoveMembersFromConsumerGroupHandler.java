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

    @Override
    public LeaveGroupRequest.Builder buildRequest(int coordinatorId, Set<CoordinatorKey> keys) {
        return new LeaveGroupRequest.Builder(groupId.idValue, members);
    }

    @Override
    public ApiResult<CoordinatorKey, Map<MemberIdentity, Errors>> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        final LeaveGroupResponse response = (LeaveGroupResponse) abstractResponse;
        Map<CoordinatorKey, Map<MemberIdentity, Errors>> completed = new HashMap<>();
        Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        List<CoordinatorKey> unmapped = new ArrayList<>();

        final Errors error = Errors.forCode(response.data().errorCode());
        if (error != Errors.NONE) {
            handleError(groupId, error, failed, unmapped);
        } else {
            final Map<MemberIdentity, Errors> memberErrors = new HashMap<>();
            for (MemberResponse memberResponse : response.memberResponses()) {
                memberErrors.put(new MemberIdentity()
                                     .setMemberId(memberResponse.memberId())
                                     .setGroupInstanceId(memberResponse.groupInstanceId()),
                                 Errors.forCode(memberResponse.errorCode()));

            }
            completed.put(groupId, memberErrors);
        }
        return new ApiResult<>(completed, failed, unmapped);
    }

    private void handleError(
        CoordinatorKey groupId,
        Errors error, Map<CoordinatorKey,
        Throwable> failed,
        List<CoordinatorKey> unmapped
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
                log.error("Received authorization failure for group {} in `LeaveGroup` response", groupId,
                        error.exception());
                failed.put(groupId, error.exception());
                break;
            case COORDINATOR_LOAD_IN_PROGRESS:
            case COORDINATOR_NOT_AVAILABLE:
                break;
            case NOT_COORDINATOR:
                log.debug("LeaveGroup request for group {} returned error {}. Will retry",
                        groupId, error);
                unmapped.add(groupId);
                break;
            default:
                log.error("Received unexpected error for group {} in `LeaveGroup` response",
                        groupId, error.exception());
                failed.put(groupId, error.exception(
                        "Received unexpected error for group " + groupId + " in `LeaveGroup` response"));
                break;
        }
    }

}
