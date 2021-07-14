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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kafka.common.Node;
import org.apache.kafka.common.message.DeleteGroupsRequestData;
import org.apache.kafka.common.message.DeleteGroupsResponseData.DeletableGroupResult;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.AbstractResponse;
import org.apache.kafka.common.requests.DeleteGroupsRequest;
import org.apache.kafka.common.requests.DeleteGroupsResponse;
import org.apache.kafka.common.requests.FindCoordinatorRequest.CoordinatorType;
import org.apache.kafka.common.utils.LogContext;
import org.slf4j.Logger;

public class DeleteConsumerGroupsHandler extends AdminApiHandler.Batched<CoordinatorKey, Void> {

    private final Logger log;
    private final AdminApiLookupStrategy<CoordinatorKey> lookupStrategy;

    public DeleteConsumerGroupsHandler(
        LogContext logContext
    ) {
        this.log = logContext.logger(DeleteConsumerGroupsHandler.class);
        this.lookupStrategy = new CoordinatorStrategy(CoordinatorType.GROUP, logContext);
    }

    @Override
    public String apiName() {
        return "deleteConsumerGroups";
    }

    @Override
    public AdminApiLookupStrategy<CoordinatorKey> lookupStrategy() {
        return lookupStrategy;
    }

    public static AdminApiFuture.SimpleAdminApiFuture<CoordinatorKey, Void> newFuture(
        Collection<String> groupIds
    ) {
        return AdminApiFuture.forKeys(buildKeySet(groupIds));
    }

    private static Set<CoordinatorKey> buildKeySet(Collection<String> groupIds) {
        return groupIds.stream()
            .map(CoordinatorKey::byGroupId)
            .collect(Collectors.toSet());
    }

    @Override
    public DeleteGroupsRequest.Builder buildBatchedRequest(
        int coordinatorId,
        Set<CoordinatorKey> keys
    ) {
        List<String> groupIds = keys.stream().map(key -> key.idValue).collect(Collectors.toList());
        DeleteGroupsRequestData data = new DeleteGroupsRequestData()
            .setGroupsNames(groupIds);
        return new DeleteGroupsRequest.Builder(data);
    }

    @Override
    public ApiResult<CoordinatorKey, Void> handleResponse(
        Node coordinator,
        Set<CoordinatorKey> groupIds,
        AbstractResponse abstractResponse
    ) {
        final DeleteGroupsResponse response = (DeleteGroupsResponse) abstractResponse;
        final Map<CoordinatorKey, Void> completed = new HashMap<>();
        final Map<CoordinatorKey, Throwable> failed = new HashMap<>();
        final Set<CoordinatorKey> groupsToUnmap = new HashSet<>();

        for (DeletableGroupResult deletedGroup : response.data().results()) {
            CoordinatorKey groupIdKey = CoordinatorKey.byGroupId(deletedGroup.groupId());
            Errors error = Errors.forCode(deletedGroup.errorCode());
            if (error != Errors.NONE) {
                handleError(groupIdKey, error, failed, groupsToUnmap);
                continue;
            }

            completed.put(groupIdKey, null);
        }

        return new ApiResult<>(completed, failed, new ArrayList<>(groupsToUnmap));
    }

    private void handleError(
        CoordinatorKey groupId,
        Errors error,
        Map<CoordinatorKey, Throwable> failed,
        Set<CoordinatorKey> groupsToUnmap
    ) {
        switch (error) {
            case GROUP_AUTHORIZATION_FAILED:
            case INVALID_GROUP_ID:
            case NON_EMPTY_GROUP:
            case GROUP_ID_NOT_FOUND:
                log.debug("`DeleteConsumerGroups` request for group id {} failed due to error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
                break;

            case COORDINATOR_LOAD_IN_PROGRESS:
                // If the coordinator is in the middle of loading, then we just need to retry
                log.debug("`DeleteConsumerGroups` request for group id {} failed because the coordinator " +
                    "is still in the process of loading state. Will retry", groupId.idValue);
                break;

            case COORDINATOR_NOT_AVAILABLE:
            case NOT_COORDINATOR:
                // If the coordinator is unavailable or there was a coordinator change, then we unmap
                // the key so that we retry the `FindCoordinator` request
                log.debug("`DeleteConsumerGroups` request for group id {} returned error {}. " +
                    "Will attempt to find the coordinator again and retry", groupId.idValue, error);
                groupsToUnmap.add(groupId);
                break;

            default:
                log.error("`DeleteConsumerGroups` request for group id {} failed due to unexpected error {}", groupId.idValue, error);
                failed.put(groupId, error.exception());
        }
    }

}
