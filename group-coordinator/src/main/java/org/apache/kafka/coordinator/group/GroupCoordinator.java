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
package org.apache.kafka.coordinator.group;

import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.BufferSupplier;

import java.util.List;
import java.util.concurrent.CompletableFuture;

public interface GroupCoordinator {

    /**
     * Join a Generic Group.
     *
     * @param context           The request context.
     * @param request           The JoinGroupRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<JoinGroupResponseData> joinGroup(
        RequestContext context,
        JoinGroupRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Sync a Generic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The SyncGroupRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<SyncGroupResponseData> syncGroup(
        RequestContext context,
        SyncGroupRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Heartbeat to a Generic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The HeartbeatRequest data.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<HeartbeatResponseData> heartbeat(
        RequestContext context,
        HeartbeatRequestData request
    );

    /**
     * Leave a Generic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The LeaveGroupRequest data.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<LeaveGroupResponseData> leaveGroup(
        RequestContext context,
        LeaveGroupRequestData request
    );

    /**
     * List Groups.
     *
     * @param context           The coordinator request context.
     * @param request           The ListGroupRequest data.
     *
     * @return A future yielding the response or an exception.
     */
    CompletableFuture<ListGroupsResponseData> listGroups(
        RequestContext context,
        ListGroupsRequestData request
    );

    /**
     * Describe Groups.
     *
     * @param context           The coordinator request context.
     * @param groupIds          The group ids.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> describeGroups(
        RequestContext context,
        List<String> groupIds
    );

    /**
     * Delete Groups.
     *
     * @param context           The request context.
     * @param groupIds          The group ids.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> deleteGroups(
        RequestContext context,
        List<String> groupIds,
        BufferSupplier bufferSupplier
    );
}

