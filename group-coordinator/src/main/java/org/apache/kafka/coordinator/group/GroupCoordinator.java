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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.message.AlterShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.AlterShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.ConsumerGroupDescribeResponseData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatResponseData;
import org.apache.kafka.common.message.DeleteGroupsResponseData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DeleteShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.DescribeGroupsResponseData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsRequestData;
import org.apache.kafka.common.message.DescribeShareGroupOffsetsResponseData;
import org.apache.kafka.common.message.HeartbeatRequestData;
import org.apache.kafka.common.message.HeartbeatResponseData;
import org.apache.kafka.common.message.JoinGroupRequestData;
import org.apache.kafka.common.message.JoinGroupResponseData;
import org.apache.kafka.common.message.LeaveGroupRequestData;
import org.apache.kafka.common.message.LeaveGroupResponseData;
import org.apache.kafka.common.message.ListGroupsRequestData;
import org.apache.kafka.common.message.ListGroupsResponseData;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.ShareGroupDescribeResponseData;
import org.apache.kafka.common.message.ShareGroupHeartbeatRequestData;
import org.apache.kafka.common.message.ShareGroupHeartbeatResponseData;
import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.common.message.StreamsGroupHeartbeatRequestData;
import org.apache.kafka.common.message.SyncGroupRequestData;
import org.apache.kafka.common.message.SyncGroupResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.BufferSupplier;
import org.apache.kafka.coordinator.group.streams.StreamsGroupHeartbeatResult;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.function.IntSupplier;

/**
 * Group Coordinator's internal API.
 */
public interface GroupCoordinator {

    /**
     * Heartbeat to a Consumer Group.
     *
     * @param context           The request context.
     * @param request           The ConsumerGroupHeartbeatResponse data.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<ConsumerGroupHeartbeatResponseData> consumerGroupHeartbeat(
        AuthorizableRequestContext context,
        ConsumerGroupHeartbeatRequestData request
    );

    /**
     * Heartbeat to a Streams Group.
     *
     * @param context           The request context.
     * @param request           The StreamsGroupHeartbeatResponseData data.
     *
     * @return  A future yielding the response together with internal topics to create.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<StreamsGroupHeartbeatResult> streamsGroupHeartbeat(
        AuthorizableRequestContext context,
        StreamsGroupHeartbeatRequestData request
    );

    /**
     * Heartbeat to a Share Group.
     *
     * @param context           The request context.
     * @param request           The ShareGroupHeartbeatResponse data.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<ShareGroupHeartbeatResponseData> shareGroupHeartbeat(
        AuthorizableRequestContext context,
        ShareGroupHeartbeatRequestData request
    );

    /**
     * Join a Classic Group.
     *
     * @param context           The request context.
     * @param request           The JoinGroupRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<JoinGroupResponseData> joinGroup(
        AuthorizableRequestContext context,
        JoinGroupRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Sync a Classic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The SyncGroupRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<SyncGroupResponseData> syncGroup(
        AuthorizableRequestContext context,
        SyncGroupRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Heartbeat to a Classic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The HeartbeatRequest data.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<HeartbeatResponseData> heartbeat(
        AuthorizableRequestContext context,
        HeartbeatRequestData request
    );

    /**
     * Leave a Classic Group.
     *
     * @param context           The coordinator request context.
     * @param request           The LeaveGroupRequest data.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<LeaveGroupResponseData> leaveGroup(
        AuthorizableRequestContext context,
        LeaveGroupRequestData request
    );

    /**
     * List Groups.
     *
     * @param context           The coordinator request context.
     * @param request           The ListGroupRequest data.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<ListGroupsResponseData> listGroups(
        AuthorizableRequestContext context,
        ListGroupsRequestData request
    );

    /**
     * Describe Groups.
     *
     * @param context           The coordinator request context.
     * @param groupIds          The group ids.
     *
     * @return  A future yielding the results.
     *          The error codes of the results are set to indicate the errors occurred during the execution.
     */
    CompletableFuture<List<DescribeGroupsResponseData.DescribedGroup>> describeGroups(
        AuthorizableRequestContext context,
        List<String> groupIds
    );

    /**
     * Describe consumer groups.
     *
     * @param context           The coordinator request context.
     * @param groupIds          The group ids.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<List<ConsumerGroupDescribeResponseData.DescribedGroup>> consumerGroupDescribe(
        AuthorizableRequestContext context,
        List<String> groupIds
    );

    /**
     * Describe streams groups.
     *
     * @param context           The coordinator request context.
     * @param groupIds          The group ids.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<List<StreamsGroupDescribeResponseData.DescribedGroup>> streamsGroupDescribe(
        AuthorizableRequestContext context,
        List<String> groupIds
    );

    /**
     * Describe share groups.
     *
     * @param context           The coordinator request context.
     * @param groupIds          The group ids.
     *
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<List<ShareGroupDescribeResponseData.DescribedGroup>> shareGroupDescribe(
        AuthorizableRequestContext context,
        List<String> groupIds
    );

    /**
     * Alter Share Group Offsets for a given group.
     *
     * @param context     The request context.
     * @param groupId     The group id.
     * @param requestData The AlterShareGroupOffsetsRequest data.
     * @return A future yielding the results or an exception.
     */
    CompletableFuture<AlterShareGroupOffsetsResponseData> alterShareGroupOffsets(
        AuthorizableRequestContext context,
        String groupId,
        AlterShareGroupOffsetsRequestData requestData
    );

    /**
     * Delete Groups.
     *
     * @param context           The request context.
     * @param groupIds          The group ids.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return  A future yielding the results.
     *          The error codes of the results are set to indicate the errors occurred during the execution.
     */
    CompletableFuture<DeleteGroupsResponseData.DeletableGroupResultCollection> deleteGroups(
        AuthorizableRequestContext context,
        List<String> groupIds,
        BufferSupplier bufferSupplier
    );

    /**
     * Fetch offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The OffsetFetchRequestGroup request.
     *
     * @return  A future yielding the results.
     *          The error codes of the results are set to indicate the errors occurred during the execution.
     */
    CompletableFuture<OffsetFetchResponseData.OffsetFetchResponseGroup> fetchOffsets(
        AuthorizableRequestContext context,
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        boolean requireStable
    );

    /**
     * Describe the Share Group Offsets for a given group.
     *
     * @param context           The request context
     * @param request           The DescribeShareGroupOffsetsRequestGroup request.
     *
     * @return  A future yielding the results.
     *          The error codes of the response are set to indicate the errors occurred during the execution.
     */
    CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> describeShareGroupOffsets(
        AuthorizableRequestContext context,
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup request
    );

    /**
     * Describe all Share Group Offsets for a given group.
     *
     * @param context           The request context
     * @param request           The DescribeShareGroupOffsetsRequestGroup request.
     *
     * @return  A future yielding the results.
     *          The error codes of the response are set to indicate the errors occurred during the execution.
     */
    CompletableFuture<DescribeShareGroupOffsetsResponseData.DescribeShareGroupOffsetsResponseGroup> describeShareGroupAllOffsets(
        AuthorizableRequestContext context,
        DescribeShareGroupOffsetsRequestData.DescribeShareGroupOffsetsRequestGroup request
    );

    /**
     * Delete the Share Group Offsets for a given group.
     *
     * @param context           The request context
     * @param request           The DeleteShareGroupOffsetsRequestGroup request.
     *
     * @return  A future yielding the results.
     *          The error codes of the response are set to indicate the errors occurred during the execution.
     */
    CompletableFuture<DeleteShareGroupOffsetsResponseData> deleteShareGroupOffsets(
        AuthorizableRequestContext context,
        DeleteShareGroupOffsetsRequestData request
    );

    /**
     * Commit offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The OffsetCommitRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<OffsetCommitResponseData> commitOffsets(
        AuthorizableRequestContext context,
        OffsetCommitRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Commit transactional offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The TnxOffsetCommitRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<TxnOffsetCommitResponseData> commitTransactionalOffsets(
        AuthorizableRequestContext context,
        TxnOffsetCommitRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Delete offsets for a given Group.
     *
     * @param context           The request context.
     * @param request           The OffsetDeleteRequest data.
     * @param bufferSupplier    The buffer supplier tight to the request thread.
     *
     * @return  A future yielding the response.
     *          The error code(s) of the response are set to indicate the error(s) occurred during the execution.
     */
    CompletableFuture<OffsetDeleteResponseData> deleteOffsets(
        AuthorizableRequestContext context,
        OffsetDeleteRequestData request,
        BufferSupplier bufferSupplier
    );

    /**
     * Complete a transaction. This is called when the WriteTxnMarkers API is called
     * by the Transaction Coordinator in order to write the markers to the
     * __consumer_offsets partitions.
     *
     * @param tp                The topic-partition.
     * @param producerId        The producer id.
     * @param producerEpoch     The producer epoch.
     * @param coordinatorEpoch  The epoch of the transaction coordinator.
     * @param result            The transaction result.
     * @param transactionVersion The transaction version (1 = TV1, 2 = TV2, etc.).
     *
     * @return A future yielding the result.
     */
    CompletableFuture<Void> completeTransaction(
        TopicPartition tp,
        long producerId,
        short producerEpoch,
        int coordinatorEpoch,
        TransactionResult result,
        short transactionVersion
    );

    /**
     * Return the partition index for the given Group.
     *
     * @param groupId           The group id.
     *
     * @return The partition index.
     */
    int partitionFor(String groupId);

    /**
     * Group coordinator is now the leader for the given partition at the
     * given leader epoch. It should load cached state from the partition
     * and begin handling requests for groups mapped to it.
     *
     * @param groupMetadataPartitionIndex         The partition index.
     * @param groupMetadataPartitionLeaderEpoch   The leader epoch of the partition.
     */
    void onElection(
        int groupMetadataPartitionIndex,
        int groupMetadataPartitionLeaderEpoch
    );

    /**
     * Group coordinator is no longer the leader for the given partition
     * at the given leader epoch. It should unload cached state and stop
     * handling requests for groups mapped to it.
     *
     * @param groupMetadataPartitionIndex         The partition index.
     * @param groupMetadataPartitionLeaderEpoch   The leader epoch of the partition as an
     *                                            optional value. An empty value means that
     *                                            the topic was deleted.
     */
    void onResignation(
        int groupMetadataPartitionIndex,
        OptionalInt groupMetadataPartitionLeaderEpoch
    );

    /**
     * A new metadata image is available.
     *
     * @param delta     The metadata delta.
     * @param newImage  The new metadata image.
     */
    void onMetadataUpdate(
        MetadataDelta delta,
        MetadataImage newImage
    );

    /**
     * Return the configuration properties of the internal group
     * metadata topic.
     *
     * @return Properties of the internal topic.
     */
    Properties groupMetadataTopicConfigs();

    /**
     * Return the configuration of the provided group.
     *
     * @param groupId       The group id.
     * @return The group config.
     */
    Optional<GroupConfig> groupConfig(String groupId);

    /**
     * Update the configuration of the provided group.
     *
     * @param groupId           The group id.
     * @param newGroupConfig    The new group config
     */
    void updateGroupConfig(String groupId, Properties newGroupConfig);

    /**
     * Startup the group coordinator.
     *
     * @param groupMetadataTopicPartitionCount  A supplier to get the number of partitions
     *                                          of the consumer offsets topic.
     */
    void startup(IntSupplier groupMetadataTopicPartitionCount);

    /**
     * Shutdown the group coordinator.
     */
    void shutdown();
}
