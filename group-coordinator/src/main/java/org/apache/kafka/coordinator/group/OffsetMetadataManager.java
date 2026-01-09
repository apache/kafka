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

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitRequestData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponsePartition;
import org.apache.kafka.common.message.TxnOffsetCommitResponseData.TxnOffsetCommitResponseTopic;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.RecordBatch;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.TransactionResult;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.common.runtime.CoordinatorRecord;
import org.apache.kafka.coordinator.common.runtime.CoordinatorResult;
import org.apache.kafka.coordinator.group.GroupCoordinatorShard.DeletedTopic;
import org.apache.kafka.coordinator.group.classic.ClassicGroup;
import org.apache.kafka.coordinator.group.classic.ClassicGroupState;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics;
import org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetricsShard;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.server.authorizer.AuthorizableRequestContext;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.apache.kafka.timeline.TimelineHashSet;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

import static org.apache.kafka.common.requests.OffsetFetchResponse.INVALID_OFFSET;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_DELETIONS_SENSOR_NAME;
import static org.apache.kafka.coordinator.group.metrics.GroupCoordinatorMetrics.OFFSET_EXPIRED_SENSOR_NAME;

/**
 * The OffsetMetadataManager manages the offsets of all the groups. It basically maintains
 * a mapping from group id to topic-partition to offset. This class has two kinds of methods:
 * 1) The request handlers which handle the requests and generate a response and records to
 *    mutate the hard state. Those records will be written by the runtime and applied to the
 *    hard state via the replay methods.
 * 2) The replay methods which apply records to the hard state. Those are used in the request
 *    handling as well as during the initial loading of the records from the partitions.
 */
public class OffsetMetadataManager {

    public static class Builder {
        private LogContext logContext = null;
        private SnapshotRegistry snapshotRegistry = null;
        private Time time = null;
        private GroupMetadataManager groupMetadataManager = null;
        private MetadataImage metadataImage = null;
        private GroupCoordinatorConfig config = null;
        private GroupCoordinatorMetricsShard metrics = null;

        public Builder withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        public Builder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        public Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        public Builder withGroupMetadataManager(GroupMetadataManager groupMetadataManager) {
            this.groupMetadataManager = groupMetadataManager;
            return this;
        }

        public Builder withGroupCoordinatorConfig(GroupCoordinatorConfig config) {
            this.config = config;
            return this;
        }

        public Builder withMetadataImage(MetadataImage metadataImage) {
            this.metadataImage = metadataImage;
            return this;
        }

        public Builder withGroupCoordinatorMetricsShard(GroupCoordinatorMetricsShard metrics) {
            this.metrics = metrics;
            return this;
        }

        public OffsetMetadataManager build() {
            if (logContext == null) logContext = new LogContext();
            if (snapshotRegistry == null) snapshotRegistry = new SnapshotRegistry(logContext);
            if (metadataImage == null) metadataImage = MetadataImage.EMPTY;
            if (time == null) time = Time.SYSTEM;

            if (groupMetadataManager == null) {
                throw new IllegalArgumentException("GroupMetadataManager cannot be null");
            }

            if (metrics == null) {
                throw new IllegalArgumentException("GroupCoordinatorMetricsShard cannot be null");
            }

            return new OffsetMetadataManager(
                snapshotRegistry,
                logContext,
                time,
                metadataImage,
                groupMetadataManager,
                config,
                metrics
            );
        }
    }

    /**
     * The logger.
     */
    private final Logger log;

    /**
     * The snapshot registry.
     */
    private final SnapshotRegistry snapshotRegistry;

    /**
     * The system time.
     */
    private final Time time;

    /**
     * The group metadata manager.
     */
    private final GroupMetadataManager groupMetadataManager;

    /**
     * The coordinator metrics.
     */
    private final GroupCoordinatorMetricsShard metrics;

    /**
     * The group coordinator config.
     */
    private final GroupCoordinatorConfig config;

    /**
     * The committed offsets.
     */
    private final Offsets offsets;

    /**
     * The pending transactional offsets keyed by producer id. This structure holds all the
     * transactional offsets that are part of ongoing transactions. When the transaction is
     * committed, they are transferred to `offsets`; when the transaction is aborted, they
     * are removed.
     */
    private final TimelineHashMap<Long, Offsets> pendingTransactionalOffsets;

    /**
     * The open transactions (producer ids) by group id, topic name and partition id.
     */
    private final OpenTransactions openTransactions;

    /**
     * Tracks open transactions (producer ids) by group id, topic name and partition id.
     * It is the responsibility of the caller to update {@link #pendingTransactionalOffsets}.
     */
    private class OpenTransactions {
        /**
         * The open transactions (producer ids) keyed by group id, topic name and partition id.
         * Tracks whether partitions have any pending transactional offsets that have not been deleted.
         *
         * Values in each level of the map will never be empty collections.
         */
        private final TimelineHashMap<String, TimelineHashMap<String, TimelineHashMap<Integer, TimelineHashSet<Long>>>> openTransactionsByGroup;

        private OpenTransactions() {
            this.openTransactionsByGroup = new TimelineHashMap<>(snapshotRegistry, 0);
        }

        /**
         * Adds a producer id to the open transactions for the given group and topic partition.
         *
         * @param groupId    The group id.
         * @param topic      The topic name.
         * @param partition  The partition.
         * @param producerId The producer id.
         * @return {@code true} if the partition did not already have a pending offset from the producer id.
         */
        private boolean add(String groupId, String topic, int partition, long producerId) {
            return openTransactionsByGroup
                .computeIfAbsent(groupId, __ -> new TimelineHashMap<>(snapshotRegistry, 1))
                .computeIfAbsent(topic, __ -> new TimelineHashMap<>(snapshotRegistry, 1))
                .computeIfAbsent(partition, __ -> new TimelineHashSet<>(snapshotRegistry, 1))
                .add(producerId);
        }

        /**
         * Clears all open transactions for the given group and topic partition.
         *
         * @param groupId    The group id.
         * @param topic      The topic name.
         * @param partition  The partition.
         */
        private void clear(String groupId, String topic, int partition) {
            TimelineHashMap<String, TimelineHashMap<Integer, TimelineHashSet<Long>>> openTransactionsByTopic =
                openTransactionsByGroup.get(groupId);
            if (openTransactionsByTopic == null) return;

            TimelineHashMap<Integer, TimelineHashSet<Long>> openTransactionsByPartition = openTransactionsByTopic.get(topic);
            if (openTransactionsByPartition == null) return;

            openTransactionsByPartition.remove(partition);

            if (openTransactionsByPartition.isEmpty()) {
                openTransactionsByTopic.remove(topic);
                if (openTransactionsByTopic.isEmpty()) {
                    openTransactionsByGroup.remove(groupId);
                }
            }
        }

        /**
         * Returns {@code true} if the given group has any pending transactional offsets.
         *
         * @param groupId The group id.
         * @return {@code true} if the given group has any pending transactional offsets.
         */
        private boolean contains(String groupId) {
            return openTransactionsByGroup.containsKey(groupId);
        }

        /**
         * Returns {@code true} if the given group has any pending transactional offsets for the given topic and partition.
         *
         * @param groupId   The group id.
         * @param topic     The topic name.
         * @param partition The partition.
         * @return {@code true} if the given group has any pending transactional offsets for the given topic and partition.
         */
        private boolean contains(String groupId, String topic, int partition) {
            TimelineHashSet<Long> openTransactions = get(groupId, topic, partition);
            return openTransactions != null;
        }

        /**
         * Performs the given action for each partition with a pending transactional offset for the given group.
         *
         * @param groupId The group id.
         * @param action  The action to be performed for each partition with a pending transactional offset.
         */
        private void forEachTopicPartition(String groupId, BiConsumer<String, Integer> action) {
            TimelineHashMap<String, TimelineHashMap<Integer, TimelineHashSet<Long>>> openTransactionsByTopic =
                openTransactionsByGroup.get(groupId);
            if (openTransactionsByTopic == null) return;

            openTransactionsByTopic.forEach((topic, openTransactionsByPartition) -> {
                openTransactionsByPartition.forEach((partition, producerIds) -> {
                    action.accept(topic, partition);
                });
            });
        }

        /**
         * Performs the given action for each producer id with a pending transactional offset for the given group and topic partition.
         *
         * @param groupId   The group id.
         * @param topic     The topic name.
         * @param partition The partition.
         * @param action    The action to be performed for each producer id with a pending transactional offset.
         */
        private void forEach(String groupId, String topic, int partition, Consumer<Long> action) {
            TimelineHashSet<Long> openTransactions = get(groupId, topic, partition);
            if (openTransactions == null) return;

            openTransactions.forEach(action);
        }

        /**
         * Gets the set of producer ids with pending transactional offsets for the given group and topic partition.
         *
         * @param groupId   The group id.
         * @param topic     The topic name.
         * @param partition The partition.
         * @return The set of producer ids with pending transactional offsets for the given group and topic partition.
         */
        private TimelineHashSet<Long> get(String groupId, String topic, int partition) {
            TimelineHashMap<String, TimelineHashMap<Integer, TimelineHashSet<Long>>> openTransactionsByTopic =
                openTransactionsByGroup.get(groupId);
            if (openTransactionsByTopic == null) return null;

            TimelineHashMap<Integer, TimelineHashSet<Long>> openTransactionsByPartition = openTransactionsByTopic.get(topic);
            if (openTransactionsByPartition == null) return null;

            return openTransactionsByPartition.get(partition);
        }

        /**
         * Removes a producer id from the open transactions for the given group and topic partition.
         *
         * @param groupId    The group id.
         * @param topic      The topic name.
         * @param partition  The partition.
         * @param producerId The producer id.
         * @return {@code true} if the group and topic partition had a pending transactional offset from the producer id.
         */
        private boolean remove(String groupId, String topic, int partition, long producerId) {
            TimelineHashSet<Long> openTransactions = get(groupId, topic, partition);
            if (openTransactions == null) return false;

            boolean removed = openTransactions.remove(producerId);

            if (openTransactions.isEmpty()) {
                // Re-use the clean up in clear.
                clear(groupId, topic, partition);
            }

            return removed;
        }
    }

    private class Offsets {
        /**
         * The offsets keyed by group id, topic name and partition id.
         */
        private final TimelineHashMap<String, TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>>> offsetsByGroup;

        private Offsets() {
            this.offsetsByGroup = new TimelineHashMap<>(snapshotRegistry, 0);
        }

        private OffsetAndMetadata get(
            String groupId,
            String topic,
            int partition
        ) {
            TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> topicOffsets = offsetsByGroup.get(groupId);
            if (topicOffsets == null) {
                return null;
            } else {
                TimelineHashMap<Integer, OffsetAndMetadata> partitionOffsets = topicOffsets.get(topic);
                if (partitionOffsets == null) {
                    return null;
                } else {
                    return partitionOffsets.get(partition);
                }
            }
        }

        private OffsetAndMetadata put(
            String groupId,
            String topic,
            int partition,
            OffsetAndMetadata offsetAndMetadata
        ) {
            TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> topicOffsets = offsetsByGroup
                .computeIfAbsent(groupId, __ -> new TimelineHashMap<>(snapshotRegistry, 0));
            TimelineHashMap<Integer, OffsetAndMetadata> partitionOffsets = topicOffsets
                .computeIfAbsent(topic, __ -> new TimelineHashMap<>(snapshotRegistry, 0));
            return partitionOffsets.put(partition, offsetAndMetadata);
        }

        private OffsetAndMetadata remove(
            String groupId,
            String topic,
            int partition
        ) {
            TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> topicOffsets = offsetsByGroup.get(groupId);
            if (topicOffsets == null)
                return null;

            TimelineHashMap<Integer, OffsetAndMetadata> partitionOffsets = topicOffsets.get(topic);
            if (partitionOffsets == null)
                return null;

            OffsetAndMetadata removedValue = partitionOffsets.remove(partition);

            if (partitionOffsets.isEmpty())
                topicOffsets.remove(topic);

            if (topicOffsets.isEmpty())
                offsetsByGroup.remove(groupId);

            return removedValue;
        }
    }

    OffsetMetadataManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        Time time,
        MetadataImage metadataImage,
        GroupMetadataManager groupMetadataManager,
        GroupCoordinatorConfig config,
        GroupCoordinatorMetricsShard metrics
    ) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(OffsetMetadataManager.class);
        this.time = time;
        this.groupMetadataManager = groupMetadataManager;
        this.config = config;
        this.metrics = metrics;
        this.offsets = new Offsets();
        this.pendingTransactionalOffsets = new TimelineHashMap<>(snapshotRegistry, 0);
        this.openTransactions = new OpenTransactions();
    }

    /**
     * Validates an OffsetCommit request.
     *
     * @param context The request context.
     * @param request The actual request.
     * @return A validator for per-partition validation.
     */
    private CommitPartitionValidator validateOffsetCommit(
        AuthorizableRequestContext context,
        OffsetCommitRequestData request
    ) throws ApiException {
        Group group;
        try {
            group = groupMetadataManager.group(request.groupId());
        } catch (GroupIdNotFoundException ex) {
            if (request.generationIdOrMemberEpoch() < 0) {
                // If the group does not exist and generation id is -1, the request comes from
                // either the admin client or a consumer which does not use the group management
                // facility. In this case, a so-called simple group is created and the request
                // is accepted.
                log.info("[GroupId {}] Creating a simple consumer group via manual offset commit.", request.groupId());
                group = groupMetadataManager.getOrMaybeCreateClassicGroup(request.groupId(), true);
            } else {
                if (context.requestVersion() >= 9) {
                    // Starting from version 9 of the OffsetCommit API, we return GROUP_ID_NOT_FOUND
                    // if the group does not exist. This error works for both the old and the new
                    // protocol for clients using this version of the API.
                    throw ex;
                } else {
                    // For older version, we return ILLEGAL_GENERATION to preserve the backward
                    // compatibility.
                    throw Errors.ILLEGAL_GENERATION.exception();
                }
            }
        }

        CommitPartitionValidator validator = group.validateOffsetCommit(
            request.memberId(),
            request.groupInstanceId(),
            request.generationIdOrMemberEpoch(),
            false,
            context.requestVersion()
        );

        // In the old consumer group protocol, the offset commits maintain the session if
        // the group is in Stable or PreparingRebalance state.
        if (group.type() == Group.GroupType.CLASSIC) {
            ClassicGroup classicGroup = (ClassicGroup) group;
            if (classicGroup.isInState(ClassicGroupState.STABLE) || classicGroup.isInState(ClassicGroupState.PREPARING_REBALANCE)) {
                groupMetadataManager.rescheduleClassicGroupMemberHeartbeat(
                    classicGroup,
                    classicGroup.member(request.memberId())
                );
            }
        }

        return validator;
    }

    /**
     * Validates an TxnOffsetCommit request.
     *
     * @param context The request context.
     * @param request The actual request.
     * @return A validator for per-partition validation.
     */
    private CommitPartitionValidator validateTransactionalOffsetCommit(
        AuthorizableRequestContext context,
        TxnOffsetCommitRequestData request
    ) throws ApiException {
        Group group;
        try {
            group = groupMetadataManager.group(request.groupId());
        } catch (GroupIdNotFoundException ex) {
            if (request.generationId() < 0) {
                // If the group does not exist and generation id is -1, the request comes from
                // either the admin client or a consumer which does not use the group management
                // facility. In this case, a so-called simple group is created and the request
                // is accepted.
                group = groupMetadataManager.getOrMaybeCreateClassicGroup(request.groupId(), true);
            } else {
                throw Errors.ILLEGAL_GENERATION.exception();
            }
        }

        try {
            return group.validateOffsetCommit(
                request.memberId(),
                request.groupInstanceId(),
                request.generationId(),
                true,
                context.requestVersion()
            );
        } catch (StaleMemberEpochException ex) {
            throw Errors.ILLEGAL_GENERATION.exception();
        }
    }

    /**
     * Validates an OffsetFetch request.
     *
     * @param request               The actual request.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     */
    private void validateOffsetFetch(
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        long lastCommittedOffset
    ) throws GroupIdNotFoundException {
        Group group = groupMetadataManager.group(request.groupId(), lastCommittedOffset);
        group.validateOffsetFetch(
            request.memberId(),
            request.memberEpoch(),
            lastCommittedOffset
        );
    }

    /**
     * Validates an OffsetDelete request.
     *
     * @param request The actual request.
     */
    private Group validateOffsetDelete(
        OffsetDeleteRequestData request
    ) throws GroupIdNotFoundException {
        Group group = groupMetadataManager.group(request.groupId());
        group.validateOffsetDelete();
        return group;
    }

    /**
     * @return True if the committed metadata is invalid; False otherwise.
     */
    private boolean isMetadataInvalid(String metadata) {
        return metadata != null && metadata.length() > config.offsetMetadataMaxSize();
    }

    /**
     * Computes the expiration timestamp based on the retention time provided in the OffsetCommit
     * request.
     *
     * The "default" expiration timestamp is defined as now + retention. The retention may be overridden
     * in versions from v2 to v4. Otherwise, the retention defined on the broker is used. If an explicit
     * commit timestamp is provided (v1 only), the expiration timestamp is computed based on that.
     *
     * @param retentionTimeMs   The retention time in milliseconds.
     * @param currentTimeMs     The current time in milliseconds.
     *
     * @return An optional containing the expiration timestamp if defined; an empty optional otherwise.
     */
    private static OptionalLong expireTimestampMs(
        long retentionTimeMs,
        long currentTimeMs
    ) {
        return retentionTimeMs == OffsetCommitRequest.DEFAULT_RETENTION_TIME ?
            OptionalLong.empty() : OptionalLong.of(currentTimeMs + retentionTimeMs);
    }

    /**
     * Handles an OffsetCommit request.
     *
     * @param context The request context.
     * @param request The OffsetCommit request.
     *
     * @return A Result containing the OffsetCommitResponseData response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<OffsetCommitResponseData, CoordinatorRecord> commitOffset(
        AuthorizableRequestContext context,
        OffsetCommitRequestData request
    ) throws ApiException {
        CommitPartitionValidator validator = validateOffsetCommit(context, request);

        final OffsetCommitResponseData response = new OffsetCommitResponseData();
        final List<CoordinatorRecord> records = new ArrayList<>();
        final long currentTimeMs = time.milliseconds();
        final OptionalLong expireTimestampMs = expireTimestampMs(request.retentionTimeMs(), currentTimeMs);

        request.topics().forEach(topic -> {
            final OffsetCommitResponseTopic topicResponse = new OffsetCommitResponseTopic()
                .setTopicId(topic.topicId())
                .setName(topic.name());
            response.topics().add(topicResponse);

            topic.partitions().forEach(partition -> {
                if (isMetadataInvalid(partition.committedMetadata())) {
                    topicResponse.partitions().add(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.OFFSET_METADATA_TOO_LARGE.code()));
                } else {
                    // Validate commit per-partition
                    validator.validate(
                        topic.name(),
                        topic.topicId(),
                        partition.partitionIndex()
                    );

                    log.debug("[GroupId {}] Committing offsets {} for partition {}-{}-{} from member {} with leader epoch {}.",
                        request.groupId(), partition.committedOffset(), topic.topicId(), topic.name(), partition.partitionIndex(),
                        request.memberId(), partition.committedLeaderEpoch());

                    topicResponse.partitions().add(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.NONE.code()));

                    final OffsetAndMetadata offsetAndMetadata = OffsetAndMetadata.fromRequest(
                        topic.topicId(),
                        partition,
                        currentTimeMs,
                        expireTimestampMs
                    );

                    records.add(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                        request.groupId(),
                        topic.name(),
                        partition.partitionIndex(),
                        offsetAndMetadata
                    ));
                }
            });
        });

        if (!records.isEmpty()) {
            metrics.record(GroupCoordinatorMetrics.OFFSET_COMMITS_SENSOR_NAME, records.size());
        }

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Handles an TxnOffsetCommit request.
     *
     * @param context The request context.
     * @param request The TxnOffsetCommit request.
     *
     * @return A Result containing the TxnOffsetCommitResponseData response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<TxnOffsetCommitResponseData, CoordinatorRecord> commitTransactionalOffset(
        AuthorizableRequestContext context,
        TxnOffsetCommitRequestData request
    ) throws ApiException {
        CommitPartitionValidator validator = validateTransactionalOffsetCommit(context, request);

        final TxnOffsetCommitResponseData response = new TxnOffsetCommitResponseData();
        final List<CoordinatorRecord> records = new ArrayList<>();
        final long currentTimeMs = time.milliseconds();

        request.topics().forEach(topic -> {
            final TxnOffsetCommitResponseTopic topicResponse = new TxnOffsetCommitResponseTopic().setName(topic.name());
            response.topics().add(topicResponse);

            topic.partitions().forEach(partition -> {
                if (isMetadataInvalid(partition.committedMetadata())) {
                    topicResponse.partitions().add(new TxnOffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.OFFSET_METADATA_TOO_LARGE.code()));
                } else {
                    // Validate commit per-partition
                    try {
                        validator.validate(
                            topic.name(),
                            org.apache.kafka.common.Uuid.ZERO_UUID,
                            partition.partitionIndex()
                        );
                    } catch (StaleMemberEpochException ex) {
                        throw Errors.ILLEGAL_GENERATION.exception();
                    }

                    log.debug("[GroupId {}] Committing transactional offsets {} for partition {}-{} from member {} with leader epoch {}.",
                        request.groupId(), partition.committedOffset(), topic.name(), partition.partitionIndex(),
                        request.memberId(), partition.committedLeaderEpoch());

                    topicResponse.partitions().add(new TxnOffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.NONE.code()));

                    final OffsetAndMetadata offsetAndMetadata = OffsetAndMetadata.fromRequest(
                        partition,
                        currentTimeMs
                    );

                    records.add(GroupCoordinatorRecordHelpers.newOffsetCommitRecord(
                        request.groupId(),
                        topic.name(),
                        partition.partitionIndex(),
                        offsetAndMetadata
                    ));
                }
            });
        });

        if (!records.isEmpty()) {
            metrics.record(GroupCoordinatorMetrics.OFFSET_COMMITS_SENSOR_NAME, records.size());
        }

        return new CoordinatorResult<>(records, response);
    }

    /**
     * Handles an OffsetDelete request.
     *
     * @param request The OffsetDelete request.
     *
     * @return A Result containing the OffsetDeleteResponseData response and
     *         a list of records to update the state machine.
     */
    public CoordinatorResult<OffsetDeleteResponseData, CoordinatorRecord> deleteOffsets(
        OffsetDeleteRequestData request
    ) throws ApiException {
        final Group group = validateOffsetDelete(request);
        final List<CoordinatorRecord> records = new ArrayList<>();
        final OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection responseTopicCollection =
            new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection();

        request.topics().forEach(topic -> {
            final OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection responsePartitionCollection =
                new OffsetDeleteResponseData.OffsetDeleteResponsePartitionCollection();

            if (group.isSubscribedToTopic(topic.name())) {
                topic.partitions().forEach(partition ->
                    responsePartitionCollection.add(new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.GROUP_SUBSCRIBED_TO_TOPIC.code())
                    )
                );
            } else {
                topic.partitions().forEach(partition -> {
                    // We always add the partition to the response.
                    responsePartitionCollection.add(new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                    );

                    // A tombstone is written if an offset is present in the main storage or
                    // if a pending transactional offset exists.
                    if (hasCommittedOffset(request.groupId(), topic.name(), partition.partitionIndex()) ||
                        hasPendingTransactionalOffsets(request.groupId(), topic.name(), partition.partitionIndex())) {
                        records.add(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(
                            request.groupId(),
                            topic.name(),
                            partition.partitionIndex()
                        ));
                    }
                });
            }

            responseTopicCollection.add(new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
                .setName(topic.name())
                .setPartitions(responsePartitionCollection)
            );
        });
        metrics.record(OFFSET_DELETIONS_SENSOR_NAME, records.size());

        return new CoordinatorResult<>(
            records,
            new OffsetDeleteResponseData().setTopics(responseTopicCollection)
        );
    }

    /**
     * Deletes offsets as part of a DeleteGroups request.
     * Populates the record list passed in with records to update the state machine.
     * Validations are done in {@link GroupCoordinatorShard#deleteGroups(AuthorizableRequestContext, List)}
     *
     * @param groupId The id of the given group.
     * @param records The record list to populate.
     *
     * @return The number of offsets to be deleted.
     */
    public int deleteAllOffsets(
        String groupId,
        List<CoordinatorRecord> records
    ) {
        TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> offsetsByTopic = offsets.offsetsByGroup.get(groupId);
        AtomicInteger numDeletedOffsets = new AtomicInteger();

        // Delete all the offsets from the main storage.
        if (offsetsByTopic != null) {
            offsetsByTopic.forEach((topic, offsetsByPartition) ->
                offsetsByPartition.keySet().forEach(partition -> {
                    records.add(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, topic, partition));
                    numDeletedOffsets.getAndIncrement();
                })
            );
        }

        // Delete all the pending transactional offsets too. Here we only write a tombstone
        // if the topic-partition was not in the main storage because we don't need to write
        // two consecutive tombstones.
        openTransactions.forEachTopicPartition(groupId, (topic, partition) -> {
            if (!hasCommittedOffset(groupId, topic, partition)) {
                records.add(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, topic, partition));
                numDeletedOffsets.getAndIncrement();
            }
        });

        return numDeletedOffsets.get();
    }

    /**
     * @return true iff there is at least one pending transactional offset for the given
     * group, topic and partition.
     *
     * Package private for testing.
     */
    boolean hasPendingTransactionalOffsets(
        String groupId,
        String topic,
        int partition
    ) {
        return openTransactions.contains(groupId, topic, partition);
    }

    /**
     * @return true iff there is a committed offset in the main offset store for the
     * given group, topic and partition.
     *
     * Package private for testing.
     */
    boolean hasCommittedOffset(
        String groupId,
        String topic,
        int partition
    ) {
        return offsets.get(groupId, topic, partition) != null;
    }

    /**
     * Fetch offsets for a given Group.
     *
     * @param request               The OffsetFetchRequestGroup request.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     *
     * @return A List of OffsetFetchResponseTopics response.
     */
    public OffsetFetchResponseData.OffsetFetchResponseGroup fetchOffsets(
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        long lastCommittedOffset
    ) throws ApiException {
        final boolean requireStable = lastCommittedOffset == Long.MAX_VALUE;

        boolean failAllPartitions = false;
        try {
            validateOffsetFetch(request, lastCommittedOffset);
        } catch (GroupIdNotFoundException ex) {
            failAllPartitions = true;
        }

        final List<OffsetFetchResponseData.OffsetFetchResponseTopics> topicResponses = new ArrayList<>(request.topics().size());
        final TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> groupOffsets =
            failAllPartitions ? null : offsets.offsetsByGroup.get(request.groupId(), lastCommittedOffset);
        // We inline the lookups from hasPendingTransactionalOffsets here, to avoid repeating string
        // comparisons of group ids and topic names for every partition. They're only used when the
        // client has requested stable offsets.
        final TimelineHashMap<String, TimelineHashMap<Integer, TimelineHashSet<Long>>> openTransactionsByTopic =
            requireStable ? openTransactions.openTransactionsByGroup.get(request.groupId(), lastCommittedOffset) : null;

        request.topics().forEach(topic -> {
            final OffsetFetchResponseData.OffsetFetchResponseTopics topicResponse =
                new OffsetFetchResponseData.OffsetFetchResponseTopics()
                    .setTopicId(topic.topicId())
                    .setName(topic.name());
            topicResponses.add(topicResponse);

            final TimelineHashMap<Integer, OffsetAndMetadata> topicOffsets = groupOffsets == null ?
                null : groupOffsets.get(topic.name(), lastCommittedOffset);
            final TimelineHashMap<Integer, TimelineHashSet<Long>> openTransactionsByPartition =
                (requireStable && openTransactionsByTopic != null) ? openTransactionsByTopic.get(topic.name(), lastCommittedOffset) : null;

            topic.partitionIndexes().forEach(partitionIndex -> {
                final OffsetAndMetadata offsetAndMetadata = topicOffsets == null ?
                    null : topicOffsets.get(partitionIndex, lastCommittedOffset);

                if (requireStable &&
                    openTransactionsByPartition != null &&
                    openTransactionsByPartition.containsKey(partitionIndex, lastCommittedOffset)) {
                    topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(partitionIndex)
                        .setErrorCode(Errors.UNSTABLE_OFFSET_COMMIT.code())
                        .setCommittedOffset(INVALID_OFFSET)
                        .setCommittedLeaderEpoch(-1)
                        .setMetadata(""));
                } else if (isOffsetInvalid(offsetAndMetadata, topic.topicId())) {
                    topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(partitionIndex)
                        .setCommittedOffset(INVALID_OFFSET)
                        .setCommittedLeaderEpoch(-1)
                        .setMetadata(""));
                } else {
                    topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(partitionIndex)
                        .setCommittedOffset(offsetAndMetadata.committedOffset)
                        .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch.orElse(-1))
                        .setMetadata(offsetAndMetadata.metadata));
                }
            });
        });

        return new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId(request.groupId())
            .setTopics(topicResponses);
    }

    private static boolean isOffsetInvalid(OffsetAndMetadata offsetAndMetadata, Uuid expectedTopicId) {
        return offsetAndMetadata == null || isMismatchedTopicId(offsetAndMetadata.topicId, expectedTopicId);
    }

    private static boolean isMismatchedTopicId(Uuid actual, Uuid expected) {
        return !actual.equals(Uuid.ZERO_UUID) && !expected.equals(Uuid.ZERO_UUID) && !actual.equals(expected);
    }

    /**
     * Fetch all offsets for a given Group.
     *
     * @param request               The OffsetFetchRequestGroup request.
     * @param lastCommittedOffset   The last committed offsets in the timeline.
     *
     * @return A List of OffsetFetchResponseTopics response.
     */
    public OffsetFetchResponseData.OffsetFetchResponseGroup fetchAllOffsets(
        OffsetFetchRequestData.OffsetFetchRequestGroup request,
        long lastCommittedOffset
    ) throws ApiException {
        final boolean requireStable = lastCommittedOffset == Long.MAX_VALUE;

        try {
            validateOffsetFetch(request, lastCommittedOffset);
        } catch (GroupIdNotFoundException ex) {
            return new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(request.groupId())
                .setTopics(List.of());
        }

        final List<OffsetFetchResponseData.OffsetFetchResponseTopics> topicResponses = new ArrayList<>();
        final TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> groupOffsets =
            offsets.offsetsByGroup.get(request.groupId(), lastCommittedOffset);
        // We inline the lookups from hasPendingTransactionalOffsets here, to avoid repeating string
        // comparisons of group ids and topic names for every partition. They're only used when the
        // client has requested stable offsets.
        final TimelineHashMap<String, TimelineHashMap<Integer, TimelineHashSet<Long>>> openTransactionsByTopic =
            requireStable ? openTransactions.openTransactionsByGroup.get(request.groupId(), lastCommittedOffset) : null;

        if (groupOffsets != null) {
            groupOffsets.entrySet(lastCommittedOffset).forEach(topicEntry -> {
                final String topic = topicEntry.getKey();
                final TimelineHashMap<Integer, OffsetAndMetadata> topicOffsets = topicEntry.getValue();
                final TimelineHashMap<Integer, TimelineHashSet<Long>> openTransactionsByPartition =
                    (requireStable && openTransactionsByTopic != null) ? openTransactionsByTopic.get(topic, lastCommittedOffset) : null;

                final OffsetFetchResponseData.OffsetFetchResponseTopics topicResponse =
                    new OffsetFetchResponseData.OffsetFetchResponseTopics()
                        // It is set to zero for now but it will be set to the persisted
                        // topic id along the committed offset, if present.
                        .setTopicId(Uuid.ZERO_UUID)
                        .setName(topic);
                topicResponses.add(topicResponse);

                topicOffsets.entrySet(lastCommittedOffset).forEach(partitionEntry -> {
                    final int partition = partitionEntry.getKey();
                    final OffsetAndMetadata offsetAndMetadata = partitionEntry.getValue();

                    if (requireStable &&
                        openTransactionsByPartition != null &&
                        openTransactionsByPartition.containsKey(partition, lastCommittedOffset)) {
                        topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                            .setPartitionIndex(partition)
                            .setErrorCode(Errors.UNSTABLE_OFFSET_COMMIT.code())
                            .setCommittedOffset(INVALID_OFFSET)
                            .setCommittedLeaderEpoch(-1)
                            .setMetadata(""));
                    } else {
                        topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                            .setPartitionIndex(partition)
                            .setCommittedOffset(offsetAndMetadata.committedOffset)
                            .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch.orElse(-1))
                            .setMetadata(offsetAndMetadata.metadata));
                    }
                });
            });
        }

        return new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId(request.groupId())
            .setTopics(topicResponses);
    }

    /**
     * Remove expired offsets for the given group.
     *
     * @param groupId The group id.
     * @param records The list of records to populate with offset commit tombstone records.
     *
     * @return True if no offsets exist after expiry and no pending transactional offsets exist,
     *         false otherwise.
     */
    public boolean cleanupExpiredOffsets(String groupId, List<CoordinatorRecord> records) {
        TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> offsetsByTopic =
            offsets.offsetsByGroup.get(groupId);
        if (offsetsByTopic == null) {
            return !openTransactions.contains(groupId);
        }

        // We expect the group to exist.
        Group group = groupMetadataManager.group(groupId);
        long currentTimestampMs = time.milliseconds();
        Optional<OffsetExpirationCondition> offsetExpirationCondition = group.offsetExpirationCondition();

        if (offsetExpirationCondition.isEmpty()) {
            return false;
        }

        AtomicBoolean allOffsetsExpired = new AtomicBoolean(true);
        OffsetExpirationCondition condition = offsetExpirationCondition.get();

        offsetsByTopic.forEach((topic, partitions) -> {
            if (!group.isSubscribedToTopic(topic)) {
                partitions.forEach((partition, offsetAndMetadata) -> {
                    // We don't expire the offset yet if there is a pending transactional offset for the partition.
                    if (condition.isOffsetExpired(offsetAndMetadata, currentTimestampMs, config.offsetsRetentionMs()) &&
                        !hasPendingTransactionalOffsets(groupId, topic, partition)) {
                        appendOffsetCommitTombstone(groupId, topic, partition, records);
                        log.debug("[GroupId {}] Expired offset for partition={}-{}", groupId, topic, partition);
                    } else {
                        allOffsetsExpired.set(false);
                    }
                });
            } else {
                allOffsetsExpired.set(false);
            }
        });
        metrics.record(OFFSET_EXPIRED_SENSOR_NAME, records.size());

        // We don't want to remove the group if there are ongoing transactions with undeleted offsets.
        return allOffsetsExpired.get() && !openTransactions.contains(groupId);
    }

    /**
     * Remove offsets of the topics that have been deleted.
     *
     * @param deletedTopics   The topics that have been deleted.
     * @return The list of tombstones (offset commit) to append.
     */
    public List<CoordinatorRecord> onTopicsDeleted(
        List<DeletedTopic> deletedTopics
    ) {
        List<CoordinatorRecord> records = new ArrayList<>();

        Consumer<Offsets> delete = offsetsToClean -> {
            offsetsToClean.offsetsByGroup.forEach((groupId, topicOffsets) -> {
                for (DeletedTopic deletedTopic : deletedTopics) {
                    var partitionOffsets = topicOffsets.get(deletedTopic.name());
                    if (partitionOffsets != null) {
                        partitionOffsets.forEach((partition, offsetAndMetadata) -> {
                            // Delete if the topic ID matches or if the stored topic ID is ZERO_UUID (legacy records).
                            if (offsetAndMetadata.topicId.equals(Uuid.ZERO_UUID) || offsetAndMetadata.topicId.equals(deletedTopic.id())) {
                                appendOffsetCommitTombstone(groupId, deletedTopic.name(), partition, records);
                            }
                        });
                    }
                }
            });
        };

        // Delete the offsets from the main storage.
        delete.accept(offsets);
        // Delete the offsets from the pending transactional offsets.
        pendingTransactionalOffsets.forEach((__, offsets) -> delete.accept(offsets));

        return records;
    }

    /**
     * Add an offset commit tombstone record for the group.
     *
     * @param groupId   The group id.
     * @param topic     The topic name.
     * @param partition The partition.
     * @param records   The list of records to append the tombstone.
     */
    private void appendOffsetCommitTombstone(
        String groupId,
        String topic,
        int partition, 
        List<CoordinatorRecord> records
    ) {
        records.add(GroupCoordinatorRecordHelpers.newOffsetCommitTombstoneRecord(groupId, topic, partition));
        log.trace("[GroupId {}] Removing expired offset and metadata for {}-{}", groupId, topic, partition);
    }

    /**
     * Replays OffsetCommitKey/Value to update or delete the corresponding offsets.
     *
     * @param recordOffset  The offset of the record in the log.
     * @param producerId    The producer id of the batch containing the provided
     *                      key and value.
     * @param key           A OffsetCommitKey key.
     * @param value         A OffsetCommitValue value.
     */
    public void replay(
        long recordOffset,
        long producerId,
        OffsetCommitKey key,
        OffsetCommitValue value
    ) {
        final String groupId = key.group();
        final String topic = key.topic();
        final int partition = key.partition();

        if (value != null) {
            // The classic or consumer group should exist when offsets are committed or
            // replayed. However, it won't if the consumer commits offsets but does not
            // use the membership functionality. In this case, we automatically create
            // a so-called "simple consumer group". This is an empty classic group
            // without a protocol type.
            try {
                groupMetadataManager.group(groupId);
            } catch (GroupIdNotFoundException ex) {
                groupMetadataManager.getOrMaybeCreateClassicGroup(groupId, true);
            }

            if (producerId == RecordBatch.NO_PRODUCER_ID) {
                log.debug("Replaying offset commit with key {}, value {}", key, value);
                // If the offset is not part of a transaction, it is directly stored
                // in the offsets store.
                OffsetAndMetadata previousValue = offsets.put(
                    groupId,
                    topic,
                    partition,
                    OffsetAndMetadata.fromRecord(recordOffset, value)
                );
                if (previousValue == null) {
                    metrics.incrementNumOffsets();
                }
            } else {
                log.debug("Replaying transactional offset commit with producer id {}, key {}, value {}", producerId, key, value);
                // Otherwise, the transaction offset is stored in the pending transactional
                // offsets store. Pending offsets there are moved to the main store when
                // the transaction is committed; or removed when the transaction is aborted.
                pendingTransactionalOffsets
                    .computeIfAbsent(producerId, __ -> new Offsets())
                    .put(
                        groupId,
                        topic,
                        partition,
                        OffsetAndMetadata.fromRecord(recordOffset, value)
                    );
                openTransactions.add(groupId, topic, partition, producerId);
            }
        } else {
            if (offsets.remove(groupId, topic, partition) != null) {
                metrics.decrementNumOffsets();
            }

            // Remove all the pending offset commits related to the tombstone.
            openTransactions.forEach(groupId, topic, partition, openProducerId -> {
                Offsets pendingOffsets = pendingTransactionalOffsets.get(openProducerId);
                if (pendingOffsets != null) {
                    pendingOffsets.remove(groupId, topic, partition);
                }
            });
            openTransactions.clear(groupId, topic, partition);
        }
    }

    /**
     * Applies the given transaction marker.
     *
     * @param producerId    The producer id.
     * @param result        The result of the transaction.
     * @throws RuntimeException if the transaction can not be completed.
     */
    @SuppressWarnings("NPathComplexity")
    public void replayEndTransactionMarker(
        long producerId,
        TransactionResult result
    ) throws RuntimeException {
        Offsets pendingOffsets = pendingTransactionalOffsets.remove(producerId);

        if (pendingOffsets == null) {
            log.debug("Replayed end transaction marker with result {} for producer id {} but " +
                "no pending offsets are present. Ignoring it.", result, producerId);
            return;
        }

        pendingOffsets.offsetsByGroup.forEach((groupId, topicOffsets) -> {
            topicOffsets.forEach((topic, partitionOffsets) -> {
                partitionOffsets.keySet().forEach(partitionId -> {
                    openTransactions.remove(groupId, topic, partitionId, producerId);
                });
            });
        });

        if (result == TransactionResult.COMMIT) {
            log.debug("Committed transactional offset commits for producer id {}.", producerId);

            pendingOffsets.offsetsByGroup.forEach((groupId, topicOffsets) -> {
                topicOffsets.forEach((topicName, partitionOffsets) -> {
                    partitionOffsets.forEach((partitionId, offsetAndMetadata) -> {
                        OffsetAndMetadata existingOffsetAndMetadata = offsets.get(
                            groupId,
                            topicName,
                            partitionId
                        );

                        // We always keep the most recent committed offset when we have a mix of transactional and regular
                        // offset commits. Without preserving information of the commit record offset, compaction of the
                        // __consumer_offsets topic itself may result in the wrong offset commit being materialized.
                        if (existingOffsetAndMetadata == null || offsetAndMetadata.recordOffset > existingOffsetAndMetadata.recordOffset) {
                            log.debug("Committed transactional offset commit {} for producer id {} in group {} " +
                                "with topic {} and partition {}.",
                                offsetAndMetadata, producerId, groupId, topicName, partitionId);
                            OffsetAndMetadata previousValue = offsets.put(
                                groupId,
                                topicName,
                                partitionId,
                                offsetAndMetadata
                            );
                            if (previousValue == null) {
                                metrics.incrementNumOffsets();
                            }
                        } else {
                            log.info("Skipped the materialization of transactional offset commit {} for producer id {} in group {} with topic {}, " +
                                "partition {} since its record offset {} is smaller than the record offset {} of the last committed offset.",
                                offsetAndMetadata, producerId, groupId, topicName, partitionId, offsetAndMetadata.recordOffset, existingOffsetAndMetadata.recordOffset);
                        }
                    });
                });
            });
        } else {
            log.debug("Aborted transactional offset commits for producer id {}.", producerId);
        }
    }

    /**
     * @return The offset for the provided groupId and topic partition or null
     * if it does not exist.
     *
     * package-private for testing.
     */
    OffsetAndMetadata offset(
        String groupId,
        String topic,
        int partition
    ) {
        return offsets.get(groupId, topic, partition);
    }

    /**
     * @return The pending transactional offset for the provided parameters or null
     * if it does not exist.
     *
     * package-private for testing.
     */
    OffsetAndMetadata pendingTransactionalOffset(
        long producerId,
        String groupId,
        String topic,
        int partition
    ) {
        Offsets offsets = pendingTransactionalOffsets.get(producerId);
        if (offsets == null) return null;
        return offsets.get(groupId, topic, partition);
    }
}
