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
import org.apache.kafka.common.errors.ApiException;
import org.apache.kafka.common.errors.GroupIdNotFoundException;
import org.apache.kafka.common.errors.StaleMemberEpochException;
import org.apache.kafka.common.message.OffsetCommitRequestData;
import org.apache.kafka.common.message.OffsetCommitResponseData;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponseTopic;
import org.apache.kafka.common.message.OffsetCommitResponseData.OffsetCommitResponsePartition;
import org.apache.kafka.common.message.OffsetDeleteRequestData;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetDeleteResponseData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.OffsetCommitRequest;
import org.apache.kafka.common.requests.RequestContext;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.coordinator.group.generated.OffsetCommitKey;
import org.apache.kafka.coordinator.group.generated.OffsetCommitValue;
import org.apache.kafka.coordinator.group.generic.GenericGroup;
import org.apache.kafka.coordinator.group.generic.GenericGroupState;
import org.apache.kafka.coordinator.group.runtime.CoordinatorResult;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.timeline.SnapshotRegistry;
import org.apache.kafka.timeline.TimelineHashMap;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.kafka.common.requests.OffsetFetchResponse.INVALID_OFFSET;

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

        Builder withLogContext(LogContext logContext) {
            this.logContext = logContext;
            return this;
        }

        Builder withSnapshotRegistry(SnapshotRegistry snapshotRegistry) {
            this.snapshotRegistry = snapshotRegistry;
            return this;
        }

        Builder withTime(Time time) {
            this.time = time;
            return this;
        }

        Builder withGroupMetadataManager(GroupMetadataManager groupMetadataManager) {
            this.groupMetadataManager = groupMetadataManager;
            return this;
        }

        Builder withGroupCoordinatorConfig(GroupCoordinatorConfig config) {
            this.config = config;
            return this;
        }

        Builder withMetadataImage(MetadataImage metadataImage) {
            this.metadataImage = metadataImage;
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

            return new OffsetMetadataManager(
                snapshotRegistry,
                logContext,
                time,
                metadataImage,
                groupMetadataManager,
                config
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
     * The metadata image.
     */
    private MetadataImage metadataImage;

    /**
     * The group metadata manager.
     */
    private final GroupMetadataManager groupMetadataManager;

    /**
     * The group coordinator config.
     */
    private final GroupCoordinatorConfig config;

    /**
     * The offsets keyed by group id, topic name and partition id.
     */
    private final TimelineHashMap<String, TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>>> offsetsByGroup;

    OffsetMetadataManager(
        SnapshotRegistry snapshotRegistry,
        LogContext logContext,
        Time time,
        MetadataImage metadataImage,
        GroupMetadataManager groupMetadataManager,
        GroupCoordinatorConfig config
    ) {
        this.snapshotRegistry = snapshotRegistry;
        this.log = logContext.logger(OffsetMetadataManager.class);
        this.time = time;
        this.metadataImage = metadataImage;
        this.groupMetadataManager = groupMetadataManager;
        this.config = config;
        this.offsetsByGroup = new TimelineHashMap<>(snapshotRegistry, 0);
    }

    /**
     * Validates an OffsetCommit request.
     *
     * @param context The request context.
     * @param request The actual request.
     */
    private Group validateOffsetCommit(
        RequestContext context,
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
                group = groupMetadataManager.getOrMaybeCreateGenericGroup(request.groupId(), true);
            } else {
                if (context.header.apiVersion() >= 9) {
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

        try {
            group.validateOffsetCommit(
                request.memberId(),
                request.groupInstanceId(),
                request.generationIdOrMemberEpoch()
            );
        } catch (StaleMemberEpochException ex) {
            // The STALE_MEMBER_EPOCH error is only returned for new consumer group (KIP-848). When
            // it is, the member should be using the OffsetCommit API version >= 9. As we don't
            // support upgrading from the old to the new protocol yet, we return UNSUPPORTED_VERSION
            // error if an older version is used. We will revise this when the upgrade path is implemented.
            if (context.header.apiVersion() >= 9) {
                throw ex;
            } else {
                throw Errors.UNSUPPORTED_VERSION.exception();
            }
        }

        return group;
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
    public CoordinatorResult<OffsetCommitResponseData, Record> commitOffset(
        RequestContext context,
        OffsetCommitRequestData request
    ) throws ApiException {
        Group group = validateOffsetCommit(context, request);

        // In the old consumer group protocol, the offset commits maintain the session if
        // the group is in Stable or PreparingRebalance state.
        if (group.type() == Group.GroupType.GENERIC) {
            GenericGroup genericGroup = (GenericGroup) group;
            if (genericGroup.isInState(GenericGroupState.STABLE) || genericGroup.isInState(GenericGroupState.PREPARING_REBALANCE)) {
                groupMetadataManager.rescheduleGenericGroupMemberHeartbeat(
                    genericGroup,
                    genericGroup.member(request.memberId())
                );
            }
        }

        final OffsetCommitResponseData response = new OffsetCommitResponseData();
        final List<Record> records = new ArrayList<>();
        final long currentTimeMs = time.milliseconds();
        final OptionalLong expireTimestampMs = expireTimestampMs(request.retentionTimeMs(), currentTimeMs);

        request.topics().forEach(topic -> {
            final OffsetCommitResponseTopic topicResponse = new OffsetCommitResponseTopic().setName(topic.name());
            response.topics().add(topicResponse);

            topic.partitions().forEach(partition -> {
                if (partition.committedMetadata() != null && partition.committedMetadata().length() > config.offsetMetadataMaxSize) {
                    topicResponse.partitions().add(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.OFFSET_METADATA_TOO_LARGE.code()));
                } else {
                    log.debug("[GroupId {}] Committing offsets {} for partition {}-{} from member {} with leader epoch {}.",
                        request.groupId(), partition.committedOffset(), topic.name(), partition.partitionIndex(),
                        request.memberId(), partition.committedLeaderEpoch());

                    topicResponse.partitions().add(new OffsetCommitResponsePartition()
                        .setPartitionIndex(partition.partitionIndex())
                        .setErrorCode(Errors.NONE.code()));

                    final OffsetAndMetadata offsetAndMetadata = OffsetAndMetadata.fromRequest(
                        partition,
                        currentTimeMs,
                        expireTimestampMs
                    );

                    records.add(RecordHelpers.newOffsetCommitRecord(
                        request.groupId(),
                        topic.name(),
                        partition.partitionIndex(),
                        offsetAndMetadata,
                        metadataImage.features().metadataVersion()
                    ));
                }
            });
        });

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
    public CoordinatorResult<OffsetDeleteResponseData, Record> deleteOffsets(
        OffsetDeleteRequestData request
    ) throws ApiException {
        final Group group = validateOffsetDelete(request);
        final List<Record> records = new ArrayList<>();
        final OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection responseTopicCollection =
            new OffsetDeleteResponseData.OffsetDeleteResponseTopicCollection();
        final TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> offsetsByTopic =
            offsetsByGroup.get(request.groupId());

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
                final TimelineHashMap<Integer, OffsetAndMetadata> offsetsByPartition = offsetsByTopic == null ?
                    null : offsetsByTopic.get(topic.name());
                if (offsetsByPartition != null) {
                    topic.partitions().forEach(partition -> {
                        if (offsetsByPartition.containsKey(partition.partitionIndex())) {
                            responsePartitionCollection.add(new OffsetDeleteResponseData.OffsetDeleteResponsePartition()
                                .setPartitionIndex(partition.partitionIndex())
                            );
                            records.add(RecordHelpers.newOffsetCommitTombstoneRecord(
                                request.groupId(),
                                topic.name(),
                                partition.partitionIndex()
                            ));
                        }
                    });
                }
            }

            responseTopicCollection.add(new OffsetDeleteResponseData.OffsetDeleteResponseTopic()
                .setName(topic.name())
                .setPartitions(responsePartitionCollection)
            );
        });

        return new CoordinatorResult<>(
            records,
            new OffsetDeleteResponseData().setTopics(responseTopicCollection)
        );
    }

    /**
     * Deletes offsets as part of a DeleteGroups request.
     * Populates the record list passed in with records to update the state machine.
     * Validations are done in {@link GroupCoordinatorShard#deleteGroups(RequestContext, List)}
     *
     * @param groupId The id of the given group.
     * @param records The record list to populate.
     *
     * @return The number of offsets to be deleted.
     */
    public int deleteAllOffsets(
        String groupId,
        List<Record> records
    ) {
        TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> offsetsByTopic = offsetsByGroup.get(groupId);
        AtomicInteger numDeletedOffsets = new AtomicInteger();

        if (offsetsByTopic != null) {
            offsetsByTopic.forEach((topic, offsetsByPartition) ->
                offsetsByPartition.keySet().forEach(partition -> {
                    records.add(RecordHelpers.newOffsetCommitTombstoneRecord(groupId, topic, partition));
                    numDeletedOffsets.getAndIncrement();
                })
            );
        }
        return numDeletedOffsets.get();
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
        boolean failAllPartitions = false;
        try {
            validateOffsetFetch(request, lastCommittedOffset);
        } catch (GroupIdNotFoundException ex) {
            failAllPartitions = true;
        }

        final List<OffsetFetchResponseData.OffsetFetchResponseTopics> topicResponses = new ArrayList<>(request.topics().size());
        final TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> groupOffsets =
            failAllPartitions ? null : offsetsByGroup.get(request.groupId(), lastCommittedOffset);

        request.topics().forEach(topic -> {
            final OffsetFetchResponseData.OffsetFetchResponseTopics topicResponse =
                new OffsetFetchResponseData.OffsetFetchResponseTopics().setName(topic.name());
            topicResponses.add(topicResponse);

            final TimelineHashMap<Integer, OffsetAndMetadata> topicOffsets = groupOffsets == null ?
                null : groupOffsets.get(topic.name(), lastCommittedOffset);

            topic.partitionIndexes().forEach(partitionIndex -> {
                final OffsetAndMetadata offsetAndMetadata = topicOffsets == null ?
                    null : topicOffsets.get(partitionIndex, lastCommittedOffset);

                if (offsetAndMetadata == null) {
                    topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(partitionIndex)
                        .setCommittedOffset(INVALID_OFFSET)
                        .setCommittedLeaderEpoch(-1)
                        .setMetadata(""));
                } else {
                    topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(partitionIndex)
                        .setCommittedOffset(offsetAndMetadata.offset)
                        .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch.orElse(-1))
                        .setMetadata(offsetAndMetadata.metadata));
                }
            });
        });

        return new OffsetFetchResponseData.OffsetFetchResponseGroup()
            .setGroupId(request.groupId())
            .setTopics(topicResponses);
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
        try {
            validateOffsetFetch(request, lastCommittedOffset);
        } catch (GroupIdNotFoundException ex) {
            return new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(request.groupId())
                .setTopics(Collections.emptyList());
        }

        final List<OffsetFetchResponseData.OffsetFetchResponseTopics> topicResponses = new ArrayList<>();
        final TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> groupOffsets =
            offsetsByGroup.get(request.groupId(), lastCommittedOffset);

        if (groupOffsets != null) {
            groupOffsets.entrySet(lastCommittedOffset).forEach(topicEntry -> {
                final String topic = topicEntry.getKey();
                final TimelineHashMap<Integer, OffsetAndMetadata> topicOffsets = topicEntry.getValue();

                final OffsetFetchResponseData.OffsetFetchResponseTopics topicResponse =
                    new OffsetFetchResponseData.OffsetFetchResponseTopics().setName(topic);
                topicResponses.add(topicResponse);

                topicOffsets.entrySet(lastCommittedOffset).forEach(partitionEntry -> {
                    final int partition = partitionEntry.getKey();
                    final OffsetAndMetadata offsetAndMetadata = partitionEntry.getValue();

                    topicResponse.partitions().add(new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                        .setPartitionIndex(partition)
                        .setCommittedOffset(offsetAndMetadata.offset)
                        .setCommittedLeaderEpoch(offsetAndMetadata.leaderEpoch.orElse(-1))
                        .setMetadata(offsetAndMetadata.metadata));
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
     * @return True if no offsets exist or if all offsets expired, false otherwise.
     */
    public boolean cleanupExpiredOffsets(String groupId, List<Record> records) {
        TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> offsetsByTopic = offsetsByGroup.get(groupId);
        if (offsetsByTopic == null) {
            return true;
        }

        // We expect the group to exist.
        Group group = groupMetadataManager.group(groupId);
        Set<String> expiredPartitions = new HashSet<>();
        long currentTimestampMs = time.milliseconds();
        Optional<OffsetExpirationCondition> offsetExpirationCondition = group.offsetExpirationCondition();

        if (!offsetExpirationCondition.isPresent()) {
            return false;
        }

        AtomicBoolean allOffsetsExpired = new AtomicBoolean(true);
        OffsetExpirationCondition condition = offsetExpirationCondition.get();

        offsetsByTopic.forEach((topic, partitions) -> {
            if (!group.isSubscribedToTopic(topic)) {
                partitions.forEach((partition, offsetAndMetadata) -> {
                    if (condition.isOffsetExpired(offsetAndMetadata, currentTimestampMs, config.offsetsRetentionMs)) {
                        expiredPartitions.add(appendOffsetCommitTombstone(groupId, topic, partition, records).toString());
                    } else {
                        allOffsetsExpired.set(false);
                    }
                });
            } else {
                allOffsetsExpired.set(false);
            }
        });

        if (!expiredPartitions.isEmpty()) {
            log.info("[GroupId {}] Expiring offsets of partitions (allOffsetsExpired={}): {}",
                groupId, allOffsetsExpired, String.join(", ", expiredPartitions));
        }

        return allOffsetsExpired.get();
    }

    /**
     * Add an offset commit tombstone record for the group.
     *
     * @param groupId   The group id.
     * @param topic     The topic name.
     * @param partition The partition.
     * @param records   The list of records to append the tombstone.
     *
     * @return The topic partition of the corresponding tombstone.
     */
    private TopicPartition appendOffsetCommitTombstone(
        String groupId,
        String topic,
        int partition, 
        List<Record> records
    ) {
        records.add(RecordHelpers.newOffsetCommitTombstoneRecord(groupId, topic, partition));
        TopicPartition tp = new TopicPartition(topic, partition);
        log.trace("[GroupId {}] Removing expired offset and metadata for {}", groupId, tp);
        return tp;
    }

    /**
     * Replays OffsetCommitKey/Value to update or delete the corresponding offsets.
     *
     * @param key   A OffsetCommitKey key.
     * @param value A OffsetCommitValue value.
     */
    public void replay(
        OffsetCommitKey key,
        OffsetCommitValue value
    ) {
        final String groupId = key.group();
        final String topic = key.topic();
        final int partition = key.partition();

        if (value != null) {
            // The generic or consumer group should exist when offsets are committed or
            // replayed. However, it won't if the consumer commits offsets but does not
            // use the membership functionality. In this case, we automatically create
            // a so-called "simple consumer group". This is an empty generic group
            // without a protocol type.
            try {
                groupMetadataManager.group(groupId);
            } catch (GroupIdNotFoundException ex) {
                groupMetadataManager.getOrMaybeCreateGenericGroup(groupId, true);
            }

            updateOffset(
                groupId,
                topic,
                partition,
                OffsetAndMetadata.fromRecord(value)
            );
        } else {
            removeOffset(
                groupId,
                topic,
                partition
            );
        }
    }

    /**
     * A new metadata image is available.
     *
     * @param newImage  The new metadata image.
     * @param delta     The delta image.
     */
    public void onNewMetadataImage(MetadataImage newImage, MetadataDelta delta) {
        metadataImage = newImage;
    }

    /**
     * @return The offset for the provided groupId and topic partition or null
     * if it does not exist.
     *
     * package-private for testing.
     */
    OffsetAndMetadata offset(String groupId, String topic, int partition) {
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

    /**
     * Updates the offset.
     *
     * @param groupId           The group id.
     * @param topic             The topic name.
     * @param partition         The partition id.
     * @param offsetAndMetadata The offset metadata.
     */
    private void updateOffset(
        String groupId,
        String topic,
        int partition,
        OffsetAndMetadata offsetAndMetadata
    ) {
        TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> topicOffsets = offsetsByGroup
            .computeIfAbsent(groupId, __ -> new TimelineHashMap<>(snapshotRegistry, 0));
        TimelineHashMap<Integer, OffsetAndMetadata> partitionOffsets = topicOffsets
            .computeIfAbsent(topic, __ -> new TimelineHashMap<>(snapshotRegistry, 0));
        partitionOffsets.put(partition, offsetAndMetadata);
    }

    /**
     * Removes the offset.
     *
     * @param groupId           The group id.
     * @param topic             The topic name.
     * @param partition         The partition id.
     */
    private void removeOffset(
        String groupId,
        String topic,
        int partition
    ) {
        TimelineHashMap<String, TimelineHashMap<Integer, OffsetAndMetadata>> topicOffsets = offsetsByGroup.get(groupId);
        if (topicOffsets == null)
            return;

        TimelineHashMap<Integer, OffsetAndMetadata> partitionOffsets = topicOffsets.get(topic);
        if (partitionOffsets == null)
            return;

        partitionOffsets.remove(partition);

        if (partitionOffsets.isEmpty())
            topicOffsets.remove(topic);

        if (topicOffsets.isEmpty())
            offsetsByGroup.remove(groupId);
    }
}
