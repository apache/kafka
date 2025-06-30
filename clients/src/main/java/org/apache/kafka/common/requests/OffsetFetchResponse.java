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
package org.apache.kafka.common.requests;

import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.OffsetFetchRequestData;
import org.apache.kafka.common.message.OffsetFetchResponseData;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseGroup;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponsePartition;
import org.apache.kafka.common.message.OffsetFetchResponseData.OffsetFetchResponseTopic;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Arrays;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.common.record.RecordBatch.NO_PARTITION_LEADER_EPOCH;
import static org.apache.kafka.common.requests.OffsetFetchRequest.BATCH_MIN_VERSION;
import static org.apache.kafka.common.requests.OffsetFetchRequest.TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION;

/**
 * Possible error codes:
 *
 * - Partition errors:
 *   - {@link Errors#UNKNOWN_TOPIC_OR_PARTITION}
 *   - {@link Errors#TOPIC_AUTHORIZATION_FAILED}
 *   - {@link Errors#UNSTABLE_OFFSET_COMMIT}
 *
 * - Group or coordinator errors:
 *   - {@link Errors#COORDINATOR_LOAD_IN_PROGRESS}
 *   - {@link Errors#COORDINATOR_NOT_AVAILABLE}
 *   - {@link Errors#NOT_COORDINATOR}
 *   - {@link Errors#GROUP_AUTHORIZATION_FAILED}
 *   - {@link Errors#UNKNOWN_MEMBER_ID}
 *   - {@link Errors#STALE_MEMBER_EPOCH}
 */
public class OffsetFetchResponse extends AbstractResponse {
    public static final long INVALID_OFFSET = -1L;
    public static final String NO_METADATA = "";

    // We only need to track the partition errors returned in version 1. This
    // is used to identify group level errors when the response is normalized.
    private static final List<Errors> PARTITION_ERRORS = Arrays.asList(
        Errors.UNKNOWN_TOPIC_OR_PARTITION,
        Errors.TOPIC_AUTHORIZATION_FAILED
    );

    private final short version;
    private final OffsetFetchResponseData data;
    // Lazily initialized when OffsetFetchResponse#group is called.
    private Map<String, OffsetFetchResponseData.OffsetFetchResponseGroup> groups = null;

    public static class Builder {
        private final List<OffsetFetchResponseGroup> groups;

        public Builder(OffsetFetchResponseGroup group) {
            this(List.of(group));
        }

        public Builder(List<OffsetFetchResponseGroup> groups) {
            this.groups = groups;
        }

        public OffsetFetchResponse build(short version) {
            var data = new OffsetFetchResponseData();

            if (version >= BATCH_MIN_VERSION) {
                data.setGroups(groups);
            } else {
                if (groups.size() != 1) {
                    throw new UnsupportedVersionException(
                        "Version " + version + " of OffsetFetchResponse only supports one group."
                    );
                }

                OffsetFetchResponseGroup group = groups.get(0);
                data.setErrorCode(group.errorCode());

                group.topics().forEach(topic -> {
                    OffsetFetchResponseTopic newTopic = new OffsetFetchResponseTopic().setName(topic.name());
                    data.topics().add(newTopic);

                    topic.partitions().forEach(partition -> {
                        newTopic.partitions().add(new OffsetFetchResponsePartition()
                            .setPartitionIndex(partition.partitionIndex())
                            .setErrorCode(partition.errorCode())
                            .setCommittedOffset(partition.committedOffset())
                            .setMetadata(partition.metadata())
                            .setCommittedLeaderEpoch(partition.committedLeaderEpoch()));
                    });
                });
            }

            return new OffsetFetchResponse(data, version);
        }
    }

    public OffsetFetchResponse(OffsetFetchResponseData data, short version) {
        super(ApiKeys.OFFSET_FETCH);
        this.data = data;
        this.version = version;
    }

    public OffsetFetchResponseData.OffsetFetchResponseGroup group(String groupId) {
        if (version < BATCH_MIN_VERSION) {
            // for version 2 and later use the top-level error code from the response.
            // for older versions there is no top-level error in the response and all errors are partition errors,
            // so if there is a group or coordinator error at the partition level use that as the top-level error.
            // this way clients can depend on the top-level error regardless of the offset fetch version.
            // we return the error differently starting with version 8, so we will only populate the
            // error field if we are between version 2 and 7. if we are in version 8 or greater, then
            // we will populate the map of group id to error codes.
            short topLevelError = version < TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION ? topLevelError(data).code() : data.errorCode();
            if (topLevelError != Errors.NONE.code()) {
                return new OffsetFetchResponseGroup()
                    .setGroupId(groupId)
                    .setErrorCode(topLevelError);
            } else {
                return new OffsetFetchResponseGroup()
                    .setGroupId(groupId)
                    .setTopics(data.topics().stream().map(topic ->
                        new OffsetFetchResponseData.OffsetFetchResponseTopics()
                            .setName(topic.name())
                            .setPartitions(topic.partitions().stream().map(partition ->
                                new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                    .setPartitionIndex(partition.partitionIndex())
                                    .setErrorCode(partition.errorCode())
                                    .setCommittedOffset(partition.committedOffset())
                                    .setMetadata(partition.metadata())
                                    .setCommittedLeaderEpoch(partition.committedLeaderEpoch())
                            ).collect(Collectors.toList()))
                    ).collect(Collectors.toList()));
            }
        } else {
            if (groups == null) {
                groups = data.groups().stream().collect(Collectors.toMap(
                    OffsetFetchResponseData.OffsetFetchResponseGroup::groupId,
                    Function.identity()
                ));
            }
            var group = groups.get(groupId);
            if (group == null) {
                throw new IllegalArgumentException("Group " + groupId + " not found in the response");
            }
            return group;
        }
    }

    private static Errors topLevelError(OffsetFetchResponseData data) {
        for (OffsetFetchResponseTopic topic : data.topics()) {
            for (OffsetFetchResponsePartition partition : topic.partitions()) {
                Errors partitionError = Errors.forCode(partition.errorCode());
                if (partitionError != Errors.NONE && !PARTITION_ERRORS.contains(partitionError)) {
                    return partitionError;
                }
            }
        }
        return Errors.NONE;
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        Map<Errors, Integer> counts = new EnumMap<>(Errors.class);
        if (version < BATCH_MIN_VERSION) {
            if (version >= TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION) {
                updateErrorCounts(counts, Errors.forCode(data.errorCode()));
            }
            data.topics().forEach(topic ->
                topic.partitions().forEach(partition ->
                    updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
                )
            );
        } else {
            data.groups().forEach(group -> {
                updateErrorCounts(counts, Errors.forCode(group.errorCode()));
                group.topics().forEach(topic ->
                    topic.partitions().forEach(partition ->
                        updateErrorCounts(counts, Errors.forCode(partition.errorCode()))
                    )
                );
            });
        }
        return counts;
    }

    public static OffsetFetchResponse parse(Readable readable, short version) {
        return new OffsetFetchResponse(new OffsetFetchResponseData(readable, version), version);
    }

    @Override
    public OffsetFetchResponseData data() {
        return data;
    }

    @Override
    public boolean shouldClientThrottle(short version) {
        return version >= 4;
    }

    public static OffsetFetchResponseData.OffsetFetchResponseGroup groupError(
        OffsetFetchRequestData.OffsetFetchRequestGroup group,
        Errors error,
        int version
    ) {
        if (version >= TOP_LEVEL_ERROR_AND_NULL_TOPICS_MIN_VERSION) {
            return new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(group.groupId())
                .setErrorCode(error.code());
        } else {
            return new OffsetFetchResponseData.OffsetFetchResponseGroup()
                .setGroupId(group.groupId())
                .setTopics(group.topics().stream().map(topic ->
                    new OffsetFetchResponseData.OffsetFetchResponseTopics()
                        .setName(topic.name())
                        .setPartitions(topic.partitionIndexes().stream().map(partition ->
                            new OffsetFetchResponseData.OffsetFetchResponsePartitions()
                                .setPartitionIndex(partition)
                                .setErrorCode(error.code())
                                .setCommittedOffset(INVALID_OFFSET)
                                .setMetadata(NO_METADATA)
                                .setCommittedLeaderEpoch(NO_PARTITION_LEADER_EPOCH)
                        ).collect(Collectors.toList()))
                ).collect(Collectors.toList()));
        }
    }
}
