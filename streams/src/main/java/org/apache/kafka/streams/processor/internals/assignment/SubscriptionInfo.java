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
package org.apache.kafka.streams.processor.internals.assignment;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.internals.generated.SubscriptionInfoData;
import org.apache.kafka.streams.internals.generated.SubscriptionInfoData.PartitionToOffsetSum;
import org.apache.kafka.streams.internals.generated.SubscriptionInfoData.TaskOffsetSum;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.Task;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;

public class SubscriptionInfo {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionInfo.class);

    static final int UNKNOWN = -1;
    static final int MIN_VERSION_OFFSET_SUM_SUBSCRIPTION = 7;
    static final long UNKNOWN_OFFSET_SUM = -3L;

    private final SubscriptionInfoData data;
    private Set<TaskId> prevTasksCache = null;
    private Set<TaskId> standbyTasksCache = null;
    private Map<TaskId, Long> taskOffsetSumsCache = null;

    static {
        // Just statically check to make sure that the generated code always stays in sync with the overall protocol
        final int subscriptionInfoLatestVersion = SubscriptionInfoData.SCHEMAS.length - 1;
        if (subscriptionInfoLatestVersion != LATEST_SUPPORTED_VERSION) {
            throw new IllegalArgumentException(
                "streams/src/main/resources/common/message/SubscriptionInfo.json needs to be updated to match the " +
                    "latest assignment protocol version. SubscriptionInfo only supports up to  ["
                    + subscriptionInfoLatestVersion + "] but needs to support up to [" + LATEST_SUPPORTED_VERSION + "].");
        }
    }

    private static void validateVersions(final int version, final int latestSupportedVersion) {
        if (latestSupportedVersion == UNKNOWN && (version < 1 || version > 2)) {
            throw new IllegalArgumentException(
                "Only versions 1 and 2 are expected to use an UNKNOWN (-1) latest supported version. " +
                    "Got " + version + "."
            );
        } else if (latestSupportedVersion != UNKNOWN && (version < 1 || version > latestSupportedVersion)) {
            throw new IllegalArgumentException(
                "version must be between 1 and " + latestSupportedVersion + "; was: " + version
            );
        }
    }

    public SubscriptionInfo(final int version,
                            final int latestSupportedVersion,
                            final UUID processId,
                            final String userEndPoint,
                            final Map<TaskId, Long> taskOffsetSums) {
        validateVersions(version, latestSupportedVersion);
        final SubscriptionInfoData data = new SubscriptionInfoData();
        data.setVersion(version);
        data.setProcessId(processId);

        if (version >= 2) {
            data.setUserEndPoint(userEndPoint == null
                                     ? new byte[0]
                                     : userEndPoint.getBytes(StandardCharsets.UTF_8));
        }
        if (version >= 3) {
            data.setLatestSupportedVersion(latestSupportedVersion);
        }

        this.data = data;

        if (version >= MIN_VERSION_OFFSET_SUM_SUBSCRIPTION) {
            setTaskOffsetSumDataFromTaskOffsetSumMap(taskOffsetSums);
        } else {
            setPrevAndStandbySetsFromParsedTaskOffsetSumMap(taskOffsetSums);
        }
    }

    private SubscriptionInfo(final SubscriptionInfoData subscriptionInfoData) {
        validateVersions(subscriptionInfoData.version(), subscriptionInfoData.latestSupportedVersion());
        this.data = subscriptionInfoData;
    }

    private void setTaskOffsetSumDataFromTaskOffsetSumMap(final Map<TaskId, Long> taskOffsetSums) {
        final Map<Integer, List<SubscriptionInfoData.PartitionToOffsetSum>> topicGroupIdToPartitionOffsetSum = new HashMap<>();
        for (final Map.Entry<TaskId, Long> taskEntry : taskOffsetSums.entrySet()) {
            final TaskId task = taskEntry.getKey();
            topicGroupIdToPartitionOffsetSum.putIfAbsent(task.topicGroupId, new ArrayList<>()).add(
                new SubscriptionInfoData.PartitionToOffsetSum()
                    .setPartition(task.partition)
                    .setOffsetSum(taskEntry.getValue()));
        }

        data.setTaskOffsetSums(topicGroupIdToPartitionOffsetSum.entrySet().stream().map(t -> {
            final SubscriptionInfoData.TaskOffsetSum taskOffsetSum = new SubscriptionInfoData.TaskOffsetSum();
            taskOffsetSum.setTopicGroupId(t.getKey());
            taskOffsetSum.setPartitionToOffsetSum(t.getValue());
            return taskOffsetSum;
        }).collect(Collectors.toList()));
    }

    private void setPrevAndStandbySetsFromParsedTaskOffsetSumMap(final Map<TaskId, Long> taskOffsetSums) {
        final Set<TaskId> prevTasks = new HashSet<>();
        final Set<TaskId> standbyTasks = new HashSet<>();

        for (final Map.Entry<TaskId, Long> taskOffsetSum : taskOffsetSums.entrySet()) {
            if (taskOffsetSum.getValue() == Task.LATEST_OFFSET) {
                prevTasks.add(taskOffsetSum.getKey());
            } else {
                standbyTasks.add(taskOffsetSum.getKey());
            }
        }

        data.setPrevTasks(prevTasks.stream().map(t -> {
            final SubscriptionInfoData.TaskId taskId = new SubscriptionInfoData.TaskId();
            taskId.setTopicGroupId(t.topicGroupId);
            taskId.setPartition(t.partition);
            return taskId;
        }).collect(Collectors.toList()));
        data.setStandbyTasks(standbyTasks.stream().map(t -> {
            final SubscriptionInfoData.TaskId taskId = new SubscriptionInfoData.TaskId();
            taskId.setTopicGroupId(t.topicGroupId);
            taskId.setPartition(t.partition);
            return taskId;
        }).collect(Collectors.toList()));
    }

    public int version() {
        return data.version();
    }

    public int latestSupportedVersion() {
        return data.latestSupportedVersion();
    }

    public UUID processId() {
        return data.processId();
    }

    public Set<TaskId> prevTasks() {
        if (prevTasksCache == null) {
            if (data.version() >= MIN_VERSION_OFFSET_SUM_SUBSCRIPTION) {
                prevTasksCache = getActiveTasksFromTaskOffsetSumMap(taskOffsetSums());
            } else {
                prevTasksCache = Collections.unmodifiableSet(
                    data.prevTasks()
                        .stream()
                        .map(t -> new TaskId(t.topicGroupId(), t.partition()))
                        .collect(Collectors.toSet())
                );
            }
        }
        return prevTasksCache;
    }

    public Set<TaskId> standbyTasks() {
        if (standbyTasksCache == null) {
            if (data.version() >= MIN_VERSION_OFFSET_SUM_SUBSCRIPTION) {
                standbyTasksCache = getStandbyTasksFromTaskOffsetSumMap(taskOffsetSums());
            } else {
                standbyTasksCache = Collections.unmodifiableSet(
                    data.standbyTasks()
                        .stream()
                        .map(t -> new TaskId(t.topicGroupId(), t.partition()))
                        .collect(Collectors.toSet())
                );
            }
        }
        return standbyTasksCache;
    }

    public Map<TaskId, Long> taskOffsetSums() {
        if (taskOffsetSumsCache == null) {
            taskOffsetSumsCache = new HashMap<>();
            if (data.version() >= MIN_VERSION_OFFSET_SUM_SUBSCRIPTION) {
                for (final TaskOffsetSum topicGroup : data.taskOffsetSums()) {
                    for (final PartitionToOffsetSum partitionOffsetSum : topicGroup.partitionToOffsetSum()) {
                        taskOffsetSumsCache.put(
                            new TaskId(topicGroup.topicGroupId(),
                                partitionOffsetSum.partition()),
                            partitionOffsetSum.offsetSum()
                        );
                    }
                }
            } else {
                prevTasks().forEach((taskId) -> taskOffsetSumsCache.put(taskId, Task.LATEST_OFFSET));
                standbyTasks().forEach((taskId) -> taskOffsetSumsCache.put(taskId, UNKNOWN_OFFSET_SUM));
            }
        }
        return taskOffsetSumsCache;
    }

    public String userEndPoint() {
        return data.userEndPoint() == null || data.userEndPoint().length == 0
            ? null
            : new String(data.userEndPoint(), StandardCharsets.UTF_8);
    }

    public static Set<TaskId> getActiveTasksFromTaskOffsetSumMap(final Map<TaskId, Long> taskOffsetSums) {
        return taskOffsetSumMapToTaskSet(taskOffsetSums, true);
    }

    public static Set<TaskId> getStandbyTasksFromTaskOffsetSumMap(final Map<TaskId, Long> taskOffsetSums) {
        return taskOffsetSumMapToTaskSet(taskOffsetSums, false);
    }

    private static Set<TaskId> taskOffsetSumMapToTaskSet(final Map<TaskId, Long> taskOffsetSums,
                                                         final boolean getActiveTasks) {
        return taskOffsetSums.entrySet().stream()
                   .filter(t -> getActiveTasks == (t.getValue() == Task.LATEST_OFFSET))
                   .map(Map.Entry::getKey)
                   .collect(Collectors.toSet());
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data
     */
    public ByteBuffer encode() {
        if (data.version() > LATEST_SUPPORTED_VERSION) {
            throw new IllegalStateException(
                "Should never try to encode a SubscriptionInfo with version [" +
                    data.version() + "] > LATEST_SUPPORTED_VERSION [" + LATEST_SUPPORTED_VERSION + "]"
            );
        } else {
            final ObjectSerializationCache cache = new ObjectSerializationCache();
            final ByteBuffer buffer = ByteBuffer.allocate(data.size(cache, (short) data.version()));
            final ByteBufferAccessor accessor = new ByteBufferAccessor(buffer);
            data.write(accessor, cache, (short) data.version());
            buffer.rewind();
            return buffer;
        }
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data
     */
    public static SubscriptionInfo decode(final ByteBuffer data) {
        data.rewind();
        final int version = data.getInt();

        if (version > LATEST_SUPPORTED_VERSION) {
            // in this special case, we only rely on the version and latest version,
            //
            final int latestSupportedVersion = data.getInt();
            final SubscriptionInfoData subscriptionInfoData = new SubscriptionInfoData();
            subscriptionInfoData.setVersion(version);
            subscriptionInfoData.setLatestSupportedVersion(latestSupportedVersion);
            LOG.info("Unable to decode subscription data: used version: {}; latest supported version: {}",
                version,
                latestSupportedVersion
            );
            return new SubscriptionInfo(subscriptionInfoData);
        } else {
            data.rewind();
            final ByteBufferAccessor accessor = new ByteBufferAccessor(data);
            final SubscriptionInfoData subscriptionInfoData = new SubscriptionInfoData(accessor, (short) version);
            return new SubscriptionInfo(subscriptionInfoData);
        }
    }

    @Override
    public int hashCode() {
        return data.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof SubscriptionInfo) {
            final SubscriptionInfo other = (SubscriptionInfo) o;
            return data.equals(other.data);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return data.toString();
    }
}
