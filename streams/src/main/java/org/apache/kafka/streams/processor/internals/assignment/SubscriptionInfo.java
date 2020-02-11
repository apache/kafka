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

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.internals.generated.SubscriptionInfoData;
import org.apache.kafka.streams.processor.TaskId;
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

    private final SubscriptionInfoData data;
    private Set<TaskId> prevTasksCache = null;
    private Set<TaskId> standbyTasksCache = null;

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
                            final Set<TaskId> prevTasks,
                            final Set<TaskId> standbyTasks,
                            final String userEndPoint) {
        validateVersions(version, latestSupportedVersion);
        final SubscriptionInfoData data = new SubscriptionInfoData();
        data.setVersion(version);
        if (version >= 2) {
            data.setUserEndPoint(userEndPoint == null
                                     ? new byte[0]
                                     : userEndPoint.getBytes(StandardCharsets.UTF_8));
        }
        if (version >= 3) {
            data.setLatestSupportedVersion(latestSupportedVersion);
        }
        data.setProcessId(processId);
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

        this.data = data;
    }

    private SubscriptionInfo(final SubscriptionInfoData subscriptionInfoData) {
        validateVersions(subscriptionInfoData.version(), subscriptionInfoData.latestSupportedVersion());
        this.data = subscriptionInfoData;
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
            prevTasksCache = Collections.unmodifiableSet(
                data.prevTasks()
                    .stream()
                    .map(t -> new TaskId(t.topicGroupId(), t.partition()))
                    .collect(Collectors.toSet())
            );
        }
        return prevTasksCache;
    }

    public Set<TaskId> standbyTasks() {
        if (standbyTasksCache == null) {
            standbyTasksCache = Collections.unmodifiableSet(
                data.standbyTasks()
                    .stream()
                    .map(t -> new TaskId(t.topicGroupId(), t.partition()))
                    .collect(Collectors.toSet())
            );
        }
        return standbyTasksCache;
    }

    public String userEndPoint() {
        return data.userEndPoint() == null || data.userEndPoint().length == 0
            ? null
            : new String(data.userEndPoint(), StandardCharsets.UTF_8);
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
                     version, latestSupportedVersion);
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
