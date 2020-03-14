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

import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.LATEST_SUPPORTED_VERSION;

import org.apache.kafka.streams.errors.TaskAssignmentException;
import org.apache.kafka.streams.processor.TaskId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class LegacySubscriptionInfoSerde {

    private static final Logger log = LoggerFactory.getLogger(LegacySubscriptionInfoSerde.class);

    static final int UNKNOWN = -1;

    private final int usedVersion;
    private final int latestSupportedVersion;
    private final UUID processId;
    private final Set<TaskId> prevTasks;
    private final Set<TaskId> standbyTasks;
    private final String userEndPoint;

    public LegacySubscriptionInfoSerde(final int version,
                                       final int latestSupportedVersion,
                                       final UUID processId,
                                       final Set<TaskId> prevTasks,
                                       final Set<TaskId> standbyTasks,
                                       final String userEndPoint) {
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
        usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
        this.processId = processId;
        this.prevTasks = prevTasks;
        this.standbyTasks = standbyTasks;
        // Coerce empty string to null. This was the effect of the serialization logic, anyway.
        this.userEndPoint = userEndPoint == null || userEndPoint.isEmpty() ? null : userEndPoint;
    }

    public int version() {
        return usedVersion;
    }

    public int latestSupportedVersion() {
        return latestSupportedVersion;
    }

    public UUID processId() {
        return processId;
    }

    public Set<TaskId> prevTasks() {
        return prevTasks;
    }

    public Set<TaskId> standbyTasks() {
        return standbyTasks;
    }

    public String userEndPoint() {
        return userEndPoint;
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data
     */
    public ByteBuffer encode() {
        if (usedVersion == 3 || usedVersion == 4 || usedVersion == 5 || usedVersion == 6) {
            final byte[] endPointBytes = prepareUserEndPoint(this.userEndPoint);

            final ByteBuffer buf = ByteBuffer.allocate(
                4 + // used version
                    4 + // latest supported version version
                    16 + // client ID
                    4 + prevTasks.size() * 8 + // length + prev tasks
                    4 + standbyTasks.size() * 8 + // length + standby tasks
                    4 + endPointBytes.length
            );

            buf.putInt(usedVersion); // used version
            buf.putInt(LATEST_SUPPORTED_VERSION); // supported version
            encodeClientUUID(buf, processId());
            encodeTasks(buf, prevTasks);
            encodeTasks(buf, standbyTasks);
            encodeUserEndPoint(buf, endPointBytes);

            buf.rewind();

            return buf;
        } else if (usedVersion == 2) {
            final byte[] endPointBytes = prepareUserEndPoint(this.userEndPoint);

            final ByteBuffer buf = ByteBuffer.allocate(
                4 + // version
                    16 + // client ID
                    4 + prevTasks.size() * 8 + // length + prev tasks
                    4 + standbyTasks.size() * 8 + // length + standby tasks
                    4 + endPointBytes.length
            );

            buf.putInt(2); // version
            encodeClientUUID(buf, processId());
            encodeTasks(buf, prevTasks);
            encodeTasks(buf, standbyTasks);
            encodeUserEndPoint(buf, endPointBytes);

            buf.rewind();

            return buf;
        } else if (usedVersion == 1) {
            final ByteBuffer buf1 = ByteBuffer.allocate(
                4 + // version
                    16 + // client ID
                    4 + prevTasks.size() * 8 + // length + prev tasks
                    4 + standbyTasks.size() * 8
            );

            buf1.putInt(1); // version
            encodeClientUUID(buf1, processId());
            encodeTasks(buf1, prevTasks);
            encodeTasks(buf1, standbyTasks);
            buf1.rewind();
            return buf1;
        } else {
            throw new IllegalStateException("Unknown metadata version: " + usedVersion
                                                + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
        }
    }

    public static void encodeClientUUID(final ByteBuffer buf, final UUID processId) {
        buf.putLong(processId.getMostSignificantBits());
        buf.putLong(processId.getLeastSignificantBits());
    }

    public static void encodeTasks(final ByteBuffer buf,
                                   final Collection<TaskId> taskIds) {
        buf.putInt(taskIds.size());
        for (final TaskId id : taskIds) {
            id.writeTo(buf);
        }
    }

    public static void encodeUserEndPoint(final ByteBuffer buf,
                                          final byte[] endPointBytes) {
        if (endPointBytes != null) {
            buf.putInt(endPointBytes.length);
            buf.put(endPointBytes);
        }
    }

    public static byte[] prepareUserEndPoint(final String userEndPoint) {
        if (userEndPoint == null) {
            return new byte[0];
        } else {
            return userEndPoint.getBytes(StandardCharsets.UTF_8);
        }
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data
     */
    public static LegacySubscriptionInfoSerde decode(final ByteBuffer data) {

        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        final int usedVersion = data.getInt();
        if (usedVersion > 2 && usedVersion < 7) {
            final int latestSupportedVersion = data.getInt();
            final UUID processId = decodeProcessId(data);
            final Set<TaskId> prevTasks = decodeTasks(data);
            final Set<TaskId> standbyTasks = decodeTasks(data);
            final String userEndPoint = decodeUserEndpoint(data);
            return new LegacySubscriptionInfoSerde(usedVersion, latestSupportedVersion, processId, prevTasks, standbyTasks, userEndPoint);
        } else if (usedVersion == 2) {
            final UUID processId = decodeProcessId(data);
            final Set<TaskId> prevTasks = decodeTasks(data);
            final Set<TaskId> standbyTasks = decodeTasks(data);
            final String userEndPoint = decodeUserEndpoint(data);
            return new LegacySubscriptionInfoSerde(2, UNKNOWN, processId, prevTasks, standbyTasks, userEndPoint);
        } else if (usedVersion == 1) {
            final UUID processId = decodeProcessId(data);
            final Set<TaskId> prevTasks = decodeTasks(data);
            final Set<TaskId> standbyTasks = decodeTasks(data);
            return new LegacySubscriptionInfoSerde(1, UNKNOWN, processId, prevTasks, standbyTasks, null);
        } else {
            final int latestSupportedVersion = data.getInt();
            log.info("Unable to decode subscription data: used version: {}; latest supported version: {}", usedVersion, LATEST_SUPPORTED_VERSION);
            return new LegacySubscriptionInfoSerde(usedVersion, latestSupportedVersion, null, null, null, null);
        }
    }

    private static String decodeUserEndpoint(final ByteBuffer data) {
        final int userEndpointBytesLength = data.getInt();
        final byte[] userEndpointBytes = new byte[userEndpointBytesLength];
        data.get(userEndpointBytes);
        return new String(userEndpointBytes, StandardCharsets.UTF_8);
    }

    private static Set<TaskId> decodeTasks(final ByteBuffer data) {
        final Set<TaskId> prevTasks = new HashSet<>();
        final int numPrevTasks = data.getInt();
        for (int i = 0; i < numPrevTasks; i++) {
            prevTasks.add(TaskId.readFrom(data));
        }
        return prevTasks;
    }

    private static UUID decodeProcessId(final ByteBuffer data) {
        return new UUID(data.getLong(), data.getLong());
    }

    @Override
    public int hashCode() {
        final int hashCode = usedVersion ^ latestSupportedVersion ^ processId.hashCode() ^ prevTasks.hashCode() ^ standbyTasks.hashCode();
        if (userEndPoint == null) {
            return hashCode;
        }
        return hashCode ^ userEndPoint.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof LegacySubscriptionInfoSerde) {
            final LegacySubscriptionInfoSerde other = (LegacySubscriptionInfoSerde) o;
            return usedVersion == other.usedVersion &&
                latestSupportedVersion == other.latestSupportedVersion &&
                processId.equals(other.processId) &&
                prevTasks.equals(other.prevTasks) &&
                standbyTasks.equals(other.standbyTasks) &&
                userEndPoint != null ? userEndPoint.equals(other.userEndPoint) : other.userEndPoint == null;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[version=" + usedVersion
            + ", supported version=" + latestSupportedVersion
            + ", process ID=" + processId
            + ", prev tasks=" + prevTasks
            + ", standby tasks=" + standbyTasks
            + ", user endpoint=" + userEndPoint + "]";
    }
}
