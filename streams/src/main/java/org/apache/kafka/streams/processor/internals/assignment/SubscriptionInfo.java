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

public class SubscriptionInfo {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionInfo.class);

    public static final int LATEST_SUPPORTED_VERSION = 4;
    static final int UNKNOWN = -1;

    private final int usedVersion;
    private final int latestSupportedVersion;
    private UUID processId;
    private Set<TaskId> prevTasks;
    private Set<TaskId> standbyTasks;
    private String userEndPoint;

    // used for decoding; don't apply version checks
    private SubscriptionInfo(final int version,
                             final int latestSupportedVersion) {
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
    }

    public SubscriptionInfo(final UUID processId,
                            final Set<TaskId> prevTasks,
                            final Set<TaskId> standbyTasks,
                            final String userEndPoint) {
        this(LATEST_SUPPORTED_VERSION, processId, prevTasks, standbyTasks, userEndPoint);
    }

    public SubscriptionInfo(final int version,
                            final UUID processId,
                            final Set<TaskId> prevTasks,
                            final Set<TaskId> standbyTasks,
                            final String userEndPoint) {
        this(version, LATEST_SUPPORTED_VERSION, processId, prevTasks, standbyTasks, userEndPoint);

        if (version < 1 || version > LATEST_SUPPORTED_VERSION) {
            throw new IllegalArgumentException("version must be between 1 and " + LATEST_SUPPORTED_VERSION
                + "; was: " + version);
        }
    }

    // for testing only; don't apply version checks
    protected SubscriptionInfo(final int version,
                               final int latestSupportedVersion,
                               final UUID processId,
                               final Set<TaskId> prevTasks,
                               final Set<TaskId> standbyTasks,
                               final String userEndPoint) {
        this.usedVersion = version;
        this.latestSupportedVersion = latestSupportedVersion;
        this.processId = processId;
        this.prevTasks = prevTasks;
        this.standbyTasks = standbyTasks;
        this.userEndPoint = userEndPoint;
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
        final ByteBuffer buf;

        switch (usedVersion) {
            case 1:
                buf = encodeVersionOne();
                break;
            case 2:
                buf = encodeVersionTwo();
                break;
            case 3:
                buf = encodeVersionThree();
                break;
            case 4:
                buf = encodeVersionFour();
                break;
            default:
                throw new IllegalStateException("Unknown metadata version: " + usedVersion
                    + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
        }

        buf.rewind();
        return buf;
    }

    private ByteBuffer encodeVersionOne() {
        final ByteBuffer buf = ByteBuffer.allocate(getVersionOneByteLength());

        buf.putInt(1); // version
        encodeClientUUID(buf);
        encodeTasks(buf, prevTasks);
        encodeTasks(buf, standbyTasks);

        return buf;
    }

    private int getVersionOneByteLength() {
        return 4 + // version
               16 + // client ID
               4 + prevTasks.size() * 8 + // length + prev tasks
               4 + standbyTasks.size() * 8; // length + standby tasks
    }

    protected void encodeClientUUID(final ByteBuffer buf) {
        buf.putLong(processId.getMostSignificantBits());
        buf.putLong(processId.getLeastSignificantBits());
    }

    protected void encodeTasks(final ByteBuffer buf,
                               final Collection<TaskId> taskIds) {
        buf.putInt(taskIds.size());
        for (final TaskId id : taskIds) {
            id.writeTo(buf);
        }
    }

    protected byte[] prepareUserEndPoint() {
        if (userEndPoint == null) {
            return new byte[0];
        } else {
            return userEndPoint.getBytes(StandardCharsets.UTF_8);
        }
    }

    private ByteBuffer encodeVersionTwo() {
        final byte[] endPointBytes = prepareUserEndPoint();

        final ByteBuffer buf = ByteBuffer.allocate(getVersionTwoByteLength(endPointBytes));

        buf.putInt(2); // version
        encodeClientUUID(buf);
        encodeTasks(buf, prevTasks);
        encodeTasks(buf, standbyTasks);
        encodeUserEndPoint(buf, endPointBytes);

        return buf;
    }

    private int getVersionTwoByteLength(final byte[] endPointBytes) {
        return 4 + // version
               16 + // client ID
               4 + prevTasks.size() * 8 + // length + prev tasks
               4 + standbyTasks.size() * 8 + // length + standby tasks
               4 + endPointBytes.length; // length + userEndPoint
    }

    protected void encodeUserEndPoint(final ByteBuffer buf,
                                      final byte[] endPointBytes) {
        if (endPointBytes != null) {
            buf.putInt(endPointBytes.length);
            buf.put(endPointBytes);
        }
    }

    private ByteBuffer encodeVersionThree() {
        final byte[] endPointBytes = prepareUserEndPoint();

        final ByteBuffer buf = ByteBuffer.allocate(getVersionThreeAndFourByteLength(endPointBytes));

        buf.putInt(3); // used version
        buf.putInt(LATEST_SUPPORTED_VERSION); // supported version
        encodeClientUUID(buf);
        encodeTasks(buf, prevTasks);
        encodeTasks(buf, standbyTasks);
        encodeUserEndPoint(buf, endPointBytes);

        return buf;
    }

    private ByteBuffer encodeVersionFour() {
        final byte[] endPointBytes = prepareUserEndPoint();

        final ByteBuffer buf = ByteBuffer.allocate(getVersionThreeAndFourByteLength(endPointBytes));

        buf.putInt(4); // used version
        buf.putInt(LATEST_SUPPORTED_VERSION); // supported version
        encodeClientUUID(buf);
        encodeTasks(buf, prevTasks);
        encodeTasks(buf, standbyTasks);
        encodeUserEndPoint(buf, endPointBytes);

        return buf;
    }

    protected int getVersionThreeAndFourByteLength(final byte[] endPointBytes) {
        return 4 + // used version
               4 + // latest supported version version
               16 + // client ID
               4 + prevTasks.size() * 8 + // length + prev tasks
               4 + standbyTasks.size() * 8 + // length + standby tasks
               4 + endPointBytes.length; // length + userEndPoint
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data
     */
    public static SubscriptionInfo decode(final ByteBuffer data) {
        final SubscriptionInfo subscriptionInfo;

        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        final int usedVersion = data.getInt();
        final int latestSupportedVersion;
        switch (usedVersion) {
            case 1:
                subscriptionInfo = new SubscriptionInfo(usedVersion, UNKNOWN);
                decodeVersionOneData(subscriptionInfo, data);
                break;
            case 2:
                subscriptionInfo = new SubscriptionInfo(usedVersion, UNKNOWN);
                decodeVersionTwoData(subscriptionInfo, data);
                break;
            case 3:
            case 4:
                latestSupportedVersion = data.getInt();
                subscriptionInfo = new SubscriptionInfo(usedVersion, latestSupportedVersion);
                decodeVersionThreeData(subscriptionInfo, data);
                break;
            default:
                latestSupportedVersion = data.getInt();
                subscriptionInfo = new SubscriptionInfo(usedVersion, latestSupportedVersion);
                log.info("Unable to decode subscription data: used version: {}; latest supported version: {}", usedVersion, LATEST_SUPPORTED_VERSION);
        }

        return subscriptionInfo;
    }

    private static void decodeVersionOneData(final SubscriptionInfo subscriptionInfo,
                                             final ByteBuffer data) {
        decodeClientUUID(subscriptionInfo, data);
        decodeTasks(subscriptionInfo, data);
    }

    private static void decodeClientUUID(final SubscriptionInfo subscriptionInfo,
                                         final ByteBuffer data) {
        subscriptionInfo.processId = new UUID(data.getLong(), data.getLong());
    }

    private static void decodeTasks(final SubscriptionInfo subscriptionInfo,
                                    final ByteBuffer data) {
        subscriptionInfo.prevTasks = new HashSet<>();
        final int numPrevTasks = data.getInt();
        for (int i = 0; i < numPrevTasks; i++) {
            subscriptionInfo.prevTasks.add(TaskId.readFrom(data));
        }

        subscriptionInfo.standbyTasks = new HashSet<>();
        final int numStandbyTasks = data.getInt();
        for (int i = 0; i < numStandbyTasks; i++) {
            subscriptionInfo.standbyTasks.add(TaskId.readFrom(data));
        }
    }

    private static void decodeVersionTwoData(final SubscriptionInfo subscriptionInfo,
                                             final ByteBuffer data) {
        decodeClientUUID(subscriptionInfo, data);
        decodeTasks(subscriptionInfo, data);
        decodeUserEndPoint(subscriptionInfo, data);
    }

    private static void decodeUserEndPoint(final SubscriptionInfo subscriptionInfo,
                                           final ByteBuffer data) {
        final int bytesLength = data.getInt();
        if (bytesLength != 0) {
            final byte[] bytes = new byte[bytesLength];
            data.get(bytes);
            subscriptionInfo.userEndPoint = new String(bytes, StandardCharsets.UTF_8);
        }
    }

    private static void decodeVersionThreeData(final SubscriptionInfo subscriptionInfo,
                                               final ByteBuffer data) {
        decodeClientUUID(subscriptionInfo, data);
        decodeTasks(subscriptionInfo, data);
        decodeUserEndPoint(subscriptionInfo, data);
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
        if (o instanceof SubscriptionInfo) {
            final SubscriptionInfo other = (SubscriptionInfo) o;
            return this.usedVersion == other.usedVersion &&
                    this.latestSupportedVersion == other.latestSupportedVersion &&
                    this.processId.equals(other.processId) &&
                    this.prevTasks.equals(other.prevTasks) &&
                    this.standbyTasks.equals(other.standbyTasks) &&
                    this.userEndPoint != null ? this.userEndPoint.equals(other.userEndPoint) : other.userEndPoint == null;
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
