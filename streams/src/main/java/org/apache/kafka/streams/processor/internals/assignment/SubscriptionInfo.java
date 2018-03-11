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
import java.nio.charset.Charset;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class SubscriptionInfo {

    private static final Logger log = LoggerFactory.getLogger(SubscriptionInfo.class);

    public static final int LATEST_SUPPORTED_VERSION = 2;

    private final int usedVersion;
    private UUID processId;
    private Set<TaskId> prevTasks;
    private Set<TaskId> standbyTasks;
    private String userEndPoint;

    private SubscriptionInfo(final int version) {
        this.usedVersion = version;
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
        this.usedVersion = version;
        this.processId = processId;
        this.prevTasks = prevTasks;
        this.standbyTasks = standbyTasks;
        this.userEndPoint = userEndPoint;
    }

    public int version() {
        return usedVersion;
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
                buf = encodeVersionTwo(prepareUserEndPoint());
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
        encodeVersionOneData(buf);

        return buf;
    }

    private int getVersionOneByteLength() {
        return 4 + // version
               16 + // client ID
               4 + prevTasks.size() * 8 + // length + prev tasks
               4 + standbyTasks.size() * 8; // length + standby tasks
    }

    private void encodeVersionOneData(final ByteBuffer buf) {
        // encode client UUID
        buf.putLong(processId.getMostSignificantBits());
        buf.putLong(processId.getLeastSignificantBits());
        // encode ids of previously running tasks
        buf.putInt(prevTasks.size());
        for (TaskId id : prevTasks) {
            id.writeTo(buf);
        }
        // encode ids of cached tasks
        buf.putInt(standbyTasks.size());
        for (TaskId id : standbyTasks) {
            id.writeTo(buf);
        }
    }

    private byte[] prepareUserEndPoint() {
        if (userEndPoint == null) {
            return new byte[0];
        } else {
            return userEndPoint.getBytes(Charset.forName("UTF-8"));
        }
    }

    private ByteBuffer encodeVersionTwo(final byte[] endPointBytes) {
        final ByteBuffer buf = ByteBuffer.allocate(getVersionTwoByteLength(endPointBytes));

        buf.putInt(2); // version
        encodeVersionTwoData(buf, endPointBytes);

        return buf;
    }

    private int getVersionTwoByteLength(final byte[] endPointBytes) {
        return getVersionOneByteLength() +
               4 + endPointBytes.length; // length + userEndPoint
    }

    private void encodeVersionTwoData(final ByteBuffer buf,
                                      final byte[] endPointBytes) {
        encodeVersionOneData(buf);
        if (endPointBytes != null) {
            buf.putInt(endPointBytes.length);
            buf.put(endPointBytes);
        }
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data
     */
    public static SubscriptionInfo decode(final ByteBuffer data) {
        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        // decode used version
        final int usedVersion = data.getInt();
        final SubscriptionInfo subscriptionInfo = new SubscriptionInfo(usedVersion);

        switch (usedVersion) {
            case 1:
                decodeVersionOneData(subscriptionInfo, data);
                break;
            case 2:
                decodeVersionTwoData(subscriptionInfo, data);
                break;
            default:
                TaskAssignmentException fatalException = new TaskAssignmentException("Unable to decode subscription data: " +
                    "used version: " + usedVersion + "; latest supported version: " + LATEST_SUPPORTED_VERSION);
                log.error(fatalException.getMessage(), fatalException);
                throw fatalException;
        }

        return subscriptionInfo;
    }

    private static void decodeVersionOneData(final SubscriptionInfo subscriptionInfo,
                                             final ByteBuffer data) {
        // decode client UUID
        subscriptionInfo.processId = new UUID(data.getLong(), data.getLong());

        // decode previously active tasks
        final int numPrevs = data.getInt();
        subscriptionInfo.prevTasks = new HashSet<>();
        for (int i = 0; i < numPrevs; i++) {
            TaskId id = TaskId.readFrom(data);
            subscriptionInfo.prevTasks.add(id);
        }

        // decode previously cached tasks
        final int numCached = data.getInt();
        subscriptionInfo.standbyTasks = new HashSet<>();
        for (int i = 0; i < numCached; i++) {
            subscriptionInfo.standbyTasks.add(TaskId.readFrom(data));
        }
    }

    private static void decodeVersionTwoData(final SubscriptionInfo subscriptionInfo,
                                             final ByteBuffer data) {
        decodeVersionOneData(subscriptionInfo, data);

        // decode user end point (can be null)
        int bytesLength = data.getInt();
        if (bytesLength != 0) {
            final byte[] bytes = new byte[bytesLength];
            data.get(bytes);
            subscriptionInfo.userEndPoint = new String(bytes, Charset.forName("UTF-8"));
        }
    }

    @Override
    public int hashCode() {
        final int hashCode = usedVersion ^ processId.hashCode() ^ prevTasks.hashCode() ^ standbyTasks.hashCode();
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
                    this.processId.equals(other.processId) &&
                    this.prevTasks.equals(other.prevTasks) &&
                    this.standbyTasks.equals(other.standbyTasks) &&
                    this.userEndPoint != null ? this.userEndPoint.equals(other.userEndPoint) : other.userEndPoint == null;
        } else {
            return false;
        }
    }

}
