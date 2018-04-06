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
import java.util.Set;
import java.util.UUID;

public abstract class SubscriptionInfo {

    public static final int LATEST_SUPPORTED_VERSION = 2;
    private static final Logger log = LoggerFactory.getLogger(SubscriptionInfo.class);
    protected UUID processId;
    protected Set<TaskId> prevTasks;
    protected Set<TaskId> standbyTasks;
    protected String userEndPoint;


    public SubscriptionInfo() {
    }

    public SubscriptionInfo(final UUID processId,
                            final Set<TaskId> prevTasks,
                            final Set<TaskId> standbyTasks,
                            final String userEndPoint) {
        this.processId = processId;
        this.prevTasks = prevTasks;
        this.standbyTasks = standbyTasks;
        this.userEndPoint = userEndPoint;
    }

    static private SubscriptionInfo createSubscriptionInfo(final int version) {
        switch (version) {
            case 1:
                return new SubscriptionInfoV1();
            case 2:
                return new SubscriptionInfoV2();
            default:
                throw new IllegalStateException("Unknown metadata version: " + version
                        + "; latest supported version: " + LATEST_SUPPORTED_VERSION);

        }
    }

    static public SubscriptionInfo createSubsriptionInfo(final UUID processId,
                                                         final Set<TaskId> prevTasks,
                                                         final Set<TaskId> standbyTasks,
                                                         final String userEndPoint) {
        return new SubscriptionInfoV2(processId, prevTasks, standbyTasks, userEndPoint);
    }

    static public SubscriptionInfo createSubsriptionInfo(final int version,
                                                         final UUID processId,
                                                         final Set<TaskId> prevTasks,
                                                         final Set<TaskId> standbyTasks,
                                                         final String userEndPoint) {
        switch (version) {
            case 1:
                return new SubscriptionInfoV1(processId, prevTasks, standbyTasks, userEndPoint);
            case 2:
                return new SubscriptionInfoV2(processId, prevTasks, standbyTasks, userEndPoint);
            default:
                throw new IllegalStateException("Unknown metadata version: " + version
                        + "; latest supported version: " + LATEST_SUPPORTED_VERSION);

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
        final SubscriptionInfo subscriptionInfo = createSubscriptionInfo(usedVersion);
        subscriptionInfo.doDecode(data);

        return subscriptionInfo;
    }

    abstract public int version();

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

    abstract protected ByteBuffer doEncode(byte[] endPointBytes);

    /**
     * @throws TaskAssignmentException if method fails to encode the data
     */
    public ByteBuffer encode() {
        final ByteBuffer buf = doEncode(prepareUserEndPoint());
        buf.rewind();
        return buf;
    }

    abstract protected int getByteLength(final byte[] endPointBytes);

    private byte[] prepareUserEndPoint() {
        if (userEndPoint == null) {
            return new byte[0];
        } else {
            return userEndPoint.getBytes(Charset.forName("UTF-8"));
        }
    }

    abstract protected void encodeData(final ByteBuffer buf,
                                       final byte[] endPointBytes);

    abstract protected void doDecode(ByteBuffer data);


    @Override
    public int hashCode() {
        final int hashCode = version() ^ processId.hashCode() ^ prevTasks.hashCode() ^ standbyTasks.hashCode();
        if (userEndPoint == null) {
            return hashCode;
        }
        return hashCode ^ userEndPoint.hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (o instanceof SubscriptionInfo) {
            final SubscriptionInfo other = (SubscriptionInfo) o;
            return this.version() == other.version() &&
                    this.processId.equals(other.processId) &&
                    this.prevTasks.equals(other.prevTasks) &&
                    this.standbyTasks.equals(other.standbyTasks) &&
                    this.userEndPoint != null ? this.userEndPoint.equals(other.userEndPoint) : other.userEndPoint == null;
        } else {
            return false;
        }
    }

}
