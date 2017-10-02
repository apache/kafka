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

    private static final int CURRENT_VERSION = 2;

    public final int version;
    public final UUID processId;
    public final Set<TaskId> prevTasks;
    public final Set<TaskId> standbyTasks;
    public final String userEndPoint;

    public SubscriptionInfo(UUID processId, Set<TaskId> prevTasks, Set<TaskId> standbyTasks, String userEndPoint) {
        this(CURRENT_VERSION, processId, prevTasks, standbyTasks, userEndPoint);
    }

    private SubscriptionInfo(int version, UUID processId, Set<TaskId> prevTasks, Set<TaskId> standbyTasks, String userEndPoint) {
        this.version = version;
        this.processId = processId;
        this.prevTasks = prevTasks;
        this.standbyTasks = standbyTasks;
        this.userEndPoint = userEndPoint;
    }

    /**
     * @throws TaskAssignmentException if method fails to encode the data
     */
    public ByteBuffer encode() {
        byte[] endPointBytes;
        if (userEndPoint == null) {
            endPointBytes = new byte[0];
        } else {
            endPointBytes = userEndPoint.getBytes(Charset.forName("UTF-8"));
        }
        ByteBuffer buf = ByteBuffer.allocate(4 /* version */ + 16 /* process id */ + 4 +
                prevTasks.size() * 8 + 4 + standbyTasks.size() * 8
                + 4 /* length of bytes */ + endPointBytes.length
        );
        // version
        buf.putInt(version);
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
        buf.putInt(endPointBytes.length);
        buf.put(endPointBytes);
        buf.rewind();
        return buf;
    }

    /**
     * @throws TaskAssignmentException if method fails to decode the data
     */
    public static SubscriptionInfo decode(ByteBuffer data) {
        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        // Decode version
        int version = data.getInt();
        if (version == CURRENT_VERSION || version == 1) {
            // Decode client UUID
            UUID processId = new UUID(data.getLong(), data.getLong());
            // Decode previously active tasks
            Set<TaskId> prevTasks = new HashSet<>();
            int numPrevs = data.getInt();
            for (int i = 0; i < numPrevs; i++) {
                TaskId id = TaskId.readFrom(data);
                prevTasks.add(id);
            }
            // Decode previously cached tasks
            Set<TaskId> standbyTasks = new HashSet<>();
            int numCached = data.getInt();
            for (int i = 0; i < numCached; i++) {
                standbyTasks.add(TaskId.readFrom(data));
            }

            String userEndPoint = null;
            if (version == CURRENT_VERSION) {
                int bytesLength = data.getInt();
                if (bytesLength != 0) {
                    byte[] bytes = new byte[bytesLength];
                    data.get(bytes);
                    userEndPoint = new String(bytes, Charset.forName("UTF-8"));
                }

            }
            return new SubscriptionInfo(version, processId, prevTasks, standbyTasks, userEndPoint);

        } else {
            TaskAssignmentException ex = new TaskAssignmentException("unable to decode subscription data: version=" + version);
            log.error(ex.getMessage(), ex);
            throw ex;
        }
    }

    @Override
    public int hashCode() {
        int hashCode = version ^ processId.hashCode() ^ prevTasks.hashCode() ^ standbyTasks.hashCode();
        if (userEndPoint == null) {
            return hashCode;
        }
        return hashCode ^ userEndPoint.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SubscriptionInfo) {
            SubscriptionInfo other = (SubscriptionInfo) o;
            return this.version == other.version &&
                    this.processId.equals(other.processId) &&
                    this.prevTasks.equals(other.prevTasks) &&
                    this.standbyTasks.equals(other.standbyTasks) &&
                    this.userEndPoint != null ? this.userEndPoint.equals(other.userEndPoint) : other.userEndPoint == null;
        } else {
            return false;
        }
    }

}
