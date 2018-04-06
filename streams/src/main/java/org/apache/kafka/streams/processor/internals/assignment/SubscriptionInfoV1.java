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

import org.apache.kafka.streams.processor.TaskId;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class SubscriptionInfoV1 extends SubscriptionInfo {
    public SubscriptionInfoV1(UUID processId, Set<TaskId> prevTasks, Set<TaskId> standbyTasks, String userEndPoint) {
        super(processId, prevTasks, standbyTasks, userEndPoint);
    }

    public SubscriptionInfoV1() {

    }

    @Override
    public int version() {
        return 1;
    }

    @Override
    protected ByteBuffer doEncode(byte[] endPointBytes) {
        final ByteBuffer buf = ByteBuffer.allocate(getByteLength(endPointBytes));

        buf.putInt(1); // version
        encodeData(buf, endPointBytes);

        return buf;
    }

    @Override
    protected int getByteLength(final byte[] endPointBytes) {
        return 4 + // version
                16 + // client ID
                4 + prevTasks.size() * 8 + // length + prev tasks
                4 + standbyTasks.size() * 8; // length + standby tasks
    }

    @Override
    protected void encodeData(final ByteBuffer buf,
                              final byte[] endPointBytes) {
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

    @Override
    protected void doDecode(final ByteBuffer data) {
        // decode client UUID
        processId = new UUID(data.getLong(), data.getLong());

        // decode previously active tasks
        final int numPrevs = data.getInt();
        prevTasks = new HashSet<>();
        for (int i = 0; i < numPrevs; i++) {
            TaskId id = TaskId.readFrom(data);
            prevTasks.add(id);
        }

        // decode previously cached tasks
        final int numCached = data.getInt();
        standbyTasks = new HashSet<>();
        for (int i = 0; i < numCached; i++) {
            standbyTasks.add(TaskId.readFrom(data));
        }
    }

}
