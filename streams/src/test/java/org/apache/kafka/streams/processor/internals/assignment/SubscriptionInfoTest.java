/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class SubscriptionInfoTest {

    @Test
    public void testEncodeDecode() {
        UUID processId = UUID.randomUUID();

        Set<TaskId> activeTasks =
                new HashSet<>(Arrays.asList(new TaskId(0, 0), new TaskId(0, 1), new TaskId(1, 0)));
        Set<TaskId> standbyTasks =
                new HashSet<>(Arrays.asList(new TaskId(1, 1), new TaskId(2, 0)));

        SubscriptionInfo info = new SubscriptionInfo(processId, activeTasks, standbyTasks, null);
        SubscriptionInfo decoded = SubscriptionInfo.decode(info.encode());

        assertEquals(info, decoded);
    }

    @Test
    public void shouldEncodeDecodeWithUserEndPoint() throws Exception {
        SubscriptionInfo original = new SubscriptionInfo(UUID.randomUUID(),
                Collections.singleton(new TaskId(0, 0)), Collections.<TaskId>emptySet(), "localhost:80");
        SubscriptionInfo decoded = SubscriptionInfo.decode(original.encode());
        assertEquals(original, decoded);
    }

    @Test
    public void shouldBeBackwardCompatible() throws Exception {
        UUID processId = UUID.randomUUID();

        Set<TaskId> activeTasks =
                new HashSet<>(Arrays.asList(new TaskId(0, 0), new TaskId(0, 1), new TaskId(1, 0)));
        Set<TaskId> standbyTasks =
                new HashSet<>(Arrays.asList(new TaskId(1, 1), new TaskId(2, 0)));

        final ByteBuffer v1Encoding = encodePreviousVersion(processId, activeTasks, standbyTasks);
        final SubscriptionInfo decode = SubscriptionInfo.decode(v1Encoding);
        assertEquals(activeTasks, decode.prevTasks);
        assertEquals(standbyTasks, decode.standbyTasks);
        assertEquals(processId, decode.processId);
        assertNull(decode.userEndPoint);

    }


    /**
     * This is a clone of what the V1 encoding did. The encode method has changed for V2
     * so it is impossible to test compatibility without having this
     */
    private ByteBuffer encodePreviousVersion(UUID processId,  Set<TaskId> prevTasks, Set<TaskId> standbyTasks) {
        ByteBuffer buf = ByteBuffer.allocate(4 /* version */ + 16 /* process id */ + 4 + prevTasks.size() * 8 + 4 + standbyTasks.size() * 8);
        // version
        buf.putInt(1);
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
        buf.rewind();

        return buf;
    }
}
