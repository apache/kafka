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

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.state.HostInfo;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.Assert.assertEquals;

public class AssignmentInfoTest {

    @Test
    public void testEncodeDecode() {
        List<TaskId> activeTasks =
                Arrays.asList(new TaskId(0, 0), new TaskId(0, 0), new TaskId(0, 1), new TaskId(1, 0));
        Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        standbyTasks.put(new TaskId(1, 1), Utils.mkSet(new TopicPartition("t1", 1), new TopicPartition("t2", 1)));
        standbyTasks.put(new TaskId(2, 0), Utils.mkSet(new TopicPartition("t3", 0), new TopicPartition("t3", 0)));

        AssignmentInfo info = new AssignmentInfo(activeTasks, standbyTasks, new HashMap<HostInfo, Set<TopicPartition>>());
        AssignmentInfo decoded = AssignmentInfo.decode(info.encode());

        assertEquals(info, decoded);
    }

    @Test
    public void shouldDecodePreviousVersion() throws Exception {
        List<TaskId> activeTasks =
                Arrays.asList(new TaskId(0, 0), new TaskId(0, 0), new TaskId(0, 1), new TaskId(1, 0));
        Map<TaskId, Set<TopicPartition>> standbyTasks = new HashMap<>();

        standbyTasks.put(new TaskId(1, 1), Utils.mkSet(new TopicPartition("t1", 1), new TopicPartition("t2", 1)));
        standbyTasks.put(new TaskId(2, 0), Utils.mkSet(new TopicPartition("t3", 0), new TopicPartition("t3", 0)));
        final AssignmentInfo oldVersion = new AssignmentInfo(1, activeTasks, standbyTasks, null);
        final AssignmentInfo decoded = AssignmentInfo.decode(encodeV1(oldVersion));
        assertEquals(oldVersion.activeTasks, decoded.activeTasks);
        assertEquals(oldVersion.standbyTasks, decoded.standbyTasks);
        assertEquals(0, decoded.partitionsByHostState.size()); // should be empty as wasn't in V1
        assertEquals(2, decoded.version); // automatically upgraded to v2 on decode;
    }


    /**
     * This is a clone of what the V1 encoding did. The encode method has changed for V2
     * so it is impossible to test compatibility without having this
     */
    private ByteBuffer encodeV1(AssignmentInfo oldVersion) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        // Encode version
        out.writeInt(oldVersion.version);
        // Encode active tasks
        out.writeInt(oldVersion.activeTasks.size());
        for (TaskId id : oldVersion.activeTasks) {
            id.writeTo(out);
        }
        // Encode standby tasks
        out.writeInt(oldVersion.standbyTasks.size());
        for (Map.Entry<TaskId, Set<TopicPartition>> entry : oldVersion.standbyTasks.entrySet()) {
            TaskId id = entry.getKey();
            id.writeTo(out);

            Set<TopicPartition> partitions = entry.getValue();
            out.writeInt(partitions.size());
            for (TopicPartition partition : partitions) {
                out.writeUTF(partition.topic());
                out.writeInt(partition.partition());
            }
        }

        out.flush();
        out.close();

        return ByteBuffer.wrap(baos.toByteArray());

    }

}
