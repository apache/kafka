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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class AssignmentInfo {

    private static final Logger log = LoggerFactory.getLogger(AssignmentInfo.class);

    public final int version;
    public final List<TaskId> activeTasks; // each element corresponds to a partition
    public final Set<TaskId> standbyTasks;

    public AssignmentInfo(List<TaskId> activeTasks, Set<TaskId> standbyTasks) {
        this(1, activeTasks, standbyTasks);
    }

    protected AssignmentInfo(int version, List<TaskId> activeTasks, Set<TaskId> standbyTasks) {
        this.version = version;
        this.activeTasks = activeTasks;
        this.standbyTasks = standbyTasks;
    }

    public ByteBuffer encode() {
        if (version == 1) {
            ByteBuffer buf = ByteBuffer.allocate(4 + 4 + activeTasks.size() * 8 + 4 + standbyTasks.size() * 8);
            // Encode version
            buf.putInt(1);
            // Encode active tasks
            buf.putInt(activeTasks.size());
            for (TaskId id : activeTasks) {
                id.writeTo(buf);
            }
            // Encode standby tasks
            buf.putInt(standbyTasks.size());
            for (TaskId id : standbyTasks) {
                id.writeTo(buf);
            }
            buf.rewind();

            return buf;

        } else {
            TaskAssignmentException ex = new TaskAssignmentException("unable to encode assignment data: version=" + version);
            log.error(ex.getMessage(), ex);
            throw ex;
        }
    }

    public static AssignmentInfo decode(ByteBuffer data) {
        // ensure we are at the beginning of the ByteBuffer
        data.rewind();

        // Decode version
        int version = data.getInt();
        if (version == 1) {
           // Decode active tasks
            int count = data.getInt();
            List<TaskId> activeTasks = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                activeTasks.add(TaskId.readFrom(data));
            }
            // Decode standby tasks
            count = data.getInt();
            Set<TaskId> standbyTasks = new HashSet<>(count);
            for (int i = 0; i < count; i++) {
                standbyTasks.add(TaskId.readFrom(data));
            }

            return new AssignmentInfo(activeTasks, standbyTasks);

        } else {
            TaskAssignmentException ex = new TaskAssignmentException("unknown assignment data version: " + version);
            log.error(ex.getMessage(), ex);
            throw ex;
        }
    }

    @Override
    public int hashCode() {
        return version ^ activeTasks.hashCode() ^ standbyTasks.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof AssignmentInfo) {
            AssignmentInfo other = (AssignmentInfo) o;
            return this.version == other.version &&
                    this.activeTasks.equals(other.activeTasks) &&
                    this.standbyTasks.equals(other.standbyTasks);
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "[version=" + version + ", active tasks=" + activeTasks.size() + ", standby tasks=" + standbyTasks.size() + "]";
    }

}
