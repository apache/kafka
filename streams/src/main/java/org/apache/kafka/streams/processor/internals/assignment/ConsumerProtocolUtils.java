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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.kafka.streams.processor.internals.assignment.StreamsAssignmentProtocolVersions.MIN_NAMED_TOPOLOGY_VERSION;

/**
 * Utility class for common assignment or consumer protocol utility methods such as de/serialization
 */
public class ConsumerProtocolUtils {

    /**
     * @throws IOException if cannot write to output stream
     */
    public static void writeTaskIdTo(final TaskId taskId, final DataOutputStream out, final int version) throws IOException {
        out.writeInt(taskId.subtopology());
        out.writeInt(taskId.partition());
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            if (taskId.topologyName() != null) {
                out.writeInt(taskId.topologyName().length());
                out.writeChars(taskId.topologyName());
            } else {
                out.writeInt(0);
            }
        } else if (taskId.topologyName() != null) {
            throw new TaskAssignmentException("Named topologies are not compatible with protocol version " + version);
        }
    }

    /**
     * @throws IOException if cannot read from input stream
     */
    public static TaskId readTaskIdFrom(final DataInputStream in, final int version) throws IOException {
        final int subtopology = in.readInt();
        final int partition = in.readInt();
        final String namedTopology;
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            final int numNamedTopologyChars = in.readInt();
            final StringBuilder namedTopologyBuilder = new StringBuilder();
            for (int i = 0; i < numNamedTopologyChars; ++i) {
                namedTopologyBuilder.append(in.readChar());
            }
            namedTopology = namedTopologyBuilder.toString();
        } else {
            namedTopology = null;
        }
        return new TaskId(subtopology, partition, getNamedTopologyOrElseNull(namedTopology));
    }

    public static void writeTaskIdTo(final TaskId taskId, final ByteBuffer buf, final int version) {
        buf.putInt(taskId.subtopology());
        buf.putInt(taskId.partition());
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            if (taskId.topologyName() != null) {
                buf.putInt(taskId.topologyName().length());
                for (final char c : taskId.topologyName().toCharArray()) {
                    buf.putChar(c);
                }
            } else {
                buf.putInt(0);
            }
        } else if (taskId.topologyName() != null) {
            throw new TaskAssignmentException("Named topologies are not compatible with protocol version " + version);
        }
    }
    
    public static TaskId readTaskIdFrom(final ByteBuffer buf, final int version) {
        final int subtopology = buf.getInt();
        final int partition = buf.getInt();
        final String namedTopology;
        if (version >= MIN_NAMED_TOPOLOGY_VERSION) {
            final int numNamedTopologyChars = buf.getInt();
            final StringBuilder namedTopologyBuilder = new StringBuilder();
            for (int i = 0; i < numNamedTopologyChars; ++i) {
                namedTopologyBuilder.append(buf.getChar());
            }
            namedTopology = namedTopologyBuilder.toString();
        } else {
            namedTopology = null;
        }
        return new TaskId(subtopology, partition, getNamedTopologyOrElseNull(namedTopology));
    }

    /**
     * @return the namedTopology name, or null if the passed in namedTopology is null or the empty string
     */
    private static String getNamedTopologyOrElseNull(final String namedTopology) {
        return (namedTopology == null || namedTopology.length() == 0) ?
                null :
                namedTopology;
    }
}
