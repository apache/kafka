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
package org.apache.kafka.connect.runtime.distributed;

import org.apache.kafka.common.protocol.types.ArrayOf;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.protocol.types.Schema;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.protocol.types.Type;

import java.nio.ByteBuffer;

import static org.apache.kafka.common.protocol.types.Type.NULLABLE_BYTES;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ASSIGNMENT_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONFIG_OFFSET_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONFIG_STATE_V0;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECTOR_ASSIGNMENT_V0;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_HEADER_SCHEMA;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.CONNECT_PROTOCOL_V0;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.ERROR_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.LEADER_URL_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.URL_KEY_NAME;
import static org.apache.kafka.connect.runtime.distributed.ConnectProtocol.VERSION_KEY_NAME;

/**
 * This class implements a group protocol for Kafka Connect workers that support incremental and
 * cooperative rebalancing of connectors and tasks. It includes the format of worker state used when
 * joining the group and distributing assignments, and the format of assignments of connectors
 * and tasks to workers.
 */
public class IncrementalCooperativeConnectProtocol {
    public static final String ALLOCATION_KEY_NAME = "allocation";
    public static final String REVOKED_KEY_NAME = "revoked";
    public static final String SCHEDULED_DELAY_KEY_NAME = "delay";
    public static final short CONNECT_PROTOCOL_V1 = 1;

    /**
     * Connect Protocol Header V1:
     *   Version            => Int16
     */
    private static final Struct CONNECT_PROTOCOL_HEADER_V1 = new Struct(CONNECT_PROTOCOL_HEADER_SCHEMA)
            .set(VERSION_KEY_NAME, CONNECT_PROTOCOL_V1);

    /**
     * Config State V1:
     *   Url                => [String]
     *   ConfigOffset       => Int64
     */
    public static final Schema CONFIG_STATE_V1 = CONFIG_STATE_V0;

    /**
     * Allocation V1
     *   Current Assignment => [Byte]
     */
    public static final Schema ALLOCATION_V1 = new Schema(
            new Field(ALLOCATION_KEY_NAME, NULLABLE_BYTES, null, true, null));

    /**
     * Connector Assignment V1:
     *   Connector          => [String]
     *   Tasks              => [Int32]
     */
    // Assignments for each worker are a set of connectors and tasks. These are categorized by connector ID. A sentinel
    // task ID (CONNECTOR_TASK) is used to indicate the connector itself (i.e. that the assignment includes
    // responsibility for running the Connector instance in addition to any tasks it generates).
    public static final Schema CONNECTOR_ASSIGNMENT_V1 = CONNECTOR_ASSIGNMENT_V0;

    /**
     * Raw (non versioned) assignment V1:
     *   Error              => Int16
     *   Leader             => [String]
     *   LeaderUrl          => [String]
     *   ConfigOffset       => Int64
     *   Assignment         => [Connector Assignment]
     *   Revoked            => [Connector Assignment]
     *   ScheduledDelay     => Int32
     */
    public static final Schema ASSIGNMENT_V1 = new Schema(
            new Field(ERROR_KEY_NAME, Type.INT16),
            new Field(LEADER_KEY_NAME, Type.STRING),
            new Field(LEADER_URL_KEY_NAME, Type.STRING),
            new Field(CONFIG_OFFSET_KEY_NAME, Type.INT64),
            new Field(ASSIGNMENT_KEY_NAME, ArrayOf.nullable(CONNECTOR_ASSIGNMENT_V1), null, true, null),
            new Field(REVOKED_KEY_NAME, ArrayOf.nullable(CONNECTOR_ASSIGNMENT_V1), null, true, null),
            new Field(SCHEDULED_DELAY_KEY_NAME, Type.INT32, null, 0));

    /**
     * The fields are serialized in sequence as follows:
     * Subscription V1:
     *   Version            => Int16
     *   Url                => [String]
     *   ConfigOffset       => Int64
     *   Current Assignment => [Byte]
     */
    public static ByteBuffer serializeMetadata(ExtendedWorkerState workerState) {
        Struct configState = new Struct(CONFIG_STATE_V1)
                .set(URL_KEY_NAME, workerState.url())
                .set(CONFIG_OFFSET_KEY_NAME, workerState.offset());
        // Not a big issue if we embed the protocol version with the assignment in the metadata
        Struct allocation = new Struct(ALLOCATION_V1)
                .set(ALLOCATION_KEY_NAME, serializeAssignment(workerState.assignment));
        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V1.sizeOf()
                                                + CONFIG_STATE_V1.sizeOf(configState)
                                                + ALLOCATION_V1.sizeOf(allocation));
        CONNECT_PROTOCOL_HEADER_V1.writeTo(buffer);
        CONFIG_STATE_V1.write(buffer, configState);
        ALLOCATION_V1.write(buffer, allocation);
        buffer.flip();
        return buffer;
    }

    /**
     * Given a byte buffer that contains protocol metadata return the deserialized form of the
     * metadata.
     *
     * @param buffer A buffer containing the protocols metadata
     * @return the deserialized metadata
     */
    public static ExtendedWorkerState deserializeMetadata(ByteBuffer buffer) {
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct configState = CONFIG_STATE_V1.read(buffer);
        long configOffset = configState.getLong(CONFIG_OFFSET_KEY_NAME);
        String url = configState.getString(URL_KEY_NAME);
        Struct allocation = ALLOCATION_V1.read(buffer);
        // Protocol version is embedded with the assignment in the metadata
        ConnectAssignment assignment = deserializeAssignment(allocation.getBytes(ALLOCATION_KEY_NAME));
        return new ExtendedWorkerState(url, configOffset, assignment);
    }

    /**
     * The fields are serialized in sequence as follows:
     * Complete Assignment V1:
     *   Version            => Int16
     *   Error              => Int16
     *   Leader             => [String]
     *   LeaderUrl          => [String]
     *   ConfigOffset       => Int64
     *   Assignment         => [Connector Assignment]
     *   Revoked            => [Connector Assignment]
     *   ScheduledDelay     => Int32
     */
    public static ByteBuffer serializeAssignment(ConnectAssignment assignment) {
        if (assignment == null || ConnectAssignment.empty().equals(assignment)) {
            return null;
        }
        Struct struct = assignment.toStruct();
        ByteBuffer buffer = ByteBuffer.allocate(CONNECT_PROTOCOL_HEADER_V1.sizeOf()
                                                + ASSIGNMENT_V1.sizeOf(struct));
        CONNECT_PROTOCOL_HEADER_V1.writeTo(buffer);
        ASSIGNMENT_V1.write(buffer, struct);
        buffer.flip();
        return buffer;
    }

    /**
     * Given a byte buffer that contains an assignment as defined by this protocol, return the
     * deserialized form of the assignment.
     *
     * @param buffer the buffer containing a serialized assignment
     * @return the deserialized assignment
     */
    public static ConnectAssignment deserializeAssignment(ByteBuffer buffer) {
        if (buffer == null) {
            return null;
        }
        Struct header = CONNECT_PROTOCOL_HEADER_SCHEMA.read(buffer);
        Short version = header.getShort(VERSION_KEY_NAME);
        checkVersionCompatibility(version);
        Struct struct = ASSIGNMENT_V1.read(buffer);
        return ConnectAssignment.fromStruct(version, struct);
    }

    /**
     * A class that captures the deserialized form of a worker's metadata.
     */
    public static class ExtendedWorkerState extends ConnectProtocol.WorkerState {
        private final ConnectAssignment assignment;

        public ExtendedWorkerState(String url, long offset, ConnectAssignment assignment) {
            super(url, offset);
            this.assignment = assignment != null ? assignment : ConnectAssignment.empty();
        }

        /**
         * This method returns which was the assignment of connectors and tasks on a worker at the
         * moment that its state was captured by this class.
         *
         * @return the assignment of connectors and tasks
         */
        public ConnectAssignment assignment() {
            return assignment;
        }

        @Override
        public String toString() {
            return "WorkerState{" +
                    "url='" + url() + '\'' +
                    ", offset=" + offset() +
                    ", " + assignment +
                    '}';
        }
    }

    private static void checkVersionCompatibility(short version) {
        // check for invalid versions
        if (version < CONNECT_PROTOCOL_V0)
            throw new SchemaException("Unsupported subscription version: " + version);

        // otherwise, assume versions can be parsed
    }

}
