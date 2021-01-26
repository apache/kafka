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

import org.apache.kafka.common.protocol.ByteBufferAccessor;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.SchemaException;
import org.apache.kafka.connect.internals.generated.ExtendedWorkerMetadata;

import java.nio.ByteBuffer;

/**
 * A class that captures the deserialized form of a worker's metadata.
 */
public class ExtendedWorkerState extends WorkerState {

    public static ByteBuffer toByteBuffer(ExtendedWorkerState workerState, boolean sessioned) {
        short version = sessioned
                ? ConnectProtocolCompatibility.SESSIONED.protocolVersion()
                : ConnectProtocolCompatibility.COMPATIBLE.protocolVersion();
        return MessageUtil.toVersionPrefixedByteBuffer(version, new ExtendedWorkerMetadata()
                .setUrl(workerState.url())
                .setConfigOffset(workerState.offset())
                .setAssignmentByteBuffer(ExtendedAssignment.toByteBuffer(workerState.assignment())));
    }
    /**
     * Given a byte buffer that contains protocol metadata return the deserialized form of the
     * metadata.
     *
     * @param buffer A buffer containing the protocols metadata
     * @return the deserialized metadata
     * @throws SchemaException on incompatible Connect protocol version
     */
    public static ExtendedWorkerState of(ByteBuffer buffer) {
        if (buffer == null) return null;
        short version = buffer.getShort();
        if (version >= ExtendedWorkerMetadata.LOWEST_SUPPORTED_VERSION && version <= ExtendedWorkerMetadata.HIGHEST_SUPPORTED_VERSION) {
            ExtendedWorkerMetadata metadata = new ExtendedWorkerMetadata(new ByteBufferAccessor(buffer), version);
            return new ExtendedWorkerState(
                    metadata.url(),
                    metadata.configOffset(),
                    // Protocol version is embedded with the assignment in the metadata
                    ExtendedAssignment.of(metadata.assignmentByteBuffer()));
        } else throw new SchemaException("Unsupported subscription version: " + version);
    }

    private final ExtendedAssignment assignment;

    public ExtendedWorkerState(String url, long offset, ExtendedAssignment assignment) {
        super(url, offset);
        this.assignment = assignment != null ? assignment : ExtendedAssignment.empty();
    }

    /**
     * This method returns which was the assignment of connectors and tasks on a worker at the
     * moment that its state was captured by this class.
     *
     * @return the assignment of connectors and tasks
     */
    public ExtendedAssignment assignment() {
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
