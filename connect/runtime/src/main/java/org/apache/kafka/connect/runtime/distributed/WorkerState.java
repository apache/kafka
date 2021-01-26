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
public class WorkerState {

    public static ByteBuffer toByteBuffer(WorkerState workerState) {
        return MessageUtil.toVersionPrefixedByteBuffer(ExtendedWorkerMetadata.LOWEST_SUPPORTED_VERSION,
                new ExtendedWorkerMetadata()
                        .setUrl(workerState.url())
                        .setConfigOffset(workerState.offset()));
    }

    /**
     * Given a byte buffer that contains protocol metadata return the deserialized form of the
     * metadata.
     *
     * @param buffer A buffer containing the protocols metadata
     * @return the deserialized metadata
     * @throws SchemaException on incompatible Connect protocol version
     */
    static WorkerState of(ByteBuffer buffer) {
        short version = buffer.getShort();
        if (version >= ExtendedWorkerMetadata.LOWEST_SUPPORTED_VERSION && version <= ExtendedWorkerMetadata.HIGHEST_SUPPORTED_VERSION) {
            ExtendedWorkerMetadata metadata = new ExtendedWorkerMetadata(new ByteBufferAccessor(buffer), version);
            return new WorkerState(metadata.url(), metadata.configOffset());
        } else throw new SchemaException("Unsupported subscription version: " + version);
    }

    private final String url;
    private final long offset;

    public WorkerState(String url, long offset) {
        this.url = url;
        this.offset = offset;
    }

    public String url() {
        return url;
    }

    /**
     * The most up-to-date (maximum) configuration offset according known to this worker.
     *
     * @return the configuration offset
     */
    public long offset() {
        return offset;
    }

    @Override
    public String toString() {
        return "WorkerState{" +
                "url='" + url + '\'' +
                ", offset=" + offset +
                '}';
    }
}
