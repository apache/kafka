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
package org.apache.kafka.raft;

import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;

public interface RecordSerde<T> {
    /**
     * Create a new context object for to be used when serializing a batch of records.
     * This allows for state to be shared between {@link #recordSize(Object, Object)}
     * and {@link #write(Object, Object, Writable)}, which is useful in order to avoid
     * redundant work (see e.g. {@link org.apache.kafka.common.protocol.ObjectSerializationCache}).
     *
     * @return context object or null if none is needed
     */
    default Object newWriteContext() {
        return null;
    }

    /**
     * Get the size of a record.
     *
     * @param data the record that will be serialized
     * @param context context object created by {@link #newWriteContext()}
     * @return the size in bytes of the serialized record
     */
    int recordSize(T data, Object context);

    /**
     * Write the record to the output stream.
     *
     * @param data the record to serialize and write
     * @param context context object created by {@link #newWriteContext()}
     * @param out the output stream to write the record to
     */
    void write(T data, Object context, Writable out);

    /**
     * Read a record from a {@link Readable} input.
     *
     * @param input the input stream to deserialize
     * @param size the size of the record in bytes
     * @return the deserialized record
     */
    T read(Readable input, int size);
}
