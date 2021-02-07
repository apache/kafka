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

import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;

/**
 * Serde interface for records written to the Raft log. This class assumes
 * a two-pass serialization, with the first pass used to compute the size of the
 * serialized record, and the second pass to write the object.
 */
public interface RecordSerde<T> {
    /**
     * Get the size of a record. This must be called first before writing
     * the data through {@link #write(Object, ObjectSerializationCache, Writable)}.
     *
     * @param data the record that will be serialized
     * @param serializationCache serialization cache
     * @return the size in bytes of the serialized record
     */
    int recordSize(T data, ObjectSerializationCache serializationCache);

    /**
     * Write the record to the output stream. This must be called after
     * computing the size with {@link #recordSize(Object, ObjectSerializationCache)}.
     * The same {@link ObjectSerializationCache} instance must be used in both calls.
     *
     * @param data the record to serialize and write
     * @param serializationCache serialization cache
     * @param out the output stream to write the record to
     */
    void write(T data, ObjectSerializationCache serializationCache, Writable out);

    /**
     * Read a record from a {@link Readable} input.
     *
     * @param input the input stream to deserialize
     * @param size the size of the record in bytes
     * @return the deserialized record
     */
    T read(Readable input, int size);
}
