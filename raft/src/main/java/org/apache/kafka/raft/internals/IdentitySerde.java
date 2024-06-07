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
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.protocol.Readable;
import org.apache.kafka.common.protocol.Writable;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.nio.ByteBuffer;

public class IdentitySerde implements RecordSerde<ByteBuffer> {
    public static final IdentitySerde INSTANCE = new IdentitySerde();

    @Override
    public int recordSize(final ByteBuffer data, final ObjectSerializationCache serializationCache) {
        return data.remaining();
    }

    @Override
    public void write(final ByteBuffer data, final ObjectSerializationCache serializationCache, final Writable out) {
        out.writeByteBuffer(data);
    }

    @Override
    public ByteBuffer read(final Readable input, final int size) {
        return input.readByteBuffer(size);
    }
}
