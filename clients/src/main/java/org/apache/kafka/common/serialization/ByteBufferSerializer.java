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
package org.apache.kafka.common.serialization;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.utils.Utils;

import java.nio.ByteBuffer;

/**
 * {@code ByteBufferSerializer} always {@link ByteBuffer#rewind() rewinds} the position of the input buffer to zero for
 * serialization. A manual rewind is not necessary.
 * <p>
 * Note: any existing buffer position is ignored.
 * <p>
 * The position is also rewound back to zero before {@link #serialize(String, ByteBuffer)}
 * returns.
 */
public class ByteBufferSerializer implements Serializer<ByteBuffer> {
    public byte[] serialize(String topic, ByteBuffer data) {
        if (data == null)
            return null;

        data.rewind();
        return Utils.toNullableArrayZeroCopy(data);
    }

    @Override
    public ByteBuffer serializeToByteBuffer(String topic, Headers headers, ByteBuffer data) {
        if (data == null)
            return null;

        data.rewind();
        return data;
    }
}
