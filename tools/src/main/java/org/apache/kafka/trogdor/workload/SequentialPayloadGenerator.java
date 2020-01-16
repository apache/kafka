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
package org.apache.kafka.trogdor.workload;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * A PayloadGenerator which generates a sequentially increasing payload.
 *
 * The generated number will wrap around to 0 after the maximum value is reached.
 * Payloads bigger than 8 bytes will always just be padded with zeros after byte 8.
 */
public class SequentialPayloadGenerator implements PayloadGenerator {
    private final int size;
    private final long startOffset;
    private final ByteBuffer buf;

    @JsonCreator
    public SequentialPayloadGenerator(@JsonProperty("size") int size,
                                      @JsonProperty("offset") long startOffset) {
        this.size = size;
        this.startOffset = startOffset;
        this.buf = ByteBuffer.allocate(8);
        // Little-endian byte order allows us to support arbitrary lengths more easily,
        // since the first byte is always the lowest-order byte.
        this.buf.order(ByteOrder.LITTLE_ENDIAN);
    }

    @JsonProperty
    public int size() {
        return size;
    }

    @JsonProperty
    public long startOffset() {
        return startOffset;
    }

    @Override
    public synchronized byte[] generate(long position) {
        buf.clear();
        buf.putLong(position + startOffset);
        byte[] result = new byte[size];
        System.arraycopy(buf.array(), 0, result, 0, Math.min(buf.array().length, result.length));
        return result;
    }
}
