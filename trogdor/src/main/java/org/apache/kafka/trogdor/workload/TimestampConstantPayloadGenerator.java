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
import org.apache.kafka.common.utils.Time;

import java.nio.ByteOrder;
import java.nio.ByteBuffer;

/**
 * A PayloadGenerator which generates a timestamped constant payload.
 *
 * The timestamp used for this class is in milliseconds since epoch, encoded directly to the first several bytes of the
 * payload.
 *
 * This should be used in conjunction with TimestampRecordProcessor in the Consumer to measure true end-to-end latency
 * of a system.
 *
 * `size` - The size in bytes of each message.
 *
 * Here is an example spec:
 *
 * {
 *    "type": "timestampConstant",
 *    "size": 512
 * }
 *
 * This will generate a 512-byte message with the first several bytes encoded with the timestamp.
 */
public class TimestampConstantPayloadGenerator implements PayloadGenerator {
    private final int size;
    private final ByteBuffer buffer;

    @JsonCreator
    public TimestampConstantPayloadGenerator(@JsonProperty("size") int size) {
        this.size = size;
        if (size < Long.BYTES) {
            throw new RuntimeException("The size of the payload must be greater than or equal to " + Long.BYTES + ".");
        }
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @JsonProperty
    public int size() {
        return size;
    }

    @Override
    public synchronized byte[] generate(long position) {
        // Generate the byte array before the timestamp generation.
        byte[] result = new byte[size];

        // Do the timestamp generation as the very last task.
        buffer.clear();
        buffer.putLong(Time.SYSTEM.milliseconds());
        buffer.rewind();
        System.arraycopy(buffer.array(), 0, result, 0, Long.BYTES);
        return result;
    }
}
