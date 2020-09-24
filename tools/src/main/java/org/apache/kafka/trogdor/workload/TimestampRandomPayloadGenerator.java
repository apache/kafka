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
import java.util.Random;
import java.nio.ByteBuffer;

/**
 * A PayloadGenerator which generates a timestamped uniform random payload.
 *
 * This generator generates pseudo-random payloads that can be reproduced from run to run.
 * The guarantees are the same as those of java.util.Random.
 *
 * The timestamp used for this class is in milliseconds since epoch, encoded directly to the first several bytes of the
 * payload.
 *
 * This should be used in conjunction with TimestampRecordProcessor in the Consumer to measure true end-to-end latency
 * of a system.
 *
 * `size` - The size in bytes of each message.
 * `timestampBytes` - The amount of bytes to use for the timestamp.  Usually 8.
 * `seed` - Used to initialize Random() to remove some non-determinism.
 *
 * Here is an example spec:
 *
 * {
 *    "type": "timestampRandom",
 *    "size": 512,
 *    "timestampBytes": 8
 * }
 *
 * This will generate a 512-byte random message with the first 8 bytes encoded with the timestamp.
 */
public class TimestampRandomPayloadGenerator implements PayloadGenerator {
    private final int size;
    private final int timestampBytes;
    private final long seed;

    private final byte[] randomBytes;
    private final ByteBuffer buffer;

    private final Random random = new Random();

    @JsonCreator
    public TimestampRandomPayloadGenerator(@JsonProperty("size") int size,
                                           @JsonProperty("timestampBytes") int timestampBytes,
                                           @JsonProperty("seed") long seed) {
        this.size = size;
        this.seed = seed;
        this.timestampBytes = timestampBytes;
        if (size < timestampBytes) {
            throw new RuntimeException("The size of the payload must be greater than or equal to " + timestampBytes + ".");
        }
        if (timestampBytes < Long.BYTES) {
            throw new RuntimeException("The size of the timestamp must be greater than or equal to " + Long.BYTES + ".");
        }
        random.setSeed(seed);
        this.randomBytes = new byte[size - timestampBytes];
        buffer = ByteBuffer.allocate(timestampBytes);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @JsonProperty
    public int size() {
        return size;
    }

    @JsonProperty
    public long seed() {
        return seed;
    }

    @JsonProperty
    public int timestampBytes() {
        return timestampBytes;
    }

    @Override
    public synchronized byte[] generate(long position) {
        // Generate out of order to prevent inclusion of random number generation in latency numbers.
        byte[] result = new byte[size];
        if (randomBytes.length > 0) {
            random.setSeed(seed + position);
            random.nextBytes(randomBytes);
            System.arraycopy(randomBytes, 0, result, timestampBytes, randomBytes.length);
        }

        // Do the timestamp generation as the very last task.
        buffer.clear();
        buffer.putLong(Time.SYSTEM.milliseconds());
        buffer.rewind();
        System.arraycopy(buffer.array(), 0, result, 0, timestampBytes);
        return result;
    }
}
