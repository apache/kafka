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

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Random;

/**
 * This class behaves identically to TimestampConstantPayloadGenerator, except the message size follows a gaussian
 * distribution.
 *
 * This should be used in conjunction with TimestampRecordProcessor in the Consumer to measure true end-to-end latency
 * of a system.
 *
 * `messageSizeAverage` - The average size in bytes of each message.
 * `messageSizeDeviation` - The standard deviation to use when calculating message size.
 * `messagesUntilSizeChange` - The number of messages to keep at the same size.
 *
 * Here is an example spec:
 *
 * {
 *    "type": "gaussianTimestampConstant",
 *    "messageSizeAverage": 512,
 *    "messageSizeDeviation": 100,
 *    "messagesUntilSizeChange": 100
 * }
 *
 * This will generate messages on a gaussian distribution with an average size each 512-bytes. The message sizes will
 * have a standard deviation of 100 bytes, and the size will only change every 100 messages.  The distribution of
 * messages will be as follows:
 *
 *    The average size of the messages are 512 bytes.
 *    ~68% of the messages are between 412 and 612 bytes
 *    ~95% of the messages are between 312 and 712 bytes
 *    ~99% of the messages are between 212 and 812 bytes
 */

public class GaussianTimestampConstantPayloadGenerator implements PayloadGenerator {
    private final int messageSizeAverage;
    private final double messageSizeDeviation;
    private final int messagesUntilSizeChange;
    private final long seed;

    private final Random random = new Random();
    private final ByteBuffer buffer;

    private int messageTracker = 0;
    private int messageSize = 0;

    @JsonCreator
    public GaussianTimestampConstantPayloadGenerator(@JsonProperty("messageSizeAverage") int messageSizeAverage,
                                                     @JsonProperty("messageSizeDeviation") double messageSizeDeviation,
                                                     @JsonProperty("messagesUntilSizeChange") int messagesUntilSizeChange,
                                                     @JsonProperty("seed") long seed) {
        this.messageSizeAverage = messageSizeAverage;
        this.messageSizeDeviation = messageSizeDeviation;
        this.seed = seed;
        this.messagesUntilSizeChange = messagesUntilSizeChange;
        buffer = ByteBuffer.allocate(Long.BYTES);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @JsonProperty
    public int messageSizeAverage() {
        return messageSizeAverage;
    }

    @JsonProperty
    public double messageSizeDeviation() {
        return messageSizeDeviation;
    }

    @JsonProperty
    public int messagesUntilSizeChange() {
        return messagesUntilSizeChange;
    }

    @JsonProperty
    public long seed() {
        return seed;
    }

    @Override
    public synchronized byte[] generate(long position) {
        // Make the random number generator deterministic for unit tests.
        random.setSeed(seed + position);

        // Calculate the next message size based on a gaussian distribution.
        if ((messageSize == 0) || (messageTracker >= messagesUntilSizeChange)) {
            messageTracker = 0;
            messageSize = Math.max((int) (random.nextGaussian() * messageSizeDeviation) + messageSizeAverage, Long.BYTES);
        }
        messageTracker += 1;

        // Generate the byte array before the timestamp generation.
        byte[] result = new byte[messageSize];

        // Do the timestamp generation as the very last task.
        buffer.clear();
        buffer.putLong(Time.SYSTEM.milliseconds());
        buffer.rewind();
        System.arraycopy(buffer.array(), 0, result, 0, Long.BYTES);
        return result;
    }
}
