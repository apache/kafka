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

import java.util.Random;

/**
 * A PayloadGenerator which generates a uniform random payload.
 *
 * This generator generates pseudo-random payloads that can be reproduced from run to run.
 * The guarantees are the same as those of java.util.Random.
 *
 * This payload generator also has the option to append padding bytes at the end of the payload.
 * The padding bytes are always the same, no matter what the position is.  This is useful when
 * simulating a partly-compressible stream of user data.
 */
public class UniformRandomPayloadGenerator implements PayloadGenerator {
    private final int size;
    private final long seed;
    private final int padding;
    private final Random random = new Random();
    private final byte[] padBytes;
    private final byte[] randomBytes;

    @JsonCreator
    public UniformRandomPayloadGenerator(@JsonProperty("size") int size,
                                         @JsonProperty("seed") long seed,
                                         @JsonProperty("padding") int padding) {
        this.size = size;
        this.seed = seed;
        this.padding = padding;
        if (padding < 0 || padding > size) {
            throw new RuntimeException("Invalid value " + padding + " for " +
                "padding: the number of padding bytes must not be smaller than " +
                "0 or greater than the total payload size.");
        }
        this.padBytes = new byte[padding];
        random.setSeed(seed);
        random.nextBytes(padBytes);
        this.randomBytes = new byte[size - padding];
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
    public int padding() {
        return padding;
    }

    @Override
    public synchronized byte[] generate(long position) {
        byte[] result = new byte[size];
        if (randomBytes.length > 0) {
            random.setSeed(seed + position);
            random.nextBytes(randomBytes);
            System.arraycopy(randomBytes, 0, result, 0, Math.min(randomBytes.length, result.length));
        }
        if (padBytes.length > 0) {
            System.arraycopy(padBytes, 0, result, randomBytes.length, result.length - randomBytes.length);
        }
        return result;
    }
}
