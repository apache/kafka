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
 * A PayloadGenerator which generates a random payload that is a uniformly random byte sequence or null.
 *
 * This generator generates pseudo-random payloads that can be reproduced from run to run.
 * The guarantees are the same as those of java.util.Random.
 */
public class RandomNullPayloadGenerator implements PayloadGenerator {
    private final int size;
    private final long seed;
    private final Random random = new Random();
    private final byte[] randomBytes;

    @JsonCreator
    public RandomNullPayloadGenerator(@JsonProperty("size") int size,
                                         @JsonProperty("seed") long seed) {
        this.size = size;
        this.seed = seed;
        random.setSeed(seed);
        this.randomBytes = new byte[size];
    }

    @JsonProperty
    public int size() {
        return size;
    }

    @JsonProperty
    public long seed() {
        return seed;
    }

    @Override
    public synchronized byte[] generate(long position) {
        random.setSeed(seed + position);
        // Call once so we get more varied values.
        random.nextBoolean();
        // Decide if null or byte array
        if (random.nextBoolean()) {
            return null;
        }
        byte[] result = new byte[size];
        random.nextBytes(randomBytes);
        System.arraycopy(randomBytes, 0, result, 0, Math.min(randomBytes.length, result.length));
        return result;
    }
}
