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
package org.apache.kafka.coordinator.group.assignor.uniform2.util;

/**
 * Packs a pair of ints in a long, without allocating: the high int in the high 32 bits of the long
 * and the low int in the low 32 bits.
 *
 * <p>Pairs of non-negative ints sort as their longs do: by the high int, then the low int. The
 * assignor sorts pairs this way in a {@code long[]} or a {@link LongHeap}, typically a key in the
 * high int and the element it orders in the low int.
 */
public final class IntPair {
    private IntPair() {}

    /**
     * @param high The high int.
     * @param low  The low int.
     * @return The long holding the pair.
     */
    public static long pack(int high, int low) {
        return ((long) high << 32) | (low & 0xFFFFFFFFL);
    }

    /**
     * @return The high int of the pair.
     */
    public static int high(long pair) {
        return (int) (pair >>> 32);
    }

    /**
     * @return The low int of the pair.
     */
    public static int low(long pair) {
        return (int) pair;
    }
}
