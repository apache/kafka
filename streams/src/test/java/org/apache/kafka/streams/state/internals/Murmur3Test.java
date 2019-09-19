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
package org.apache.kafka.streams.state.internals;

import static org.junit.Assert.*;

import org.junit.Test;

import java.util.Map;

/**
 * This class was taken from Hive org.apache.hive.common.util;
 * https://github.com/apache/hive/blob/master/storage-api/src/test/org/apache/hive/common/util/TestMurmur3.java
 * Commit: dffa3a16588bc8e95b9d0ab5af295a74e06ef702
 *
 *
 * Tests for Murmur3 variants.
 */
public class Murmur3Test {

    @Test
    public void testMurmur3_32() {
        Map<byte[], Integer> cases = new java.util.HashMap<>();
        cases.put("21".getBytes(), 896581614);
        cases.put("foobar".getBytes(), -328928243);
        cases.put("a-little-bit-long-string".getBytes(), -1479816207);
        cases.put("a-little-bit-longer-string".getBytes(), -153232333);
        cases.put("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8".getBytes(), 13417721);
        cases.put(new byte[]{'a', 'b', 'c'}, 461137560);

        int seed = 123;
        for (Map.Entry c : cases.entrySet()) {
            byte[] b = (byte[]) c.getKey();
            assertEquals(c.getValue(), Murmur3.hash32(b, b.length, seed));
        }
    }

    @Test
    public void testMurmur3_128() {
        Map<byte[], long[]> cases = new java.util.HashMap<>();
        cases.put("21".getBytes(), new long[]{5857341059704281894L, -5288187638297930763L});
        cases.put("foobar".getBytes(), new long[]{-351361463397418609L, 8959716011862540668L});
        cases.put("a-little-bit-long-string".getBytes(), new long[]{8836256500583638442L, -198172363548498523L});
        cases.put("a-little-bit-longer-string".getBytes(), new long[]{1838346159335108511L, 8794688210320490705L});
        cases.put("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8".getBytes(), new long[]{-4024021876037397259L, -1482317706335141238L});
        cases.put(new byte[]{'a', 'b', 'c'}, new long[]{1489494923063836066L, -5440978547625122829L});

        int seed = 123;

        for (Map.Entry c : cases.entrySet()) {
            byte[] b = (byte[]) c.getKey();
            long[] result = Murmur3.hash128(b, 0, b.length, seed);
            assertArrayEquals((long[]) c.getValue(), result);
        }
    }
}