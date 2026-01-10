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
package org.apache.kafka.jmh.util;

import org.apache.kafka.common.utils.Bytes;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Fork(2)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 10, time = 1)
public class BytesCompareBenchmark {
    private static final int TREE_SIZE = 10240;

    @Param({"8", "16", "32", "128", "1024"})
    private int bytes;

    private byte[][] tv;
    private TreeMap<byte[], Integer> oldMap = new TreeMap<>(new HandwrittenLexicoComparator());
    private TreeMap<byte[], Integer> newMap = new TreeMap<>(Bytes.BYTES_LEXICO_COMPARATOR);

    @Setup
    public void setup() {
        tv = new byte[TREE_SIZE][bytes];
        for (int i = 0; i < TREE_SIZE; i++) {
            tv[i][bytes - 4] = (byte) (i >>> 24);
            tv[i][bytes - 3] = (byte) (i >>> 16);
            tv[i][bytes - 2] = (byte) (i >>> 8);
            tv[i][bytes - 1] = (byte) i;
            oldMap.put(tv[i], i);
            newMap.put(tv[i], i);
        }
    }

    @Benchmark
    public void samePrefixLexicoCustom(Blackhole bh) {
        for (int i = 0; i < TREE_SIZE; i++) {
            bh.consume(oldMap.get(tv[i]));
        }
    }

    @Benchmark
    public void samePrefixLexicoJdk(Blackhole bh) {
        for (int i = 0; i < TREE_SIZE; i++) {
            bh.consume(newMap.get(tv[i]));
        }
    }

    static class HandwrittenLexicoComparator implements Bytes.ByteArrayComparator {
        @Override
        public int compare(byte[] buffer1, byte[] buffer2) {
            return compare(buffer1, 0, buffer1.length, buffer2, 0, buffer2.length);
        }

        public int compare(final byte[] buffer1, int offset1, int length1,
                           final byte[] buffer2, int offset2, int length2) {

            // short circuit equal case
            if (buffer1 == buffer2 &&
                    offset1 == offset2 &&
                    length1 == length2) {
                return 0;
            }

            int end1 = offset1 + length1;
            int end2 = offset2 + length2;
            for (int i = offset1, j = offset2; i < end1 && j < end2; i++, j++) {
                int a = buffer1[i] & 0xff;
                int b = buffer2[j] & 0xff;
                if (a != b) {
                    return a - b;
                }
            }
            return length1 - length2;
        }
    }
}
