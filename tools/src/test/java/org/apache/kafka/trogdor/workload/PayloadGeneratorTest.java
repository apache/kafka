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

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;


public class PayloadGeneratorTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testConstantPayloadGenerator() {
        byte[] alphabet = new byte[26];
        for (int i = 0; i < alphabet.length; i++) {
            alphabet[i] = (byte) ('a' + i);
        }
        byte[] expectedSuperset = new byte[512];
        for (int i = 0; i < expectedSuperset.length; i++) {
            expectedSuperset[i] = (byte) ('a' + (i % 26));
        }
        for (int i : new int[] {1, 5, 10, 100, 511, 512}) {
            ConstantPayloadGenerator generator = new ConstantPayloadGenerator(i, alphabet);
            assertArrayContains(expectedSuperset, generator.generate(0));
            assertArrayContains(expectedSuperset, generator.generate(10));
            assertArrayContains(expectedSuperset, generator.generate(100));
        }
    }

    private static void assertArrayContains(byte[] expectedSuperset, byte[] actual) {
        byte[] expected = new byte[actual.length];
        System.arraycopy(expectedSuperset, 0, expected, 0, expected.length);
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testSequentialPayloadGenerator() {
        SequentialPayloadGenerator g4 = new SequentialPayloadGenerator(4, 1);
        assertLittleEndianArrayEquals(1, g4.generate(0));
        assertLittleEndianArrayEquals(2, g4.generate(1));

        SequentialPayloadGenerator g8 = new SequentialPayloadGenerator(8, 0);
        assertLittleEndianArrayEquals(0, g8.generate(0));
        assertLittleEndianArrayEquals(1, g8.generate(1));
        assertLittleEndianArrayEquals(123123123123L, g8.generate(123123123123L));

        SequentialPayloadGenerator g2 = new SequentialPayloadGenerator(2, 0);
        assertLittleEndianArrayEquals(0, g2.generate(0));
        assertLittleEndianArrayEquals(1, g2.generate(1));
        assertLittleEndianArrayEquals(1, g2.generate(1));
        assertLittleEndianArrayEquals(1, g2.generate(131073));
    }

    private static void assertLittleEndianArrayEquals(long expected, byte[] actual) {
        byte[] longActual = new byte[8];
        System.arraycopy(actual, 0, longActual, 0, Math.min(actual.length, longActual.length));
        ByteBuffer buf = ByteBuffer.wrap(longActual).order(ByteOrder.LITTLE_ENDIAN);
        assertEquals(expected, buf.getLong());
    }

    @Test
    public void testUniformRandomPayloadGenerator() {
        PayloadIterator iter = new PayloadIterator(
            new UniformRandomPayloadGenerator(1234, 456, 0));
        byte[] prev = iter.next();
        for (int uniques = 0; uniques < 1000; ) {
            byte[] cur = iter.next();
            assertEquals(prev.length, cur.length);
            if (!Arrays.equals(prev, cur)) {
                uniques++;
            }
        }
        testReproducible(new UniformRandomPayloadGenerator(1234, 456, 0));
        testReproducible(new UniformRandomPayloadGenerator(1, 0, 0));
        testReproducible(new UniformRandomPayloadGenerator(10, 6, 5));
        testReproducible(new UniformRandomPayloadGenerator(512, 123, 100));
    }

    private static void testReproducible(PayloadGenerator generator) {
        byte[] val = generator.generate(123);
        generator.generate(456);
        byte[] val2 = generator.generate(123);
        assertArrayEquals(val, val2);
    }

    @Test
    public void testUniformRandomPayloadGeneratorPaddingBytes() {
        UniformRandomPayloadGenerator generator =
            new UniformRandomPayloadGenerator(1000, 456, 100);
        byte[] val1 = generator.generate(0);
        byte[] val1End = new byte[100];
        System.arraycopy(val1, 900, val1End, 0, 100);
        byte[] val2 = generator.generate(100);
        byte[] val2End = new byte[100];
        System.arraycopy(val2, 900, val2End, 0, 100);
        byte[] val3 = generator.generate(200);
        byte[] val3End = new byte[100];
        System.arraycopy(val3, 900, val3End, 0, 100);
        assertArrayEquals(val1End, val2End);
        assertArrayEquals(val1End, val3End);
    }

    @Test
    public void testPayloadIterator() {
        final int expectedSize = 50;
        PayloadIterator iter = new PayloadIterator(
            new ConstantPayloadGenerator(expectedSize, new byte[0]));
        final byte[] expected = new byte[expectedSize];
        assertEquals(0, iter.position());
        assertArrayEquals(expected, iter.next());
        assertEquals(1, iter.position());
        assertArrayEquals(expected, iter.next());
        assertArrayEquals(expected, iter.next());
        assertEquals(3, iter.position());
        iter.seek(0);
        assertEquals(0, iter.position());
    }

    @Test
    public void testNullPayloadGenerator() {
        NullPayloadGenerator generator = new NullPayloadGenerator();
        assertEquals(null, generator.generate(0));
        assertEquals(null, generator.generate(1));
        assertEquals(null, generator.generate(100));
    }
}
