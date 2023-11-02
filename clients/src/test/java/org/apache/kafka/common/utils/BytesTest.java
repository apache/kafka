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
package org.apache.kafka.common.utils;

import org.junit.jupiter.api.Test;

import java.util.Comparator;
import java.util.NavigableMap;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class BytesTest {

    @Test
    public void testIncrement() {
        byte[] input = new byte[]{(byte) 0xAB, (byte) 0xCD, (byte) 0xFF};
        byte[] expected = new byte[]{(byte) 0xAB, (byte) 0xCE, (byte) 0x00};
        Bytes output = Bytes.increment(Bytes.wrap(input));
        assertArrayEquals(output.get(), expected);
    }

    @Test
    public void testIncrementUpperBoundary() {
        byte[] input = new byte[]{(byte) 0xFF, (byte) 0xFF, (byte) 0xFF};
        assertThrows(IndexOutOfBoundsException.class, () -> Bytes.increment(Bytes.wrap(input)));
    }

    @Test
    public void testIncrementWithSubmap() {
        final NavigableMap<Bytes, byte[]> map = new TreeMap<>();
        Bytes key1 = Bytes.wrap(new byte[]{(byte) 0xAA});
        byte[] val = new byte[]{(byte) 0x00};
        map.put(key1, val);

        Bytes key2 = Bytes.wrap(new byte[]{(byte) 0xAA, (byte) 0xAA});
        map.put(key2, val);

        Bytes key3 = Bytes.wrap(new byte[]{(byte) 0xAA, (byte) 0x00, (byte) 0xFF, (byte) 0xFF, (byte) 0xFF});
        map.put(key3, val);

        Bytes key4 = Bytes.wrap(new byte[]{(byte) 0xAB, (byte) 0x00});
        map.put(key4, val);

        Bytes key5 = Bytes.wrap(new byte[]{(byte) 0x00, (byte) 0x00, (byte) 0x00, (byte) 0x01});
        map.put(key5, val);

        Bytes prefix = key1;
        Bytes prefixEnd = Bytes.increment(prefix);

        Comparator<? super Bytes> comparator = map.comparator();
        final int result = comparator == null ? prefix.compareTo(prefixEnd) : comparator.compare(prefix, prefixEnd);
        NavigableMap<Bytes, byte[]> subMapResults;
        if (result > 0) {
            //Prefix increment would cause a wrap-around. Get the submap from toKey to the end of the map
            subMapResults = map.tailMap(prefix, true);
        } else {
            subMapResults = map.subMap(prefix, true, prefixEnd, false);
        }

        NavigableMap<Bytes, byte[]> subMapExpected = new TreeMap<>();
        subMapExpected.put(key1, val);
        subMapExpected.put(key2, val);
        subMapExpected.put(key3, val);

        assertEquals(subMapExpected.keySet(), subMapResults.keySet());
    }
}
