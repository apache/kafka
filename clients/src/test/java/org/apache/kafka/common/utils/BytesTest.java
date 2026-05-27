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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class BytesTest {
    @Test
    public void equalsReflectsContentIdentityAndType() {
        Bytes a = Bytes.wrap(new byte[]{0x01, 0x02, 0x03});
        Bytes b = Bytes.wrap(new byte[]{0x01, 0x02, 0x03});
        Bytes c = Bytes.wrap(new byte[]{0x01, 0x02, 0x04});
        Bytes d = Bytes.wrap(new byte[]{0x01, 0x02});
        Bytes empty1 = Bytes.wrap(new byte[]{});
        Bytes empty2 = Bytes.wrap(new byte[]{});

        assertEquals(a, a);
        assertEquals(a, b);
        assertEquals(b, a);
        assertEquals(empty1, empty2);
        assertNotEquals(a, c);
        assertNotEquals(a, d);
        assertNotEquals(null, a);
        assertNotEquals("notABytesObject", a);
    }

    @Test
    public void hashCodeIsConsistentAndContentBased() {
        Bytes a = Bytes.wrap(new byte[]{0x01, 0x02, 0x03});
        Bytes b = Bytes.wrap(new byte[]{0x01, 0x02, 0x03});
        Bytes c = Bytes.wrap(new byte[]{0x04, 0x05, 0x06});
        Bytes empty = Bytes.wrap(new byte[]{});

        assertEquals(a.hashCode(), a.hashCode());
        assertEquals(a.hashCode(), b.hashCode());
        assertEquals(empty.hashCode(), empty.hashCode());
        assertNotEquals(a.hashCode(), c.hashCode());
    }

    @Test
    public void compareToReflectsLexicographicUnsignedOrdering() {
        Bytes a = Bytes.wrap(new byte[]{0x01, 0x02, 0x03});
        Bytes b = Bytes.wrap(new byte[]{0x01, 0x02, 0x03});
        Bytes smaller = Bytes.wrap(new byte[]{0x01, 0x02});
        Bytes larger = Bytes.wrap(new byte[]{0x01, 0x03});
        Bytes prefix = Bytes.wrap(new byte[]{0x01, 0x02});
        Bytes extended = Bytes.wrap(new byte[]{0x01, 0x02, 0x03});
        Bytes empty = Bytes.wrap(new byte[]{});
        Bytes nonEmpty = Bytes.wrap(new byte[]{0x01});
        Bytes high = Bytes.wrap(new byte[]{(byte) 0xFF});
        Bytes low = Bytes.wrap(new byte[]{0x01});

        assertEquals(0, a.compareTo(a));
        assertEquals(0, a.compareTo(b));
        assertEquals(0, empty.compareTo(Bytes.wrap(new byte[]{})));
        assertTrue(smaller.compareTo(larger) < 0);
        assertTrue(larger.compareTo(smaller) > 0);
        assertTrue(prefix.compareTo(extended) < 0);
        assertTrue(extended.compareTo(prefix) > 0);
        assertTrue(empty.compareTo(nonEmpty) < 0);
        assertTrue(high.compareTo(low) > 0);
    }

    @Test
    public void wrapReturnsBytesBackedBySameArray() {
        byte[] raw = new byte[]{0x01, 0x02, 0x03};
        Bytes wrapped = Bytes.wrap(raw);
        assertSame(raw, wrapped.get());
    }

    @Test
    public void wrapReturnsNullForNullInput() {
        assertNull(Bytes.wrap(null));
    }

    @Test
    public void wrapEmptyArrayProducesEmptyBytes() {
        Bytes wrapped = Bytes.wrap(new byte[]{});
        assertArrayEquals(new byte[]{}, wrapped.get());
    }

    @Test
    public void constructorThrowsForNullInput() {
        assertThrows(NullPointerException.class, () -> new Bytes(null));
    }

    @Test
    public void getReturnsUnderlyingArray() {
        byte[] raw = new byte[]{0x0A, 0x0B, 0x0C};
        Bytes bytes = Bytes.wrap(raw);
        assertArrayEquals(raw, bytes.get());
    }
}
