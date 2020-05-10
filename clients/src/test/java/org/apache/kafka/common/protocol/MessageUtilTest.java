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

package org.apache.kafka.common.protocol;

import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.Timeout;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public final class MessageUtilTest {
    @Rule
    final public Timeout globalTimeout = Timeout.millis(120000);

    @Test
    public void testDeepToString() {
        assertEquals("[1, 2, 3]",
            MessageUtil.deepToString(Arrays.asList(1, 2, 3).iterator()));
        assertEquals("[foo]",
            MessageUtil.deepToString(Arrays.asList("foo").iterator()));
    }

    @Test
    public void testByteBufferToArray() {
        assertArrayEquals(new byte[]{1, 2, 3},
            MessageUtil.byteBufferToArray(ByteBuffer.wrap(new byte[]{1, 2, 3})));
        assertArrayEquals(new byte[]{},
            MessageUtil.byteBufferToArray(ByteBuffer.wrap(new byte[]{})));
    }

    @Test
    public void testDuplicate() {
        assertEquals(null, MessageUtil.duplicate(null));
        assertArrayEquals(new byte[] {},
            MessageUtil.duplicate(new byte[] {}));
        assertArrayEquals(new byte[] {1, 2, 3},
            MessageUtil.duplicate(new byte[] {1, 2, 3}));
    }

    @Test
    public void testCompareRawTaggedFields() {
        assertTrue(MessageUtil.compareRawTaggedFields(null, null));
        assertTrue(MessageUtil.compareRawTaggedFields(null, Collections.emptyList()));
        assertTrue(MessageUtil.compareRawTaggedFields(Collections.emptyList(), null));
        assertFalse(MessageUtil.compareRawTaggedFields(Collections.emptyList(),
            Collections.singletonList(new RawTaggedField(1, new byte[] {1}))));
        assertFalse(MessageUtil.compareRawTaggedFields(null,
            Collections.singletonList(new RawTaggedField(1, new byte[] {1}))));
        assertFalse(MessageUtil.compareRawTaggedFields(
            Collections.singletonList(new RawTaggedField(1, new byte[] {1})),
            Collections.emptyList()));
        assertTrue(MessageUtil.compareRawTaggedFields(
            Arrays.asList(new RawTaggedField(1, new byte[] {1}),
                new RawTaggedField(2, new byte[] {})),
            Arrays.asList(new RawTaggedField(1, new byte[] {1}),
                new RawTaggedField(2, new byte[] {}))));
    }
}
