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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.BinaryNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import org.apache.kafka.common.protocol.types.RawTaggedField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(120)
public final class MessageUtilTest {

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
        assertNull(MessageUtil.duplicate(null));
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

    @Test
    public void testConstants() {
        assertEquals(MessageUtil.UNSIGNED_SHORT_MAX, 0xFFFF);
        assertEquals(MessageUtil.UNSIGNED_INT_MAX, 0xFFFFFFFFL);
    }

    @Test
    public void testBinaryNode() throws IOException {
        byte[] expected = new byte[] {5, 2, 9, 4, 1, 8, 7, 0, 3, 6};
        StringWriter writer = new StringWriter();
        ObjectMapper mapper = new ObjectMapper();

        mapper.writeTree(mapper.createGenerator(writer), new BinaryNode(expected));

        JsonNode textNode = mapper.readTree(writer.toString());

        assertTrue(textNode.isTextual(), String.format("Expected a JSON string but was: %s", textNode));
        byte[] actual = MessageUtil.jsonNodeToBinary(textNode, "Test base64 JSON string");
        assertArrayEquals(expected, actual);
    }

    @Test
    public void testInvalidBinaryNode() {
        assertThrows(
            IllegalArgumentException.class,
            () -> MessageUtil.jsonNodeToBinary(new IntNode(42), "Test int to binary")
        );
        assertThrows(
            UncheckedIOException.class,
            () -> MessageUtil.jsonNodeToBinary(new TextNode("This is not base64!"), "Test non-base64 to binary")
        );
    }
}
