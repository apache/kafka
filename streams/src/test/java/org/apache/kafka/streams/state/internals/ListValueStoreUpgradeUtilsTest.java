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

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.AggregationWithHeaders;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ListValueStoreUpgradeUtilsTest {

    private static final String TOPIC = "t";
    private static final Headers EMPTY = new RecordHeaders();

    @SuppressWarnings("unchecked")
    private static final Serde<List<byte[]>> LIST_SERDE = Serdes.ListSerde(ArrayList.class, Serdes.ByteArray());

    private final LeftOrRightValueSerde<String, String> plainSerde =
        new LeftOrRightValueSerde<>(Serdes.String(), Serdes.String());
    private final AggregationWithHeadersSerde<LeftOrRightValue<String, String>> headersSerde =
        new AggregationWithHeadersSerde<>(plainSerde);

    private byte[] plainElement(final LeftOrRightValue<String, String> value) {
        return plainSerde.serializer().serialize(TOPIC, EMPTY, value);
    }

    private byte[] plainListBlob(final List<LeftOrRightValue<String, String>> values) {
        final List<byte[]> elements = new ArrayList<>();
        for (final LeftOrRightValue<String, String> v : values) {
            elements.add(plainElement(v));
        }
        return LIST_SERDE.serializer().serialize(null, elements);
    }

    private List<AggregationWithHeaders<LeftOrRightValue<String, String>>> readHeadersListBlob(final byte[] blob) {
        final List<byte[]> elements = LIST_SERDE.deserializer().deserialize(null, blob);
        final List<AggregationWithHeaders<LeftOrRightValue<String, String>>> out = new ArrayList<>();
        for (final byte[] e : elements) {
            out.add(headersSerde.deserializer().deserialize(TOPIC, e));
        }
        return out;
    }

    @Test
    public void shouldConvertRightValueWithoutCorruption() {
        final byte[] plain = plainListBlob(List.of(LeftOrRightValue.makeRightValue("right")));

        final byte[] converted = ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(plain);
        final List<AggregationWithHeaders<LeftOrRightValue<String, String>>> result = readHeadersListBlob(converted);

        assertEquals(1, result.size());
        // The pre-fix bug silently corrupted right values (right -> ight); assert it's intact now.
        assertEquals("right", result.get(0).aggregation().rightValue());
        assertNull(result.get(0).aggregation().leftValue());
        assertFalse(result.get(0).headers().iterator().hasNext(), "lifted headers should be empty");
    }

    @Test
    public void shouldConvertLeftValueThatPreviouslyThrew() {
        final byte[] plain = plainListBlob(List.of(LeftOrRightValue.makeLeftValue("left")));

        final byte[] converted = ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(plain);
        final List<AggregationWithHeaders<LeftOrRightValue<String, String>>> result = readHeadersListBlob(converted);

        assertEquals(1, result.size());
        assertEquals("left", result.get(0).aggregation().leftValue());
        assertNull(result.get(0).aggregation().rightValue());
    }

    @Test
    public void shouldConvertMultiElementMixedList() {
        final byte[] plain = plainListBlob(List.of(
            LeftOrRightValue.makeLeftValue("a"),
            LeftOrRightValue.makeRightValue("b"),
            LeftOrRightValue.makeLeftValue("c")
        ));

        final List<AggregationWithHeaders<LeftOrRightValue<String, String>>> result =
            readHeadersListBlob(ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(plain));

        assertEquals(3, result.size());
        assertEquals("a", result.get(0).aggregation().leftValue());
        assertEquals("b", result.get(1).aggregation().rightValue());
        assertEquals("c", result.get(2).aggregation().leftValue());
    }

    @Test
    public void shouldPassThroughNull() {
        assertNull(ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(null));
    }

    @Test
    public void shouldConvertEmptyListToEmptyList() {
        final byte[] emptyBlob = LIST_SERDE.serializer().serialize(null, new ArrayList<>());

        final byte[] converted = ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(emptyBlob);

        assertEquals(0, LIST_SERDE.deserializer().deserialize(null, converted).size());
    }

    @Test
    public void convertedElementShouldBePlainElementPrefixedWithZeroByte() {
        final byte[] plainEl = plainElement(LeftOrRightValue.makeRightValue("x"));
        final byte[] plain = plainListBlob(List.of(LeftOrRightValue.makeRightValue("x")));

        final List<byte[]> convertedElements =
            LIST_SERDE.deserializer().deserialize(null, ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(plain));

        final byte[] expected = new byte[plainEl.length + 1];
        System.arraycopy(plainEl, 0, expected, 1, plainEl.length); // expected[0] == 0x00
        assertTrue(Arrays.equals(expected, convertedElements.get(0)));
    }
}
