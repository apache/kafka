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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.AggregationWithHeaders;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    private byte[] headersElement(final LeftOrRightValue<String, String> value, final Headers headers) {
        return headersSerde.serializer().serialize(TOPIC, AggregationWithHeaders.make(value, headers));
    }

    private byte[] headersListBlob(final List<byte[]> elements) {
        return LIST_SERDE.serializer().serialize(null, elements);
    }

    private static Headers headers(final String key, final String value) {
        return new RecordHeaders().add(key, value.getBytes(StandardCharsets.UTF_8));
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

    // ---------------------------------------------------------------------------------------------
    // changelog split / join: keeping the changelog value in the PLAIN format
    // ---------------------------------------------------------------------------------------------

    @Test
    public void shouldSplitToExactlyTheBytesAPlainStoreWouldHaveWritten() {
        // This is the whole point of the split: whatever headers the elements carry locally, the
        // changelog value must be byte-identical to what a PLAIN store writes, so that an old reader
        // (or a HEADERS -> PLAIN config flip) can still decode it.
        final List<LeftOrRightValue<String, String>> values = List.of(
            LeftOrRightValue.makeLeftValue("left"),
            LeftOrRightValue.makeRightValue("right")
        );
        final byte[] headersBlob = headersListBlob(List.of(
            headersElement(values.get(0), headers("A", "1")),
            headersElement(values.get(1), headers("B", "2"))
        ));

        final ListValueStoreUpgradeUtils.SplitListBlob split =
            ListValueStoreUpgradeUtils.splitHeadersListBlob(headersBlob);

        assertArrayEquals(plainListBlob(values), split.plainListBlob);
    }

    @Test
    public void shouldRoundTripPerElementHeadersThroughSplitAndJoin() {
        final byte[] headersBlob = headersListBlob(List.of(
            headersElement(LeftOrRightValue.makeLeftValue("a"), headers("A", "1")),
            headersElement(LeftOrRightValue.makeRightValue("b"), new RecordHeaders()),
            headersElement(LeftOrRightValue.makeLeftValue("c"), headers("C", "3"))
        ));

        final ListValueStoreUpgradeUtils.SplitListBlob split =
            ListValueStoreUpgradeUtils.splitHeadersListBlob(headersBlob);
        final byte[] rejoined = ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(
            split.plainListBlob, split.elementHeaders);

        assertArrayEquals(headersBlob, rejoined);

        final List<AggregationWithHeaders<LeftOrRightValue<String, String>>> result = readHeadersListBlob(rejoined);
        assertEquals(3, result.size());
        assertEquals("a", result.get(0).aggregation().leftValue());
        assertEquals("1", new String(result.get(0).headers().lastHeader("A").value(), StandardCharsets.UTF_8));
        assertEquals("b", result.get(1).aggregation().rightValue());
        assertFalse(result.get(1).headers().iterator().hasNext(), "middle element should have no headers");
        assertEquals("c", result.get(2).aggregation().leftValue());
        assertEquals("3", new String(result.get(2).headers().lastHeader("C").value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldNotLoseHeadersWithDuplicateKeysAcrossElements() {
        // Two elements both carrying "traceId": safe because record headers are a list, not a map, and
        // because each element's headers stay in their own self-delimiting chunk.
        final byte[] headersBlob = headersListBlob(List.of(
            headersElement(LeftOrRightValue.makeLeftValue("a"), headers("traceId", "first")),
            headersElement(LeftOrRightValue.makeRightValue("b"), headers("traceId", "second"))
        ));

        final ListValueStoreUpgradeUtils.SplitListBlob split =
            ListValueStoreUpgradeUtils.splitHeadersListBlob(headersBlob);
        final List<AggregationWithHeaders<LeftOrRightValue<String, String>>> result = readHeadersListBlob(
            ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(split.plainListBlob, split.elementHeaders));

        assertEquals("first", new String(result.get(0).headers().lastHeader("traceId").value(), StandardCharsets.UTF_8));
        assertEquals("second", new String(result.get(1).headers().lastHeader("traceId").value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldEncodeAnElementWithoutHeadersAsASingleZeroByte() {
        final byte[] headersBlob = headersListBlob(List.of(
            headersElement(LeftOrRightValue.makeRightValue("x"), new RecordHeaders()),
            headersElement(LeftOrRightValue.makeRightValue("y"), new RecordHeaders())
        ));

        final ListValueStoreUpgradeUtils.SplitListBlob split =
            ListValueStoreUpgradeUtils.splitHeadersListBlob(headersBlob);

        // One 0x00 per element, and nothing else: the prefix blob carries its own lengths, so there is
        // no count and no padding.
        assertArrayEquals(new byte[]{0, 0}, split.elementHeaders);
    }

    @Test
    public void shouldTreatAbsentElementHeadersAsEmptyHeadersOnJoin() {
        // A legacy changelog record: PLAIN value, no control header. Restoring it must be identical to
        // lifting every element to empty headers -- i.e. there is only one restore path, not two.
        final byte[] plain = plainListBlob(List.of(
            LeftOrRightValue.makeLeftValue("a"),
            LeftOrRightValue.makeRightValue("b")
        ));

        assertArrayEquals(
            ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(plain),
            ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(plain, null));
        assertArrayEquals(
            ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(plain),
            ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(plain, new byte[0]));
    }

    @Test
    public void shouldPassThroughTombstonesOnSplitAndJoin() {
        final ListValueStoreUpgradeUtils.SplitListBlob split =
            ListValueStoreUpgradeUtils.splitHeadersListBlob(null);

        assertNull(split.plainListBlob);
        assertNull(split.elementHeaders);
        assertNull(ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(null, new byte[]{0}));
    }

    @Test
    public void shouldSplitEmptyListToEmptyPrefixes() {
        final byte[] emptyBlob = LIST_SERDE.serializer().serialize(null, new ArrayList<>());

        final ListValueStoreUpgradeUtils.SplitListBlob split =
            ListValueStoreUpgradeUtils.splitHeadersListBlob(emptyBlob);

        assertEquals(0, LIST_SERDE.deserializer().deserialize(null, split.plainListBlob).size());
        assertArrayEquals(new byte[0], split.elementHeaders);
        assertEquals(0, LIST_SERDE.deserializer().deserialize(
            null,
            ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(split.plainListBlob, split.elementHeaders)
        ).size());
    }

    @Test
    public void shouldReadElementHeadersFromRecordHeaders() {
        final byte[] prefixes = {0, 0};
        final Headers withControlHeader = new RecordHeaders()
            .add(ListValueStoreUpgradeUtils.LIST_VALUE_HEADERS_HEADER_KEY, prefixes);

        assertArrayEquals(prefixes, ListValueStoreUpgradeUtils.elementHeaders(withControlHeader));
        assertNull(ListValueStoreUpgradeUtils.elementHeaders(new RecordHeaders()));
        assertNull(ListValueStoreUpgradeUtils.elementHeaders(null));
    }

    @Test
    public void shouldIgnoreUnrelatedHeadersAppendedAfterTheControlHeader() {
        // ProcessorContextImpl#logChange appends the consistency vector clock to the very Headers
        // instance the changelog store hands it, so the control header must not depend on position.
        final byte[] headersBlob = headersListBlob(List.of(
            headersElement(LeftOrRightValue.makeLeftValue("a"), headers("A", "1"))
        ));
        final ListValueStoreUpgradeUtils.SplitListBlob split =
            ListValueStoreUpgradeUtils.splitHeadersListBlob(headersBlob);

        final Headers recordHeaders = new RecordHeaders()
            .add("user-header", "u".getBytes(StandardCharsets.UTF_8))
            .add(ListValueStoreUpgradeUtils.LIST_VALUE_HEADERS_HEADER_KEY, split.elementHeaders)
            .add("v", new byte[]{0})
            .add("c", new byte[]{1, 2, 3});

        assertArrayEquals(headersBlob, ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(
            split.plainListBlob, ListValueStoreUpgradeUtils.elementHeaders(recordHeaders)));
    }

    @Test
    public void shouldThrowOnTooFewElementHeaderPrefixes() {
        final byte[] plain = plainListBlob(List.of(
            LeftOrRightValue.makeLeftValue("a"),
            LeftOrRightValue.makeRightValue("b")
        ));

        final SerializationException e = assertThrows(SerializationException.class,
            () -> ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(plain, new byte[]{0}));
        assertTrue(e.getMessage().contains("fewer headers prefixes"), e.getMessage());
    }

    @Test
    public void shouldThrowOnTrailingElementHeaderBytes() {
        final byte[] plain = plainListBlob(List.of(LeftOrRightValue.makeLeftValue("a")));

        final SerializationException e = assertThrows(SerializationException.class,
            () -> ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(plain, new byte[]{0, 0}));
        assertTrue(e.getMessage().contains("trailing bytes"), e.getMessage());
    }

    @Test
    public void shouldThrowOnHeadersSizeLongerThanTheElement() {
        final byte[] corrupt = headersListBlob(List.of(new byte[]{20, 1, 2}));

        assertThrows(SerializationException.class,
            () -> ListValueStoreUpgradeUtils.splitHeadersListBlob(corrupt));
    }
}
