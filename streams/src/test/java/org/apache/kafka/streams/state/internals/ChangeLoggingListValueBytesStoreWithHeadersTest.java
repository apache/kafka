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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.internals.LogContext;
import org.apache.kafka.streams.processor.internals.MockStreamsMetrics;
import org.apache.kafka.streams.state.AggregationWithHeaders;
import org.apache.kafka.test.InternalMockProcessorContext;
import org.apache.kafka.test.MockRecordCollector;
import org.apache.kafka.test.TestUtils;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

/**
 * The changelog format is the part of this store that can never be changed again, so these tests pin
 * it down: the value bytes stay in the PLAIN element format an old reader understands, and the
 * per-element headers travel in a reserved record header instead.
 */
public class ChangeLoggingListValueBytesStoreWithHeadersTest {

    private static final String TOPIC = "t";

    @SuppressWarnings("unchecked")
    private static final Serde<List<byte[]>> LIST_SERDE = Serdes.ListSerde(ArrayList.class, Serdes.ByteArray());

    private final MockRecordCollector collector = new MockRecordCollector();
    private final InMemoryKeyValueStore inner = new InMemoryKeyValueStore("list");
    private final ListValueStore listStore = new ListValueStore(inner);
    private final ChangeLoggingListValueBytesStoreWithHeaders store =
        new ChangeLoggingListValueBytesStoreWithHeaders(listStore);

    private final LeftOrRightValueSerde<String, String> plainSerde =
        new LeftOrRightValueSerde<>(Serdes.String(), Serdes.String());
    private final AggregationWithHeadersSerde<LeftOrRightValue<String, String>> headersSerde =
        new AggregationWithHeadersSerde<>(plainSerde);

    private final Bytes key = Bytes.wrap("k".getBytes(StandardCharsets.UTF_8));

    private InternalMockProcessorContext<String, String> context;

    @BeforeEach
    public void before() {
        context = new InternalMockProcessorContext<>(
            TestUtils.tempDirectory(),
            Serdes.String(),
            Serdes.String(),
            collector,
            new ThreadCache(new LogContext("testCache "), 0, new MockStreamsMetrics(new Metrics()))
        );
        context.setTime(42L);
        store.init(context, store);
    }

    @AfterEach
    public void after() {
        store.close();
    }

    private byte[] headersElement(final LeftOrRightValue<String, String> value, final Headers headers) {
        return headersSerde.serializer().serialize(TOPIC, AggregationWithHeaders.make(value, headers));
    }

    private static Headers headers(final String key, final String value) {
        return new RecordHeaders().add(key, value.getBytes(StandardCharsets.UTF_8));
    }

    private byte[] loggedValue(final int index) {
        return (byte[]) collector.collected().get(index).value();
    }

    private Headers loggedHeaders(final int index) {
        return collector.collected().get(index).headers();
    }

    @Test
    public void shouldLogElementsAnOldPlainReaderCanStillDecode() {
        // The regression this whole encoding exists for: if the local [headersSize][headers][flag][value]
        // format reached the changelog, an old PLAIN reader would consume the leading empty-headers 0x00
        // as the LeftOrRightValue flag -- silently turning left values into right ones.
        store.put(key, headersElement(LeftOrRightValue.makeLeftValue("left"), headers("A", "1")));
        store.put(key, headersElement(LeftOrRightValue.makeRightValue("right"), new RecordHeaders()));

        // Read the last logged blob back the way a PLAIN store would.
        final List<byte[]> elements = LIST_SERDE.deserializer().deserialize(null, loggedValue(1));
        assertEquals(2, elements.size());

        final LeftOrRightValue<String, String> first = plainSerde.deserializer().deserialize(TOPIC, new RecordHeaders(), elements.get(0));
        assertEquals("left", first.leftValue());
        assertNull(first.rightValue(), "a left value must not come back as a right value");

        final LeftOrRightValue<String, String> second = plainSerde.deserializer().deserialize(TOPIC, new RecordHeaders(), elements.get(1));
        assertEquals("right", second.rightValue());
        assertNull(second.leftValue());
    }

    @Test
    public void shouldRestoreTheLocalBlobFromTheChangelogRecord() {
        store.put(key, headersElement(LeftOrRightValue.makeLeftValue("a"), headers("A", "1")));
        store.put(key, headersElement(LeftOrRightValue.makeRightValue("b"), headers("B", "2")));

        final ConsumerRecord<byte[], byte[]> changelogRecord = new ConsumerRecord<>(
            "changelog", 0, 0L, 42L, TimestampType.CREATE_TIME, 0, 0,
            key.get(), loggedValue(1), loggedHeaders(1), Optional.empty());

        // What restore reconstructs must be byte-identical to what the local store holds.
        assertArrayEquals(
            inner.get(key),
            RecordConverters.rawListValueToHeadersListValue().convert(changelogRecord).value());
    }

    @Test
    public void shouldCarryPerElementHeadersInTheControlHeader() {
        store.put(key, headersElement(LeftOrRightValue.makeLeftValue("a"), headers("A", "1")));

        final byte[] elementHeaders =
            ListValueStoreUpgradeUtils.elementHeaders(loggedHeaders(0));
        assertNotNull(elementHeaders, "the per-element headers must be on the record");
        assertArrayEquals(
            inner.get(key),
            ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(loggedValue(0), elementHeaders));
    }

    @Test
    public void shouldPreserveTheSourceRecordHeadersOnTheChangelogRecord() {
        context.headers().add("user-header", "u".getBytes(StandardCharsets.UTF_8));

        store.put(key, headersElement(LeftOrRightValue.makeLeftValue("a"), headers("A", "1")));

        assertEquals("u", new String(loggedHeaders(0).lastHeader("user-header").value(), StandardCharsets.UTF_8));
    }

    @Test
    public void shouldNotLeakTheControlHeaderIntoTheLiveRecordHeaders() {
        // The live headers are forwarded downstream, so nothing we attach to the changelog record may
        // show up on them.
        store.put(key, headersElement(LeftOrRightValue.makeLeftValue("a"), headers("A", "1")));

        assertNull(context.headers().lastHeader(ListValueStoreUpgradeUtils.LIST_VALUE_HEADERS_HEADER_KEY));
    }

    @Test
    public void shouldLogNullAndNoControlHeaderOnTombstone() {
        store.put(key, headersElement(LeftOrRightValue.makeLeftValue("a"), headers("A", "1")));
        store.put(key, null);

        assertEquals(2, collector.collected().size());
        assertNull(loggedValue(1));
        assertNull(ListValueStoreUpgradeUtils.elementHeaders(loggedHeaders(1)));
        assertEquals(42L, collector.collected().get(1).timestamp());
    }

    @Test
    public void shouldStoreTheHeadersFormatLocally() {
        // The split is a changelog concern only: on disk the elements keep their inline headers.
        final byte[] element = headersElement(LeftOrRightValue.makeLeftValue("a"), headers("A", "1"));
        store.put(key, element);

        assertArrayEquals(
            LIST_SERDE.serializer().serialize(null, List.of(element)),
            inner.get(key));
    }

    @Test
    public void shouldLogTheWholeListOnEveryAppend() {
        store.put(key, headersElement(LeftOrRightValue.makeLeftValue("a"), new RecordHeaders()));
        store.put(key, headersElement(LeftOrRightValue.makeLeftValue("b"), new RecordHeaders()));

        assertEquals(1, LIST_SERDE.deserializer().deserialize(null, loggedValue(0)).size());
        assertEquals(2, LIST_SERDE.deserializer().deserialize(null, loggedValue(1)).size());
        // One 0x00 prefix per element, so the control header grows with the list.
        assertArrayEquals(new byte[]{0}, ListValueStoreUpgradeUtils.elementHeaders(loggedHeaders(0)));
        assertArrayEquals(new byte[]{0, 0}, ListValueStoreUpgradeUtils.elementHeaders(loggedHeaders(1)));
    }
}
