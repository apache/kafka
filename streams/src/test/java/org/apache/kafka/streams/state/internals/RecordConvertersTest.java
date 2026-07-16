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
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.streams.state.internals.RecordConverters.rawListValueToHeadersListValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToHeadersValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToSessionHeadersValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToTimestampedValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

public class RecordConvertersTest {

    private final RecordConverter timestampedValueConverter = rawValueToTimestampedValue();
    private final RecordConverter headersValueConverter = rawValueToHeadersValue();
    private final RecordConverter sessionValueConverter = rawValueToSessionHeadersValue();
    private final RecordConverter listValueConverter = rawListValueToHeadersListValue();

    @SuppressWarnings("unchecked")
    private static final Serde<List<byte[]>> LIST_SERDE = Serdes.ListSerde(ArrayList.class, Serdes.ByteArray());


    @Test
    public void shouldPreserveNullValueOnConversion() {
        final ConsumerRecord<byte[], byte[]> nullValueRecord = new ConsumerRecord<>("", 0, 0L, new byte[0], null);
        assertNull(timestampedValueConverter.convert(nullValueRecord).value());
        assertNull(headersValueConverter.convert(nullValueRecord).value());
        assertNull(sessionValueConverter.convert(nullValueRecord).value());
        assertNull(listValueConverter.convert(nullValueRecord).value());
    }

    @Test
    public void shouldConvertPlainListBlobWhenMarkerAbsent() {
        final byte[] plainBlob = LIST_SERDE.serializer().serialize(null, List.of(new byte[]{1, 42}));
        final ConsumerRecord<byte[], byte[]> inputRecord = new ConsumerRecord<>(
            "topic", 1, 0, 0, TimestampType.CREATE_TIME, 0, 0, new byte[0], plainBlob,
            new RecordHeaders(), Optional.empty());

        final byte[] converted = listValueConverter.convert(inputRecord).value();

        // Each element gained a leading 0x00 empty-headers prefix.
        assertArrayEquals(
            ListValueStoreUpgradeUtils.convertPlainListBlobToHeadersListBlob(plainBlob),
            converted);
    }

    @Test
    public void shouldPassThroughWhenHeadersFormatMarkerPresent() {
        final byte[] alreadyHeadersBlob = LIST_SERDE.serializer().serialize(null, List.of(new byte[]{0, 1, 42}));
        final Headers headers = new RecordHeaders();
        headers.add(ListValueStoreUpgradeUtils.LIST_VALUE_FORMAT_HEADER_KEY, ListValueStoreUpgradeUtils.HEADERS_FORMAT_MARKER);
        final ConsumerRecord<byte[], byte[]> inputRecord = new ConsumerRecord<>(
            "topic", 1, 0, 0, TimestampType.CREATE_TIME, 0, 0, new byte[0], alreadyHeadersBlob,
            headers, Optional.empty());

        // Marked records are already in the on-disk format: returned unchanged (same record instance).
        assertSame(inputRecord, listValueConverter.convert(inputRecord));
    }

    @Test
    public void shouldAddTimestampToValueOnConversionWhenValueIsNotNull() {
        final long timestamp = 10L;
        final byte[] value = new byte[1];
        final ConsumerRecord<byte[], byte[]> inputRecord = new ConsumerRecord<>(
                "topic", 1, 0, timestamp, TimestampType.CREATE_TIME, 0, 0, new byte[0], value,
                new RecordHeaders(), Optional.empty());
        final byte[] expectedValue = ByteBuffer.allocate(9).putLong(timestamp).put(value).array();
        final byte[] actualValue = timestampedValueConverter.convert(inputRecord).value();
        assertArrayEquals(expectedValue, actualValue);
    }

    @Test
    public void shouldAddTimestampAndHeadersToValueOnConversionWhenValueIsNotNull() {
        final long timestamp = 10L;
        final byte[] value = new byte[1];
        final Headers headers = new RecordHeaders().add("header-key", "header-value".getBytes());
        final ConsumerRecord<byte[], byte[]> inputRecord = new ConsumerRecord<>(
            "topic", 1, 0, timestamp, TimestampType.CREATE_TIME, 0, 0, new byte[0], value,
            headers, Optional.empty());
        // Expected format: [headersSize(varint)][headersBytes][timestamp(8)][value]
        final byte[] expectedValue =
            {50, 2, 20, 'h', 'e', 'a', 'd', 'e', 'r', '-', 'k', 'e', 'y', 24, 'h', 'e', 'a', 'd', 'e',
                'r', '-', 'v', 'a', 'l', 'u', 'e', 0, 0, 0, 0, 0, 0, 0, 10, value[0]};
        final byte[] actualValue = headersValueConverter.convert(inputRecord).value();
        assertArrayEquals(expectedValue, actualValue);
    }

    @Test
    public void shouldAddHeadersToValueOnConversionWhenValueIsNotNull() {
        final byte[] value = new byte[1];
        final Headers headers = new RecordHeaders().add("header-key", "header-value".getBytes());
        final ConsumerRecord<byte[], byte[]> inputRecord = new ConsumerRecord<>(
            "topic", 1, 0, 0, TimestampType.CREATE_TIME, 0, 0, new byte[0], value,
            headers, Optional.empty());
        // Expected format: [headersSize(varint)][headersBytes][value]
        final byte[] expectedValue =
            {50, 2, 20, 'h', 'e', 'a', 'd', 'e', 'r', '-', 'k', 'e', 'y', 24, 'h', 'e', 'a', 'd', 'e',
                'r', '-', 'v', 'a', 'l', 'u', 'e', value[0]};
        final byte[] actualValue = sessionValueConverter.convert(inputRecord).value();
        assertArrayEquals(expectedValue, actualValue);
    }

}
