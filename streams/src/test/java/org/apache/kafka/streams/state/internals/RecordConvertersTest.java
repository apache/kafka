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

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;

import static org.apache.kafka.streams.state.internals.ListValueStore.LIST_SERDE;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawListValueToHeadersListValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToHeadersValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToSessionHeadersValue;
import static org.apache.kafka.streams.state.internals.RecordConverters.rawValueToTimestampedValue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

public class RecordConvertersTest {

    private final RecordConverter timestampedValueConverter = rawValueToTimestampedValue();
    private final RecordConverter headersValueConverter = rawValueToHeadersValue();
    private final RecordConverter sessionValueConverter = rawValueToSessionHeadersValue();
    private final RecordConverter listValueConverter = rawListValueToHeadersListValue();


    @Test
    public void shouldPreserveNullValueOnConversion() {
        final ConsumerRecord<byte[], byte[]> nullValueRecord = new ConsumerRecord<>("", 0, 0L, new byte[0], null);
        assertNull(timestampedValueConverter.convert(nullValueRecord).value());
        assertNull(headersValueConverter.convert(nullValueRecord).value());
        assertNull(sessionValueConverter.convert(nullValueRecord).value());
        assertNull(listValueConverter.convert(nullValueRecord).value());
    }

    @Test
    public void shouldGiveEveryElementEmptyHeadersWhenListValueHeadersAbsent() {
        // A legacy record, written before the headers format existed: no control header at all.
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
    public void shouldReInlineListValueHeadersWhenPresent() {
        final byte[] plainBlob = LIST_SERDE.serializer().serialize(null, List.of(new byte[]{1, 42}, new byte[]{0, 7}));
        // Element 0 carries a 3-byte headers section, element 1 none. headersSize is a zigzag varint,
        // so 3 encodes as 6 and 0 encodes as 0.
        final byte[] elementHeaders = {6, 9, 9, 9, 0};
        final Headers headers = new RecordHeaders();
        headers.add(ListValueStoreUpgradeUtils.LIST_VALUE_HEADERS_HEADER_KEY, elementHeaders);
        final ConsumerRecord<byte[], byte[]> inputRecord = new ConsumerRecord<>(
            "topic", 1, 0, 0, TimestampType.CREATE_TIME, 0, 0, new byte[0], plainBlob,
            headers, Optional.empty());

        final List<byte[]> converted =
            LIST_SERDE.deserializer().deserialize(null, listValueConverter.convert(inputRecord).value());

        assertArrayEquals(new byte[]{6, 9, 9, 9, 1, 42}, converted.get(0));
        assertArrayEquals(new byte[]{0, 0, 7}, converted.get(1));
    }

    @Test
    public void shouldRoundTripListValueHeadersThroughSplitAndJoin() {
        final byte[] headersBlob = LIST_SERDE.serializer().serialize(null,
            List.of(new byte[]{6, 9, 9, 9, 1, 42}, new byte[]{0, 0, 7}));

        final ListValueStoreUpgradeUtils.SplitListBlob split =
            ListValueStoreUpgradeUtils.splitHeadersListBlob(headersBlob);
        final Headers headers = new RecordHeaders();
        headers.add(ListValueStoreUpgradeUtils.LIST_VALUE_HEADERS_HEADER_KEY, split.elementHeaders);
        final ConsumerRecord<byte[], byte[]> inputRecord = new ConsumerRecord<>(
            "topic", 1, 0, 0, TimestampType.CREATE_TIME, 0, 0, new byte[0], split.plainListBlob,
            headers, Optional.empty());

        assertArrayEquals(headersBlob, listValueConverter.convert(inputRecord).value());
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
