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
import org.apache.kafka.common.utils.internals.ByteUtils;

import java.nio.ByteBuffer;

public final class RecordConverters {
    private static final RecordConverter IDENTITY_INSTANCE = record -> record;

    private static final RecordConverter RAW_TO_TIMESTAMED_INSTANCE = record -> {
        final byte[] rawValue = record.value();
        final long timestamp = record.timestamp();
        final byte[] recordValueWithTimestamp = rawValue == null ? null :
            ByteBuffer.allocate(8 + rawValue.length)
                .putLong(timestamp)
                .put(rawValue)
                .array();
        return new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            timestamp,
            record.timestampType(),
            record.serializedKeySize(),
            recordValueWithTimestamp != null ? recordValueWithTimestamp.length : 0,
            record.key(),
            recordValueWithTimestamp,
            record.headers(),
            record.leaderEpoch()
        );
    };

    private static final RecordConverter RAW_TO_WITH_HEADERS_INSTANCE = record -> {
        final byte[] rawValue = record.value();

        // Format: [headersSize(varint)][headersBytes][timestamp(8)][value]
        final byte[] recordValueWithTimestampAndHeaders = reconstructFromRaw(
            rawValue,
            record.timestamp(),
            record.headers()
        );

        return new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.timestampType(),
            record.serializedKeySize(),
            recordValueWithTimestampAndHeaders != null ? recordValueWithTimestampAndHeaders.length : 0,
            record.key(),
            recordValueWithTimestampAndHeaders,
            record.headers(),
            record.leaderEpoch()
        );
    };

    public static RecordConverter rawValueToHeadersValue() {
        return RAW_TO_WITH_HEADERS_INSTANCE;
    }

    private static final RecordConverter RAW_TO_SESSION_WITH_HEADERS_INSTANCE = record -> {
        final byte[] rawValue = record.value();

        // Format: [headersSize(varint)][headersBytes][aggregation] (no timestamp)
        final byte[] recordValueWithHeaders = reconstructSessionFromRaw(
            rawValue,
            record.headers()
        );

        return new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.timestampType(),
            record.serializedKeySize(),
            recordValueWithHeaders != null ? recordValueWithHeaders.length : 0,
            record.key(),
            recordValueWithHeaders,
            record.headers(),
            record.leaderEpoch()
        );
    };

    public static RecordConverter rawValueToSessionHeadersValue() {
        return RAW_TO_SESSION_WITH_HEADERS_INSTANCE;
    }

    private static final RecordConverter RAW_LIST_TO_HEADERS_LIST_INSTANCE = record -> {
        // The outer-join ListValueStore changelog stores the whole list blob, always in the PLAIN
        // element format, with the per-element headers parked in a reserved record header. Restoring
        // means re-inlining them. Legacy records written before the headers format simply lack that
        // header, which is the same as "every element has empty headers" — so there is one path, not
        // two. A tombstone (null value) is passed through.
        if (record.value() == null) {
            return record;
        }

        final byte[] convertedValue = ListValueStoreUpgradeUtils.joinPlainListBlobWithElementHeaders(
            record.value(),
            ListValueStoreUpgradeUtils.elementHeaders(record.headers())
        );

        // Our control header has done its job once the prefixes are back inside the value, so keep it
        // off the restored record. Copy rather than remove in place: the Headers instance belongs to the
        // caller, and a converter must not have side effects on its input. Everything else is passed
        // through, because the restore path reads the position/vector clock back out of these headers
        // (see ChangelogRecordDeserializationHelper#applyChecksAndUpdatePosition).
        final Headers headers = new RecordHeaders(record.headers());
        headers.remove(ListValueStoreUpgradeUtils.LIST_VALUE_HEADERS_HEADER_KEY);

        return new ConsumerRecord<>(
            record.topic(),
            record.partition(),
            record.offset(),
            record.timestamp(),
            record.timestampType(),
            record.serializedKeySize(),
            convertedValue.length,
            record.key(),
            convertedValue,
            headers,
            record.leaderEpoch()
        );
    };

    public static RecordConverter rawListValueToHeadersListValue() {
        return RAW_LIST_TO_HEADERS_LIST_INSTANCE;
    }

    // privatize the constructor so the class cannot be instantiated (only used for its static members)
    private RecordConverters() {}

    public static RecordConverter rawValueToTimestampedValue() {
        return RAW_TO_TIMESTAMED_INSTANCE;
    }

    public static RecordConverter identity() {
        return IDENTITY_INSTANCE;
    }

    /**
     * Reconstructs the AggregationWithHeaders format from raw value bytes and headers (no timestamp).
     * Used during state restoration from changelog topics for session stores.
     *
     * @param rawValue the raw aggregation bytes
     * @param headers the headers
     * @return the serialized AggregationWithHeaders format
     */
    static byte[] reconstructSessionFromRaw(final byte[] rawValue, final Headers headers) {
        if (rawValue == null) {
            return null;
        }

        final HeadersSerializer.PreSerializedHeaders preSerializedHeaders = HeadersSerializer.prepareSerialization(headers);

        final int payloadSize = preSerializedHeaders.requiredBufferSizeForHeaders + rawValue.length;

        // Format: [headersSize(varint)][headersBytes][value]
        final ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(preSerializedHeaders.requiredBufferSizeForHeaders) + payloadSize);
        ByteUtils.writeVarint(preSerializedHeaders.requiredBufferSizeForHeaders, buffer);

        return HeadersSerializer.serialize(preSerializedHeaders, buffer)
            .put(rawValue)
            .array();
    }

    /**
     * Reconstructs the ValueTimestampHeaders format from raw value bytes, timestamp, and headers.
     * Used during state restoration from changelog topics.
     *
     * @param rawValue the raw value bytes
     * @param timestamp the timestamp
     * @param headers the headers
     * @return the serialized ValueTimestampHeaders format
     */
    static byte[] reconstructFromRaw(final byte[] rawValue, final long timestamp, final Headers headers) {
        if (rawValue == null) {
            return null;
        }

        final HeadersSerializer.PreSerializedHeaders preSerializedHeaders = HeadersSerializer.prepareSerialization(headers);

        final int payloadSize = preSerializedHeaders.requiredBufferSizeForHeaders + 8 + rawValue.length;

        // Format: [headersSize(varint)][headersBytes][timestamp(8)][value]
        final ByteBuffer buffer = ByteBuffer.allocate(ByteUtils.sizeOfVarint(preSerializedHeaders.requiredBufferSizeForHeaders) + payloadSize);
        ByteUtils.writeVarint(preSerializedHeaders.requiredBufferSizeForHeaders, buffer);

        return HeadersSerializer.serialize(preSerializedHeaders, buffer)
            .putLong(timestamp)
            .put(rawValue)
            .array();
    }
}
