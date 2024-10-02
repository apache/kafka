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
package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.streams.processor.RecordContext;
import org.apache.kafka.streams.processor.api.RecordMetadata;

import java.nio.ByteBuffer;
import java.util.Objects;

import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.kafka.common.utils.Utils.getNullableSizePrefixedArray;

public class ProcessorRecordContext implements RecordContext, RecordMetadata {

    private final long timestamp;
    private final long offset;
    private final String topic;
    private final int partition;
    private final Headers headers;

    public ProcessorRecordContext(final long timestamp,
                                  final long offset,
                                  final int partition,
                                  final String topic,
                                  final Headers headers) {
        this.timestamp = timestamp;
        this.offset = offset;
        this.topic = topic;
        this.partition = partition;
        this.headers = Objects.requireNonNull(headers);
    }

    @Override
    public long offset() {
        return offset;
    }

    @Override
    public long timestamp() {
        return timestamp;
    }

    @Override
    public String topic() {
        return topic;
    }

    @Override
    public int partition() {
        return partition;
    }

    @Override
    public Headers headers() {
        return headers;
    }

    public long residentMemorySizeEstimate() {
        long size = 0;
        size += Long.BYTES; // value.context.timestamp
        size += Long.BYTES; // value.context.offset
        if (topic != null) {
            size += topic.toCharArray().length;
        }
        size += Integer.BYTES; // partition
        for (final Header header : headers) {
            size += header.key().toCharArray().length;
            final byte[] value = header.value();
            if (value != null) {
                size += value.length;
            }
        }
        return size;
    }

    public byte[] serialize() {
        final byte[] topicBytes = topic.getBytes(UTF_8);
        final byte[][] headerKeysBytes;
        final byte[][] headerValuesBytes;

        int size = 0;
        size += Long.BYTES; // value.context.timestamp
        size += Long.BYTES; // value.context.offset
        size += Integer.BYTES; // size of topic
        size += topicBytes.length;
        size += Integer.BYTES; // partition
        size += Integer.BYTES; // number of headers

        final Header[] headers = this.headers.toArray();
        headerKeysBytes = new byte[headers.length][];
        headerValuesBytes = new byte[headers.length][];

        for (int i = 0; i < headers.length; i++) {
            size += 2 * Integer.BYTES; // sizes of key and value

            final byte[] keyBytes = headers[i].key().getBytes(UTF_8);
            size += keyBytes.length;
            final byte[] valueBytes = headers[i].value();
            if (valueBytes != null) {
                size += valueBytes.length;
            }

            headerKeysBytes[i] = keyBytes;
            headerValuesBytes[i] = valueBytes;
        }

        final ByteBuffer buffer = ByteBuffer.allocate(size);
        buffer.putLong(timestamp);
        buffer.putLong(offset);

        // not handling the null condition because we believe topic will never be null in cases where we serialize
        buffer.putInt(topicBytes.length);
        buffer.put(topicBytes);

        buffer.putInt(partition);
        buffer.putInt(headerKeysBytes.length);
        for (int i = 0; i < headerKeysBytes.length; i++) {
            buffer.putInt(headerKeysBytes[i].length);
            buffer.put(headerKeysBytes[i]);

            if (headerValuesBytes[i] != null) {
                buffer.putInt(headerValuesBytes[i].length);
                buffer.put(headerValuesBytes[i]);
            } else {
                buffer.putInt(-1);
            }
        }

        return buffer.array();
    }

    public static ProcessorRecordContext deserialize(final ByteBuffer buffer) {
        final long timestamp = buffer.getLong();
        final long offset = buffer.getLong();
        final String topic;
        {
            // we believe the topic will never be null when we serialize
            final byte[] topicBytes = requireNonNull(getNullableSizePrefixedArray(buffer));
            topic = new String(topicBytes, UTF_8);
        }
        final int partition = buffer.getInt();
        final int headerCount = buffer.getInt();
        final Headers headers;
        if (headerCount == -1) { // keep for backward compatibility
            headers = new RecordHeaders();
        } else {
            final Header[] headerArr = new Header[headerCount];
            for (int i = 0; i < headerCount; i++) {
                final byte[] keyBytes = requireNonNull(getNullableSizePrefixedArray(buffer));
                final byte[] valueBytes = getNullableSizePrefixedArray(buffer);
                headerArr[i] = new RecordHeader(new String(keyBytes, UTF_8), valueBytes);
            }
            headers = new RecordHeaders(headerArr);
        }

        return new ProcessorRecordContext(timestamp, offset, partition, topic, headers);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ProcessorRecordContext that = (ProcessorRecordContext) o;
        return timestamp == that.timestamp &&
            offset == that.offset &&
            partition == that.partition &&
            Objects.equals(topic, that.topic) &&
            Objects.equals(headers, that.headers);
    }

    /**
     * Equality is implemented in support of tests, *not* for use in Hash collections, since this class is mutable
     * due to the {@link Headers} field it contains.
     */
    @Deprecated
    @Override
    public int hashCode() {
        throw new UnsupportedOperationException("ProcessorRecordContext is unsafe for use in Hash collections "
                                                    + "due to the mutable Headers field");
    }

    @Override
    public String toString() {
        return "ProcessorRecordContext{" +
            "topic='" + topic + '\'' +
            ", partition=" + partition +
            ", offset=" + offset +
            ", timestamp=" + timestamp +
            ", headers=" + headers +
            '}';
    }
}
