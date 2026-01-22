/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.utils.ByteUtils;
import org.apache.kafka.streams.kstream.internals.WrappingNullableDeserializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableDeserializer;

class ValueTimestampHeadersDeserializer<V>
    implements WrappingNullableDeserializer<ValueTimestampHeaders<V>, Void, V> {

  private static final LongDeserializer LONG_DESERIALIZER = new LongDeserializer();

  public final Deserializer<V> valueDeserializer;
  private final Deserializer<Long> timestampDeserializer;
  private final HeadersDeserializer headersDeserializer;

  ValueTimestampHeadersDeserializer(final Deserializer<V> valueDeserializer) {
    Objects.requireNonNull(valueDeserializer);
    this.valueDeserializer = valueDeserializer;
    this.timestampDeserializer = new LongDeserializer();
    this.headersDeserializer = new HeadersDeserializer();
  }

  @Override
  public void configure(final Map<String, ?> configs,
                        final boolean isKey) {
    valueDeserializer.configure(configs, isKey);
    timestampDeserializer.configure(configs, isKey);
    // HeadersDeserializer has no internal config
  }

  @Override
  public ValueTimestampHeaders<V> deserialize(final String topic,
                                              final byte[] valueTimestampHeadersBytes) {
    if (valueTimestampHeadersBytes == null) {
      return null;
    }
    final Headers headers =
        headersDeserializer.deserialize(topic, rawHeaders(valueTimestampHeadersBytes));
    final long timestamp =
        timestampDeserializer.deserialize(topic, rawTimestamp(valueTimestampHeadersBytes));

    final V value =
        valueDeserializer.deserialize(topic, rawValue(valueTimestampHeadersBytes));

    return ValueTimestampHeaders.makeAllowNullable(value, timestamp, headers);
  }

  @Override
  public void close() {
    valueDeserializer.close();
    timestampDeserializer.close();
    // HeadersDeserializer has nothing to close
  }

  static byte[] rawValue(final byte[] recordBytes) {
    if (recordBytes == null) {
      return null;
    }

    final ByteBuffer buffer = ByteBuffer.wrap(recordBytes);

    try {
      // 1. Read the header length using the UNSIGNED/PLAIN varint reader
      // This moves the buffer position to the start of [header_bytes]
      final int headerLength = ByteUtils.readUnsignedVarint(buffer);

      // 2. Calculate the skip distance: header_bytes + 8-byte timestamp
      int offsetToValue = buffer.position() + headerLength + Long.BYTES;

      // 3. Check if the record is long enough to actually contain a value
      if (offsetToValue > recordBytes.length) {
        throw new IllegalArgumentException(
            "Corrupt record: Calculated value offset " + offsetToValue +
                " exceeds total record length " + recordBytes.length
        );
      }

      // 4. Move the pointer to the start of [value_bytes]
      buffer.position(offsetToValue);

      // 5. The remaining bytes in the buffer are the [value_bytes]
      final byte[] value = new byte[buffer.remaining()];
      buffer.get(value);

      return value;

    } catch (Exception e) {
      throw new IllegalArgumentException("Failed to extract raw value from record", e);
    }
  }

  /**
   * Extract the 8-byte timestamp from the layout:
   * [ varint header_length ][ header_bytes ][ 8-byte timestamp ][ value_bytes ]
   */
  private static byte[] rawTimestamp(final byte[] rawBytes) {
    if (rawBytes == null) {
      return null;
    }

    final ByteBuffer buffer = ByteBuffer.wrap(rawBytes);

    try {
      // 1. Read the length of the headers (Plain/Unsigned Varint)
      // This advances the position past the varint itself
      final int headerLength = ByteUtils.readUnsignedVarint(buffer);

      // 2. Skip the header bytes to reach the timestamp
      // New position = [Position after Varint] + [headerLength]
      int timestampOffset = buffer.position() + headerLength;

      // 3. Validate that we have enough bytes remaining for an 8-byte long
      if (timestampOffset + Long.BYTES > rawBytes.length) {
        throw new IllegalArgumentException(
            "Corrupt record: header length " + headerLength +
                " pushes timestamp past end of array."
        );
      }

      // 4. Extract the 8 bytes
      byte[] timestampBytes = new byte[Long.BYTES];
      System.arraycopy(rawBytes, timestampOffset, timestampBytes, 0, Long.BYTES);

      return timestampBytes;

    } catch (Exception e) {
      throw new IllegalArgumentException("Could not parse record structure", e);
    }
  }

  /**
   * Extract the timestamp value directly from the serialized record.
   */
  static long timestamp(final byte[] rawBytes) {
    if (rawBytes == null) {
      throw new IllegalArgumentException("Cannot read timestamp from null bytes");
    }
    return LONG_DESERIALIZER.deserialize(null, rawTimestamp(rawBytes));
  }

  /**
   * Extract the raw headers_bytes segment (between timestamp and value).
   */
  private static byte[] rawHeaders(final byte[] rawBytes) {

    if (rawBytes == null) {
      return null;
    }

    // Safety check: Minimal possible size (1 byte varint + 8 bytes TS)
    if (rawBytes.length < 9) {
      throw new IllegalArgumentException("Record too short to contain headers and timestamp");
    }

    final ByteBuffer buffer = ByteBuffer.wrap(rawBytes);

    // 1. Read Varint (Starts at Index 0)
    // This moves the buffer position to the start of the Header Bytes
    final int headerLength;
    try {
      headerLength = readPlainVarint(buffer);
    } catch (Exception e) {
      throw new IllegalArgumentException("Could not parse Header Length Varint", e);
    }

    // 2. Validation
    // We need at least (headerLength + 8 bytes for Timestamp) remaining
    if (headerLength < 0 || buffer.remaining() < headerLength + Long.BYTES) {
      throw new IllegalArgumentException("Invalid header length: " + headerLength
          + ". Buffer remaining: " + buffer.remaining());
    }

    if (headerLength == 0) {
      return new byte[0];
    }

    // 3. Extract the bytes
    // buffer.get() copies headerLength bytes from the current position into the array
    byte[] headers = new byte[headerLength];
    buffer.get(headers);

    return headers;
  }

  public static int readPlainVarint(ByteBuffer buffer) {
    int value = 0;
    int i = 0;
    byte b;
    while (((b = buffer.get()) & 0x80) != 0) {
      value |= (b & 0x7F) << i;
      i += 7;
      if (i > 28) throw new IllegalArgumentException("Varint too long");
    }
    value |= b << i;
    return value;
  }

  @Override
  public void setIfUnset(final SerdeGetter getter) {
    // ValueTimestampHeadersDeserializer never wraps a null deserializer (or configure would throw),
    // but it may wrap a deserializer that itself wraps a null deserializer.
    initNullableDeserializer(valueDeserializer, getter);
  }
}