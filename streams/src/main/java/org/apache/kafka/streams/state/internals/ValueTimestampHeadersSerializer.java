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
import org.apache.kafka.common.serialization.LongSerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.kstream.internals.WrappingNullableSerializer;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.ValueTimestampHeaders;

import java.io.ByteArrayInputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Objects;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.initNullableSerializer;

/**
 * Serializer for {@link ValueTimestampHeaders}, analogous to {@link ValueAndTimestampSerializer}.
 */
public class ValueTimestampHeadersSerializer<V>
    implements WrappingNullableSerializer<ValueTimestampHeaders<V>, Void, V> {

  public final Serializer<V> valueSerializer;
  private final Serializer<Long> timestampSerializer;
  public final HeadersSerializer headersSerializer;

  ValueTimestampHeadersSerializer(final Serializer<V> valueSerializer) {
    Objects.requireNonNull(valueSerializer);
    this.valueSerializer = valueSerializer;
    this.timestampSerializer = new LongSerializer();
    this.headersSerializer = new HeadersSerializer();
  }

  /**
   * Compare two serialized records (produced by this serializer) and return true iff:
   *  - the underlying value bytes are identical, and
   *  - the new timestamp is strictly greater than the old timestamp.
   * <p>
   */
  public static boolean valuesAndHeadersAreSameAndTimeIsIncreasing(final byte[] oldRecord,
                                                         final byte[] newRecord) {
    if (oldRecord == newRecord) {
      // same reference, so they are trivially the same (might both be null)
      return true;
    } else if (oldRecord == null || newRecord == null) {
      // only one is null, so they cannot be the same
      return false;
    } else if (newRecord.length != oldRecord.length) {
      // they are different length, so they cannot be the same
      return false;
    } else if (timeIsDecreasing(oldRecord, newRecord)) {
      // the record time represents the beginning of the validity interval,
      // so if the time moves backwards, we need to do the update regardless
      // of whether the value has changed
      return false;
    } else {
      // all other checks have fallen through, so we actually compare
      // the binary data of the two values
      return valuesAndHeadersAreSame(oldRecord, newRecord);
    }
  }

  @Override
  public void configure(final Map<String, ?> configs,
                        final boolean isKey) {
    valueSerializer.configure(configs, isKey);
    timestampSerializer.configure(configs, isKey);
    headersSerializer.configure(configs, isKey);
  }

  @Override
  public byte[] serialize(final String topic,
                          final ValueTimestampHeaders<V> data) {
    if (data == null) {
      return null;
    }
    return serialize(topic, data.value(), data.timestamp(), data.headers());
  }

  public byte[] serialize(final String topic,
                          final V data,
                          final long timestamp,
                          final Headers headers
                          ) {
    if (data == null) {
      return null;
    }

    final byte[] rawValue = valueSerializer.serialize(topic, headers, data);
    // Since we can't control the result of the internal serializer, we make sure that the result
    // is not null as well.
    //
    // Serializing non-null values to null can be useful when working with Optional-like values
    // where the Optional.empty case is serialized to null.
    // See the discussion here: https://github.com/apache/kafka/pull/7679
    if (rawValue == null) {
      return null;
    }

    final byte[] rawTimestamp = timestampSerializer.serialize(topic, timestamp);
    final byte[] rawHeaders = headersSerializer.serialize(topic, headers);
    return ByteBuffer
        .allocate(rawHeaders.length + rawTimestamp.length + rawValue.length)
        .put(rawHeaders)
        .put(rawTimestamp)
        .put(rawValue)
        .array();
  }

  @Override
  public void close() {
    valueSerializer.close();
    timestampSerializer.close();
    headersSerializer.close();
  }

  private static boolean timeIsDecreasing(final byte[] oldRecord,
                                          final byte[] newRecord) {
    return extractTimestamp(newRecord) <= extractTimestamp(oldRecord);
  }

  private static long extractTimestamp(final byte[] bytes) {

    final byte[] timestampBytes = new byte[Long.BYTES];
    System.arraycopy(bytes, headersFiledLength (bytes), timestampBytes, 0, Long.BYTES);
    return ByteBuffer.wrap(timestampBytes).getLong();
  }


  private static boolean valuesAndHeadersAreSame(final byte[] left,
                                       final byte[] right) {
    int headersFiledLength = headersFiledLength (left);
    // check if headers are same
    for (int i = 0; i < headersFiledLength; i++) {
      if (left[i] != right[i]) {
        return false;
      }
    }
    // check if values are same
    for (int i = headersFiledLength (left) + Long.BYTES; i < left.length; i++) {
      if (left[i] != right[i]) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void setIfUnset(final SerdeGetter getter) {
    // ValueTimestampHeadersSerializer never wraps a null serializer (or configure would throw),
    // but it may wrap a serializer that itself wraps a null serializer.
    initNullableSerializer(valueSerializer, getter);
  }

  private static int readVarint(byte[] data, Integer offset) {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    return readVarint(bais, offset);
  }

  /**
   * Reads an unsigned varint from an Input Stream.
   * This is useful because it advances the "pointer" to the
   * actual data following the varint.
   */
  private static int readVarint(ByteArrayInputStream in, Integer offset) {
    int value = 0;
    int shift = 0;
    int b;

    // Loop until we find a byte with the MSB set to 0
    while (true) {
      b = in.read();
      if (b == -1) throw new RuntimeException("Unexpected end of stream while reading varint");

      // Extract the 7 least significant bits and shift them into place
      value |= (b & 0x7F) << shift;

      // If the MSB (0x80) is not set, this is the last byte of the varint
      if ((b & 0x80) == 0) {
        break;
      }

      shift += 7;
      offset += 1;

      // Safety check for 32-bit integers
      if (shift >= 35) {
        throw new RuntimeException("Varint is too long (overflow)");
      }
    }

    return value;
  }

  private static int headersFiledLength(byte[] bytes) {
    Integer offset = 0;
    int headersLength =  readVarint(bytes, offset);
    return headersLength + offset;
  }
}
