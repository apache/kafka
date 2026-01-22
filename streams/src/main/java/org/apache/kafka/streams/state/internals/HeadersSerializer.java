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

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Map;

/*
 * Serializer for org.apache.kafka.common.header.Headers using the KIP‑1271 format:
 *
 * headers_bytes :=
 *   varint header_count
 *   repeated header_count times:
 *     varint key_length
 *     key_length bytes of UTF‑8 key
 *     varint value_length   // -1 means null
 *     value_length bytes of value (if value_length >= 0)
 *
 * Note: "varint" here is a signed int encoded with zigzag + LEB128.
 */
public final class HeadersSerializer implements Serializer<Headers> {

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
    // no-op
  }

  @Override
  public byte[] serialize(final String topic, final Headers headers) {
    if (headers == null || !headers.iterator().hasNext()) {
      return new byte[0];
    }

    final ByteArrayOutputStream out = new ByteArrayOutputStream();

    // 1) header_count
    final int headerCount = count(headers);
    writeVarint(headerCount, out);

    // 2) each header in iteration order (order is preserved)
    for (final Header header : headers) {
      final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
      final byte[] valueBytes = header.value();

      // key length + key bytes
      writeVarint(keyBytes.length, out);
      out.writeBytes(keyBytes);

      // value length (-1 for null) + value bytes
      if (valueBytes == null) {
        writeVarint(-1, out);
      } else {
        writeVarint(valueBytes.length, out);
        out.writeBytes(valueBytes);
      }
    }

    byte[] headersBytes = out.toByteArray();
    byte[] headersLength = getLengthAsVarint(headersBytes);
    return ByteBuffer.allocate(headersLength.length + headersBytes.length )
        .put(headersLength)
        .put(headersBytes)
        .array();
  }

  @Override
  public void close() {
    // no-op
  }

  // ----------------------------------------------------------------------
  // Varint encoding (signed int via zigzag + LEB128)
  // ----------------------------------------------------------------------

  private static void writeVarint(final int value, final ByteArrayOutputStream out) {
    // Zigzag transform: map signed int -> unsigned so that small negative
    // and small positive values both get small encodings.
    int v = (value << 1) ^ (value >> 31);

    while ((v & ~0x7F) != 0) {
      out.write((v & 0x7F) | 0x80);
      v >>>= 7;
    }
    out.write(v & 0x7F);
  }

  private static int count(final Headers headers) {
    int c = 0;
    for (final Header ignored : headers) {
      c++;
    }
    return c;
  }

  private byte[] getLengthAsVarint(byte[] data) {
    if (data == null) {
      return encodeVarint(-1); // Handle null as -1
    }
    return encodeVarint(data.length);
  }

  /**
   * Encodes an integer into an unsigned varint (LEB128).
   */
  private byte[] encodeVarint(int value) {
    // We assume value is non-negative and treated as unsigned in the 32-bit range.
    byte[] buffer = new byte[5]; // max 5 bytes for 32-bit varint
    int pos = 0;

    while ((value & ~0x7F) != 0) {
      // Write 7 bits and set continuation bit
      buffer[pos++] = (byte) ((value & 0x7F) | 0x80);
      value >>>= 7;
    }

    // Last byte (no continuation bit)
    buffer[pos++] = (byte) (value & 0x7F);

    // Trim to actual length
    byte[] result = new byte[pos];
    System.arraycopy(buffer, 0, result, 0, pos);
    return result;
  }

}
