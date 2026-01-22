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
import org.apache.kafka.common.serialization.Deserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

/*
 * Deserializer for org.apache.kafka.common.header.Headers using the KIP‑1271 format:
 *
 * headers_bytes :=
 *   varint header_count
 *   repeated header_count times:
 *     varint key_length
 *     key_length bytes of UTF-8 key
 *     varint value_length   // -1 means null
 *     value_length bytes of value (if value_length >= 0)
 */

public final class HeadersDeserializer implements Deserializer<Headers> {

  @Override
  public void configure(final Map<String, ?> configs, final boolean isKey) {
  }

  @Override
  public Headers deserialize(final String topic, final byte[] data) {
    if (data == null || data.length == 0) {
      return new RecordHeaders();
    }

    final Index idx = new Index();
    final int headerCount = readVarint(data, idx);

    if (headerCount < 0) {
      throw new IllegalArgumentException("Negative headerCount: " + headerCount);
    }

    final RecordHeaders headers = new RecordHeaders();

    for (int i = 0; i < headerCount; i++) {
      // key length
      final int keyLen = readVarint(data, idx);
      if (keyLen < 0) {
        throw new IllegalArgumentException("Negative key length: " + keyLen);
      }

      final String key = new String(data, idx.pos, keyLen, StandardCharsets.UTF_8);
      idx.pos += keyLen;

      // value length (-1 == null)
      final int valueLen = readVarint(data, idx);
      final byte[] value;
      if (valueLen < 0) {
        value = null;
      } else {
        value = new byte[valueLen];
        System.arraycopy(data, idx.pos, value, 0, valueLen);
        idx.pos += valueLen;
      }

      headers.add(key, value);
    }

    return headers;
  }

  @Override
  public void close() {
    // no-op
  }

  private static final class Index {
    int pos = 0;
  }

  /**
   * Read a signed int encoded as zigzag+LEB128 varint.
   */
  private static int readVarint(final byte[] data, final Index idx) {
    int raw = 0;
    int shift = 0;

    while (true) {
      if (idx.pos >= data.length) {
        throw new IllegalArgumentException("Truncated varint at position " + idx.pos);
      }
      final int b = data[idx.pos++] & 0xFF;
      raw |= (b & 0x7F) << shift;
      if ((b & 0x80) == 0) {
        break;
      }
      shift += 7;
      if (shift > 28) {
        throw new IllegalArgumentException("Varint too long at position " + (idx.pos - 1));
      }
    }

    // inverse zigzag: signed int
    return (raw >>> 1) ^ -(raw & 1);
  }
}