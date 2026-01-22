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
package org.apache.kafka.streams.state;

import java.nio.ByteBuffer;

/**
 * Marker interface to indicate that a bytes store understands the value-with-headers format
 * and can convert legacy "plain value" entries to the new format.
 * <p>
 * Per the KIP, the value format is: [header_length(varint)][headers_bytes][payload_bytes]
 * where payload_bytes is the existing serialized value (e.g., [timestamp(8)][value] for timestamped stores).
 */
public interface HeadersBytesStore {

  /**
   * Converts a legacy value (without headers) to the header-embedded format.
   * <p>
   * For timestamped stores, the legacy format is: [timestamp(8)][value]
   * The new format is: [header_length(2)][headers][timestamp(8)][value]
   * <p>
   * This method adds empty headers to the existing value format.
   *
   * @param key   the key bytes (may be used for context-dependent conversion; typically unused)
   * @param value the legacy value bytes (for timestamped stores: [timestamp(8)][value])
   * @return the value in header-embedded format with empty headers
   */
  static byte[] convertToHeaderFormat(final byte[] key, final byte[] value) {
    return null;
  }
}