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
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.state.HeadersBytesStore;

import java.util.ArrayList;
import java.util.List;

/**
 * Helpers for migrating the outer-join {@link ListValueStore} from the pre-headers PLAIN element
 * format to the HEADERS element format (KIP-1271, added for AK 4.4).
 * <p>
 * The store persists, per key, a {@link Serdes#ListSerde} blob whose elements are single serialized
 * values. The element encoding differs by {@code dsl.store.format}:
 * <ul>
 *   <li>PLAIN:   {@code [leftFlag(1B)][rawValue]} (a {@code LeftOrRightValue})</li>
 *   <li>HEADERS: {@code [headersSize(varint)][headersBytes][leftFlag(1B)][rawValue]}
 *       (an {@code AggregationWithHeaders<LeftOrRightValue>})</li>
 * </ul>
 * A PLAIN element becomes a HEADERS element with <em>empty</em> headers simply by prepending a single
 * {@code 0x00} byte (the empty-headers varint) — see {@link HeadersBytesStore#convertToHeaderFormat}.
 * So a whole PLAIN list blob is converted by prepending {@code 0x00} to each element and re-serializing
 * the same {@code ListSerde}.
 */
final class ListValueStoreUpgradeUtils {

    /**
     * Reserved changelog record-header key stamped on records written in HEADERS format, so that the
     * restore-time {@link RecordConverters#rawListValueToHeadersListValue() converter} can tell an
     * already-converted (post-upgrade) blob from a legacy PLAIN blob. Deliberately namespaced to avoid
     * colliding with user headers that ride along on the changelog record.
     */
    static final String LIST_VALUE_FORMAT_HEADER_KEY = "__kafka_streams_list_value_headers_format__";

    static final byte[] HEADERS_FORMAT_MARKER = {(byte) 1};

    // Must match ListValueStore.LIST_SERDE.
    @SuppressWarnings("unchecked")
    private static final Serde<List<byte[]>> LIST_SERDE = Serdes.ListSerde(ArrayList.class, Serdes.ByteArray());

    private ListValueStoreUpgradeUtils() {}

    /**
     * Converts a whole PLAIN list blob into the HEADERS list blob by lifting each element to the
     * empty-headers format. {@code null} (a tombstone / whole-list delete) is passed through.
     */
    static byte[] convertPlainListBlobToHeadersListBlob(final byte[] plainListBlob) {
        if (plainListBlob == null) {
            return null;
        }
        final List<byte[]> plainElements = LIST_SERDE.deserializer().deserialize(null, plainListBlob);
        final List<byte[]> headersElements = new ArrayList<>(plainElements.size());
        for (final byte[] element : plainElements) {
            // convertToHeaderFormat(null) returns null, preserving any null list members.
            headersElements.add(HeadersBytesStore.convertToHeaderFormat(element));
        }
        return LIST_SERDE.serializer().serialize(null, headersElements);
    }

    /**
     * @return {@code true} if the record carries the HEADERS-format marker, i.e. its value blob is
     *         already in the headers format and must NOT be converted again on restore.
     */
    static boolean hasHeadersFormatMarker(final Headers headers) {
        if (headers == null) {
            return false;
        }
        final Header marker = headers.lastHeader(LIST_VALUE_FORMAT_HEADER_KEY);
        return marker != null
            && marker.value() != null
            && marker.value().length > 0
            && marker.value()[0] == HEADERS_FORMAT_MARKER[0];
    }
}
