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

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.internals.ByteUtils;
import org.apache.kafka.streams.state.HeadersBytesStore;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.apache.kafka.streams.state.internals.ListValueStore.LIST_SERDE;

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
 * <p>
 * The HEADERS element format above is the <em>local, on-disk</em> format only. As everywhere else in
 * KIP-1271, the changelog value must keep the pre-headers format so that downgrading — either to an
 * older version or just by flipping {@code dsl.store.format} back to PLAIN — can still decode it. The
 * {@link #splitHeadersListBlob(byte[]) split} / {@link #joinPlainListBlobWithElementHeaders(byte[], byte[]) join}
 * pair moves the per-element headers between the value bytes and a reserved record header for that
 * purpose; {@link #LIST_VALUE_HEADERS_HEADER_KEY} documents the wire encoding.
 */
final class ListValueStoreUpgradeUtils {

    /**
     * Reserved changelog record-header key carrying the per-element headers of a HEADERS-format list,
     * so that the changelog <em>value</em> can stay in the format an old PLAIN store understands.
     * <p>
     * Its value is the concatenation of the {@code [headersSize(varint)][headersBytes]} prefixes that
     * were stripped off the list elements, in list order. Each chunk carries its own length, so the
     * blob is self-delimiting and no element count is needed. An element with no headers contributes
     * a single {@code 0x00} byte.
     * <p>
     * Kept short like the {@code "v"} and {@code "c"} keys Kafka already writes onto changelog records:
     * this one is paid on every record, and the changelog record carries no user headers to collide with
     * (see {@code ChangeLoggingListValueBytesStoreWithHeaders#changelogHeaders}).
     * <p>
     * A record <em>without</em> this header is a legacy PLAIN record: see
     * {@link #joinPlainListBlobWithElementHeaders(byte[], byte[])}.
     */
    static final String LIST_VALUE_HEADERS_HEADER_KEY = "vh";

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
     * A HEADERS list blob taken apart for the changelog: the value bytes an old PLAIN store can still
     * read, plus the per-element headers prefixes to park in {@link #LIST_VALUE_HEADERS_HEADER_KEY}.
     */
    static final class SplitListBlob {
        final byte[] plainListBlob;
        final byte[] elementHeaders;

        SplitListBlob(final byte[] plainListBlob, final byte[] elementHeaders) {
            this.plainListBlob = plainListBlob;
            this.elementHeaders = elementHeaders;
        }
    }

    /**
     * Splits a HEADERS list blob into the PLAIN list blob plus the concatenated per-element headers
     * prefixes. Inverse of {@link #joinPlainListBlobWithElementHeaders(byte[], byte[])}.
     * <p>
     * This is the list-aware counterpart of {@link Utils#rawPlainValue(byte[])}: it keeps the changelog
     * value in the pre-headers format so that an old PLAIN store — or a store whose
     * {@code dsl.store.format} was flipped back to PLAIN — can still decode it.
     *
     * @param headersListBlob a {@code ListSerde} blob of {@code [headersSize][headers][flag][value]}
     *                        elements, or {@code null} for a whole-list tombstone
     */
    static SplitListBlob splitHeadersListBlob(final byte[] headersListBlob) {
        if (headersListBlob == null) {
            return new SplitListBlob(null, null);
        }
        final List<byte[]> headersElements = LIST_SERDE.deserializer().deserialize(null, headersListBlob);
        final List<byte[]> plainElements = new ArrayList<>(headersElements.size());
        final ByteArrayOutputStream elementHeaders = new ByteArrayOutputStream();
        boolean anyHeaders = false;

        for (final byte[] element : headersElements) {
            if (element == null) {
                // ListValueStore is the only writer and never appends null: put/putIfAbsent turn a null
                // value into a whole-list delete, and putAll is unsupported.
                throw new SerializationException("Unexpected null element in list-value blob of "
                    + headersElements.size() + " elements");
            }
            final int prefixLength = headersPrefixLength(element);
            // An empty headers section is exactly the one-byte varint 0, so anything longer carries headers.
            anyHeaders |= prefixLength > 1;
            elementHeaders.write(element, 0, prefixLength);
            final byte[] plainElement = new byte[element.length - prefixLength];
            System.arraycopy(element, prefixLength, plainElement, 0, plainElement.length);
            plainElements.add(plainElement);
        }

        return new SplitListBlob(
            LIST_SERDE.serializer().serialize(null, plainElements),
            // All-empty prefixes carry no information: the join side reads an absent blob as "every
            // element has empty headers", so dropping it makes the changelog record byte-identical to
            // what a PLAIN store would have logged.
            anyHeaders ? elementHeaders.toByteArray() : null
        );
    }

    /**
     * Rebuilds a HEADERS list blob by re-inlining each element's headers prefix. Inverse of
     * {@link #splitHeadersListBlob(byte[])}, and the restore-time counterpart of the split.
     *
     * @param plainListBlob  a {@code ListSerde} blob of {@code [flag][value]} elements, or {@code null}
     * @param elementHeaders the concatenated prefixes written by the split, or {@code null}/empty for a
     *                       legacy record that predates the headers format — in which case every element
     *                       gets empty headers, i.e. exactly
     *                       {@link #convertPlainListBlobToHeadersListBlob(byte[])}
     */
    static byte[] joinPlainListBlobWithElementHeaders(final byte[] plainListBlob, final byte[] elementHeaders) {
        if (plainListBlob == null) {
            return null;
        }
        // Every element contributes at least the one-byte headersSize varint, so an absent or empty
        // prefix blob can only mean "legacy record" or "empty list" — both are the all-empty case.
        if (elementHeaders == null || elementHeaders.length == 0) {
            return convertPlainListBlobToHeadersListBlob(plainListBlob);
        }

        final List<byte[]> plainElements = LIST_SERDE.deserializer().deserialize(null, plainListBlob);
        final List<byte[]> headersElements = new ArrayList<>(plainElements.size());
        final ByteBuffer prefixes = ByteBuffer.wrap(elementHeaders);

        for (final byte[] plainElement : plainElements) {
            final byte[] prefix = readNextHeadersPrefix(prefixes);
            if (plainElement == null) {
                // As in splitHeadersListBlob: ListValueStore never appends null, so a null element here
                // means a corrupt or foreign changelog record.
                throw new SerializationException("Unexpected null element in list-value changelog record of "
                    + plainElements.size() + " elements");
            }
            final byte[] headersElement = new byte[prefix.length + plainElement.length];
            System.arraycopy(prefix, 0, headersElement, 0, prefix.length);
            System.arraycopy(plainElement, 0, headersElement, prefix.length, plainElement.length);
            headersElements.add(headersElement);
        }

        if (prefixes.hasRemaining()) {
            throw new SerializationException("Invalid list-value headers: " + prefixes.remaining()
                + " trailing bytes after " + plainElements.size() + " list elements");
        }
        return LIST_SERDE.serializer().serialize(null, headersElements);
    }

    /**
     * @return the per-element headers prefixes carried by a changelog record, or {@code null} if the
     *         record has none — i.e. it is a legacy record written before the headers format
     */
    static byte[] elementHeaders(final Headers headers) {
        final Header header = headers.lastHeader(LIST_VALUE_HEADERS_HEADER_KEY);
        return header == null ? null : header.value();
    }

    /**
     * @return the length of the {@code [headersSize(varint)][headersBytes]} prefix of a HEADERS element
     */
    private static int headersPrefixLength(final byte[] headersElement) {
        final ByteBuffer buffer = ByteBuffer.wrap(headersElement);
        final int headersSize = ByteUtils.readVarint(buffer);
        if (headersSize < 0 || headersSize > buffer.remaining()) {
            throw new SerializationException("Invalid headers size " + headersSize + " in list element of "
                + headersElement.length + " bytes");
        }
        return buffer.position() + headersSize;
    }

    /**
     * Reads one self-delimiting {@code [headersSize(varint)][headersBytes]} chunk, advancing the buffer.
     */
    private static byte[] readNextHeadersPrefix(final ByteBuffer prefixes) {
        if (!prefixes.hasRemaining()) {
            throw new SerializationException(
                "Invalid list-value headers: fewer headers prefixes than list elements");
        }
        final int start = prefixes.position();
        final int headersSize = ByteUtils.readVarint(prefixes);
        if (headersSize < 0 || headersSize > prefixes.remaining()) {
            throw new SerializationException("Invalid list-value headers: headers size " + headersSize
                + " but only " + prefixes.remaining() + " bytes remaining");
        }
        final int end = prefixes.position() + headersSize;
        prefixes.position(start);
        return Utils.readBytes(prefixes, end - start);
    }
}
