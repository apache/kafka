package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;


class RocksDBTimestampedWindowStoreWithHeaders
    extends WrappedStateStore<SegmentedBytesStore, Object, Object>
    implements TimestampedWindowStoreWithHeaders<Bytes, byte[]> {

    private final boolean retainDuplicates;
    private final long windowSize;
    private int seqnum = 0;

    RocksDBTimestampedWindowStoreWithHeaders(final SegmentedBytesStore bytesStore,
                                             final boolean retainDuplicates,
                                             final long windowSize) {
        super(bytesStore);
        this.retainDuplicates = retainDuplicates;
        this.windowSize = windowSize;
    }

    @Override
    public void init(final StateStoreContext context, final StateStore root) {
        wrapped().init(context, root);
    }

    private void maybeUpdateSeqnumForDups() {
        if (retainDuplicates) {
            seqnum = (seqnum + 1) & 0x7FFFFFFF;
        }
    }

    @Override
    public void put(final Bytes key,
                    final byte[] value,
                    final long windowStartTimestamp,
                    final long timestamp,
                    final Headers headers) {
        final byte[] encodedValue = encodeValueWithTimestampAndHeaders(value, timestamp, headers);

        // Skip if value is null and duplicates are allowed since this delete is a no-op
        if (!(encodedValue == null && retainDuplicates)) {
            maybeUpdateSeqnumForDups();
            wrapped().put(WindowKeySchema.toStoreKeyBinary(key, windowStartTimestamp, seqnum), encodedValue);
        }
    }

    @Override
    public void put(final Bytes key,
                    final ValueTimestampHeaders<byte[]> valueAndHeaders,
                    final long windowStartTimestamp) {
        if (valueAndHeaders == null) {
            put(key, null, windowStartTimestamp, 0L, null);
        } else {
            put(key,
                valueAndHeaders.value(),
                windowStartTimestamp,
                valueAndHeaders.timestamp(),
                valueAndHeaders.headers());
        }
    }

    @Override
    public ValueTimestampHeaders<byte[]> fetch(final Bytes key, final long timestamp) {
        final byte[] encodedValue = wrapped().get(WindowKeySchema.toStoreKeyBinary(key, timestamp, seqnum));
        return decodeValueWithTimestampAndHeaders(encodedValue);
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<byte[]>> fetch(final Bytes key,
                                                                    final long timeFrom,
                                                                    final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(key, timeFrom, timeTo);
        return new WindowStoreHeaderIteratorWrapper(
            new WindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator()
        );
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<byte[]>> backwardFetch(final Bytes key,
                                                                            final long timeFrom,
                                                                            final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(key, timeFrom, timeTo);
        return new WindowStoreHeaderIteratorWrapper(
            new WindowStoreIteratorWrapper(bytesIterator, windowSize).valuesIterator()
        );
    }

    @Override
    public WindowStoreIterator<ValueTimestampHeaders<byte[]>> fetchWithHeaders(final Bytes key,
                                                                               final long timeFrom,
                                                                               final long timeTo) {
        return fetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> fetch(final Bytes keyFrom,
                                                                                  final Bytes keyTo,
                                                                                  final long timeFrom,
                                                                                  final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetch(keyFrom, keyTo, timeFrom, timeTo);
        return new KeyValueIteratorHeaderWrapper(
            new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator()
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> backwardFetch(final Bytes keyFrom,
                                                                                          final Bytes keyTo,
                                                                                          final long timeFrom,
                                                                                          final long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
        return new KeyValueIteratorHeaderWrapper(
            new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator()
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> fetchWithHeaders(Bytes keyFrom,
                                                                                             Bytes keyTo,
                                                                                             long timeFrom,
                                                                                             long timeTo) {
        return fetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> all() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().all();
        return new KeyValueIteratorHeaderWrapper(
            new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator()
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> backwardAll() {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardAll();
        return new KeyValueIteratorHeaderWrapper(
            new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator()
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> fetchAll(long timeFrom, long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().fetchAll(timeFrom, timeTo);
        return new KeyValueIteratorHeaderWrapper(
            new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator()
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> backwardFetchAll(long timeFrom, long timeTo) {
        final KeyValueIterator<Bytes, byte[]> bytesIterator = wrapped().backwardFetchAll(timeFrom, timeTo);
        return new KeyValueIteratorHeaderWrapper(
            new WindowStoreIteratorWrapper(bytesIterator, windowSize).keyValueIterator()
        );
    }

    @Override
    public KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> fetchAllWithHeaders(long timeFrom, long timeTo) {
        return fetchAll(timeFrom, timeTo);
    }

    /**
     * Encodes value, timestamp, and headers into a single byte array.
     * Format: [HeaderSize(2)][Headers(variable)][Timestamp(8)][Payload(variable)]
     *
     * Per KIP specification:
     * - HeaderSize is 2 bytes unsigned (max 65535 bytes)
     * - Headers are serialized using the serializeHeaders() format
     * - Timestamp is 8 bytes
     * - Payload is the value bytes
     *
     * @param value the value bytes (can be null)
     * @param timestamp the timestamp
     * @param headers the headers (can be null)
     * @return encoded byte array
     * @throws org.apache.kafka.streams.errors.StreamsException if headers exceed maximum size
     */
    private byte[] encodeValueWithTimestampAndHeaders(final byte[] value,
                                                       final long timestamp,
                                                       final Headers headers) {
        if (value == null) {
            return null;
        }

        // Serialize headers first to know their size
        final byte[] serializedHeaders = serializeHeaders(headers);
        final int headerSize = serializedHeaders.length;

        // Validate header size against 2-byte unsigned max (65535)
        // KIP specifies streams.store.headers.max.bytes default of 65536, but 2-byte unsigned max is 65535
        if (headerSize > 65535) {
            throw new IllegalStateException(
                "Serialized headers size " + headerSize +
                " bytes exceeds maximum of 65535 bytes (2-byte unsigned limit). " +
                "Consider reducing header count or size."
            );
        }

        // Calculate total size: headerSize(2) + headers + timestamp(8) + value
        final int totalSize = 2 + headerSize + 8 + value.length;

        // Allocate buffer and write data
        return ByteBuffer.allocate(totalSize)
            .putShort((short) headerSize)  // Header size (2 bytes, unsigned)
            .put(serializedHeaders)        // Serialized headers
            .putLong(timestamp)            // Timestamp (8 bytes)
            .put(value)                    // Value payload
            .array();
    }

    /**
     * Decodes byte array back into ValueTimestampHeaders.
     * Reverses the encoding format.
     *
     * Format: [HeaderSize(2)][Headers(variable)][Timestamp(8)][Payload(variable)]
     *
     * @param encodedValue the encoded byte array
     * @return ValueTimestampHeaders instance
     */
    private ValueTimestampHeaders<byte[]> decodeValueWithTimestampAndHeaders(final byte[] encodedValue) {
        if (encodedValue == null) {
            return null;
        }

        final ByteBuffer buffer = ByteBuffer.wrap(encodedValue);

        // Read header size (2 bytes, unsigned)
        // Use & 0xFFFF to convert signed short to unsigned int
        final int headerSize = buffer.getShort() & 0xFFFF;

        // Read headers
        final byte[] headerBytes = new byte[headerSize];
        buffer.get(headerBytes);
        final Headers headers = deserializeHeaders(headerBytes);

        // Read timestamp
        final long timestamp = buffer.getLong();

        // Read value
        final int valueSize = buffer.remaining();
        final byte[] value = new byte[valueSize];
        buffer.get(value);

        return ValueTimestampHeaders.make(value, timestamp, headers);
    }

    /**
     * Serializes headers into byte array.
     * Format: [NumHeaders(4)][Header1][Header2]...
     * Each header: [KeyLength(4)][KeyBytes][ValueLength(4)][ValueBytes]
     * ValueLength is -1 for null values.
     */
    private byte[] serializeHeaders(final Headers headers) {
        if (headers == null) {
            return ByteBuffer.allocate(4).putInt(0).array();
        }

        // First pass: calculate size
        int totalSize = 4; // For number of headers
        int headerCount = 0;

        for (final Header header : headers) {
            headerCount++;
            final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = header.value();

            totalSize += 4; // Key length
            totalSize += keyBytes.length;
            totalSize += 4; // Value length
            if (valueBytes != null) {
                totalSize += valueBytes.length;
            }
        }

        // Second pass: write data
        final ByteBuffer buffer = ByteBuffer.allocate(totalSize);
        buffer.putInt(headerCount);

        for (final Header header : headers) {
            final byte[] keyBytes = header.key().getBytes(StandardCharsets.UTF_8);
            final byte[] valueBytes = header.value();

            buffer.putInt(keyBytes.length);
            buffer.put(keyBytes);

            if (valueBytes == null) {
                buffer.putInt(-1);
            } else {
                buffer.putInt(valueBytes.length);
                buffer.put(valueBytes);
            }
        }

        return buffer.array();
    }

    /**
     * Deserializes headers from byte array.
     * Reverses the serialization format.
     */
    private Headers deserializeHeaders(final byte[] headerBytes) {
        if (headerBytes == null || headerBytes.length == 0) {
            return new RecordHeaders();
        }

        final ByteBuffer buffer = ByteBuffer.wrap(headerBytes);
        final int headerCount = buffer.getInt();

        final RecordHeaders headers = new RecordHeaders();

        for (int i = 0; i < headerCount; i++) {
            // Read key
            final int keyLength = buffer.getInt();
            final byte[] keyBytes = new byte[keyLength];
            buffer.get(keyBytes);
            final String key = new String(keyBytes, StandardCharsets.UTF_8);

            // Read value
            final int valueLength = buffer.getInt();
            final byte[] value;
            if (valueLength == -1) {
                value = null;
            } else {
                value = new byte[valueLength];
                buffer.get(value);
            }

            headers.add(new RecordHeader(key, value));
        }

        return headers;
    }

    /**
     * Iterator wrapper for single-key fetch (WindowStoreIterator).
     * Wraps the underlying iterator and decodes values on the fly.
     */
    private class WindowStoreHeaderIteratorWrapper implements WindowStoreIterator<ValueTimestampHeaders<byte[]>> {
        private final WindowStoreIterator<byte[]> innerIterator;

        WindowStoreHeaderIteratorWrapper(final WindowStoreIterator<byte[]> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Long peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Long, ValueTimestampHeaders<byte[]>> next() {
            final KeyValue<Long, byte[]> next = innerIterator.next();
            return KeyValue.pair(
                next.key,
                decodeValueWithTimestampAndHeaders(next.value)
            );
        }
    }

    /**
     * Iterator wrapper for range/all fetch (KeyValueIterator).
     * Wraps the underlying iterator and decodes values on the fly.
     */
    private class KeyValueIteratorHeaderWrapper implements KeyValueIterator<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> {
        private final KeyValueIterator<Windowed<Bytes>, byte[]> innerIterator;

        KeyValueIteratorHeaderWrapper(final KeyValueIterator<Windowed<Bytes>, byte[]> innerIterator) {
            this.innerIterator = innerIterator;
        }

        @Override
        public void close() {
            innerIterator.close();
        }

        @Override
        public Windowed<Bytes> peekNextKey() {
            return innerIterator.peekNextKey();
        }

        @Override
        public boolean hasNext() {
            return innerIterator.hasNext();
        }

        @Override
        public KeyValue<Windowed<Bytes>, ValueTimestampHeaders<byte[]>> next() {
            final KeyValue<Windowed<Bytes>, byte[]> next = innerIterator.next();
            return KeyValue.pair(
                next.key,
                decodeValueWithTimestampAndHeaders(next.value)
            );
        }
    }
}
