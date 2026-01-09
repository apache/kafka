package org.apache.kafka.streams.state;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Windowed;

import java.time.Instant;

/**
 * Interface for storing the aggregated values of fixed-size time windows.
 * <p>
 * Note, that the stores's physical key type is {@link Windowed Windowed&lt;K&gt;}.
 * In contrast to a {@link WindowStore} that stores plain windowedKeys-value pairs,
 * a {@code TimestampedWindowStore} stores windowedKeys-(value/timestamp) pairs.
 * <p>
 * While the window start- and end-timestamp are fixed per window, the value-side timestamp is used
 * to store the last update timestamp of the corresponding window.
 *
 * @param <K> Type of keys
 * @param <V> Type of values
 */
public interface TimestampedWindowStoreWithHeaders<K, V> extends WindowStore<K, ValueAndTimestampWithHeaders<V>> {

    /**
     * Put a key-value-timestamp pair, along with its associated Kafka headers, into the window
     * with the given window start timestamp.
     * <p>
     * This method is the header-aware extension of the base TimestampedWindowStore#put.
     *
     * @param key                  The key to associate the value to
     * @param value                The value; can be null
     * @param windowStartTimestamp The timestamp of the beginning of the window
     * @param timestamp            The record timestamp
     * @param headers              The Kafka headers associated with the record (contains Schema ID)
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException if the given key or headers are {@code null}
     */
    void put(K key, V value, long windowStartTimestamp, long timestamp, Headers headers);

    // --- 2. READ OPERATIONS RETURNING HEADERS (Per-key, long-based) ---

    /**
     * Get all the key-value-timestamp pairs with the given key and time range,
     * including the associated headers.
     * <p>
     * This iterator must be closed after use.
     *
     * @param key      the key to fetch
     * @param timeFrom time range start (inclusive)
     * @param timeTo   time range end (inclusive)
     * @return an iterator over key-value-timestamp-header pairs {@code <timestamp, ValueTimestampHeaders<V>>}
     * @throws InvalidStateStoreException if the store is not initialized
     * @throws NullPointerException       if the given key is {@code null}
     */
    WindowStoreIterator<ValueAndTimestampWithHeaders<V>> fetchWithHeaders(K key, long timeFrom, long timeTo);

    default WindowStoreIterator<ValueAndTimestampWithHeaders<V>> backwardFetchWithHeaders(final K key,
                                                                                          final long timeFrom,
                                                                                          final long timeTo) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of TimestampedWindowStoreWithHeaders.");
    }

    // --- 3. READ OPERATIONS RETURNING HEADERS (Key-range, long-based) ---

    /**
     * Get all the key-value-timestamp pairs in the given key range and time range,
     * including the associated headers.
     * <p>
     * This iterator must be closed after use.
     *
     * @param keyFrom  the first key in the range (inclusive)
     * @param keyTo    the last key in the range (inclusive)
     * @param timeFrom time range start (inclusive)
     * @param timeTo   time range end (inclusive)
     * @return an iterator over windowed key-value-timestamp-header pairs {@code <Windowed<K>, ValueTimestampHeaders<V>>}
     * @throws InvalidStateStoreException if the store is not initialized
     */
    KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> fetchWithHeaders(K keyFrom, K keyTo, long timeFrom, long timeTo);

    default KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> backwardFetchWithHeaders(final K keyFrom,
                                                                                                    final K keyTo,
                                                                                                    final long timeFrom,
                                                                                                    final long timeTo) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of TimestampedWindowStoreWithHeaders.");
    }

    // --- 4. READ OPERATIONS RETURNING HEADERS (All keys, long-based) ---

    /**
     * Gets all the key-value-timestamp pairs that belong to the windows within the given time range,
     * including the associated headers.
     * <p>
     * This iterator must be closed after use.
     *
     * @param timeFrom the beginning of the time slot from which to search (inclusive)
     * @param timeTo   the end of the time slot from which to search (inclusive)
     * @return an iterator over windowed key-value-timestamp-header pairs {@code <Windowed<K>, ValueTimestampHeaders<V>>}
     * @throws InvalidStateStoreException if the store is not initialized
     */
    KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> fetchAllWithHeaders(long timeFrom, long timeTo);

    default KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> backwardFetchAllWithHeaders(final long timeFrom,
                                                                                                       final long timeTo) {
        throw new UnsupportedOperationException("This API is not supported by this implementation of TimestampedWindowStoreWithHeaders.");
    }

    // --- 5. READ OPERATIONS RETURNING HEADERS (Instant-based overloads) ---

    // The default Instant methods rely on the long-based methods above, simplifying the implementation.

    default WindowStoreIterator<ValueAndTimestampWithHeaders<V>> fetchWithHeaders(final K key,
                                                                                  final Instant timeFrom,
                                                                                  final Instant timeTo) throws IllegalArgumentException {
        return fetchWithHeaders(key, timeFrom.toEpochMilli(), timeTo.toEpochMilli());
    }

    default WindowStoreIterator<ValueAndTimestampWithHeaders<V>> backwardFetchWithHeaders(final K key,
                                                                                          final Instant timeFrom,
                                                                                          final Instant timeTo) throws IllegalArgumentException {
        return backwardFetchWithHeaders(key, timeFrom.toEpochMilli(), timeTo.toEpochMilli());
    }

    default KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> fetchWithHeaders(final K keyFrom,
                                                                                            final K keyTo,
                                                                                            final Instant timeFrom,
                                                                                            final Instant timeTo) throws IllegalArgumentException {
        return fetchWithHeaders(keyFrom, keyTo, timeFrom.toEpochMilli(), timeTo.toEpochMilli());
    }

    default KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> backwardFetchWithHeaders(final K keyFrom,
                                                                                                    final K keyTo,
                                                                                                    final Instant timeFrom,
                                                                                                    final Instant timeTo) throws IllegalArgumentException {
        return backwardFetchWithHeaders(keyFrom, keyTo, timeFrom.toEpochMilli(), timeTo.toEpochMilli());
    }

    default KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> fetchAllWithHeaders(final Instant timeFrom,
                                                                                               final Instant timeTo) throws IllegalArgumentException {
        return fetchAllWithHeaders(timeFrom.toEpochMilli(), timeTo.toEpochMilli());
    }

    default KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> backwardFetchAllWithHeaders(final Instant timeFrom,
                                                                                                       final Instant timeTo) throws IllegalArgumentException {
        return backwardFetchAllWithHeaders(timeFrom.toEpochMilli(), timeTo.toEpochMilli());
    }
}
