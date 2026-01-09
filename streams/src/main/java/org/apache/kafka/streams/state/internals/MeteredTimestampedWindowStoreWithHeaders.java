package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.TimestampedWindowStoreWithHeaders;
import org.apache.kafka.streams.state.ValueAndTimestampWithHeaders;
import org.apache.kafka.streams.state.WindowStore;
import org.apache.kafka.streams.state.WindowStoreIterator;

import java.time.Instant;

public class MeteredTimestampedWindowStoreWithHeaders<K, V>
    extends MeteredWindowStore<K, ValueAndTimestampWithHeaders<V>>
    implements TimestampedWindowStoreWithHeaders<K, V> {

    MeteredTimestampedWindowStoreWithHeaders(final WindowStore<Bytes, byte[]> inner,
                                             final long windowSizeMs,
                                             final String metricScope,
                                             final Time time,
                                             final Serde<K> keySerde,
                                             final Serde<ValueAndTimestampWithHeaders<V>> valueSerde) {
        super(inner, windowSizeMs, metricScope, time, keySerde, valueSerde);
    }

    @Override
    public void put(K key, V value, long windowStartTimestamp, long timestamp, Headers headers) {
        final ValueAndTimestampWithHeaders<V> valueWithHeaders = ValueAndTimestampWithHeaders.make(value, timestamp, headers);
        super.put(key, valueWithHeaders, windowStartTimestamp);
    }

    @Override
    public WindowStoreIterator<ValueAndTimestampWithHeaders<V>> fetchWithHeaders(K key, long timeFrom, long timeTo) {
        return fetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> fetchWithHeaders(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        return fetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> fetchAllWithHeaders(long timeFrom, long timeTo) {
        return fetchAll(timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueAndTimestampWithHeaders<V>> backwardFetchWithHeaders(K key, long timeFrom, long timeTo) {
        return backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> backwardFetchWithHeaders(K keyFrom, K keyTo, long timeFrom, long timeTo) {
        return backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> backwardFetchAllWithHeaders(long timeFrom, long timeTo) {
        return backwardFetchAll(timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueAndTimestampWithHeaders<V>> fetchWithHeaders(K key, Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return fetch(key, timeFrom, timeTo);
    }

    @Override
    public WindowStoreIterator<ValueAndTimestampWithHeaders<V>> backwardFetchWithHeaders(K key, Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return backwardFetch(key, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> fetchWithHeaders(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return fetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> backwardFetchWithHeaders(K keyFrom, K keyTo, Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return backwardFetch(keyFrom, keyTo, timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> fetchAllWithHeaders(Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return fetchAll(timeFrom, timeTo);
    }

    @Override
    public KeyValueIterator<Windowed<K>, ValueAndTimestampWithHeaders<V>> backwardFetchAllWithHeaders(Instant timeFrom, Instant timeTo) throws IllegalArgumentException {
        return backwardFetchAll(timeFrom, timeTo);
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Serde<ValueAndTimestampWithHeaders<V>> prepareValueSerde(final Serde<ValueAndTimestampWithHeaders<V>> valueSerde,
                                                                       final SerdeGetter getter) {
        if (valueSerde == null) {
            // TODO: ValueTimestampHeadersSerde
            return new ValueTimestampHeadersSerde<>((Serde<V>) getter.valueSerde());
        } else {
            return super.prepareValueSerde(valueSerde, getter);
        }
    }
}
