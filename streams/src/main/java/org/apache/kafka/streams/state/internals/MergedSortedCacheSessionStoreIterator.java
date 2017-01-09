package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.internals.SessionKeySerde;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.StateSerdes;

/**
 * Merges two iterators. Assumes each of them is sorted by key
 *
 * @param <K>
 * @param <AGG>
 */
class MergedSortedCacheSessionStoreIterator<K, AGG> extends AbstractMergedSortedCacheStoreIterator<Windowed<K>, Windowed<Bytes>, AGG> {
    private final StateSerdes<K, AGG> rawSerdes;


    MergedSortedCacheSessionStoreIterator(final PeekingKeyValueIterator<Bytes, LRUCacheEntry> cacheIterator,
                                          final KeyValueIterator<Windowed<Bytes>, byte[]> storeIterator,
                                          final StateSerdes<K, AGG> serdes) {
        super(cacheIterator, storeIterator, new StateSerdes<>(serdes.stateName(),
                                                              new SessionKeySerde<>(serdes.keySerde()),
                                                              serdes.valueSerde()));

        rawSerdes = serdes;
    }

    @Override
    public KeyValue<Windowed<K>, AGG> deserializeStorePair(KeyValue<Windowed<Bytes>, byte[]> pair) {
        final K key = rawSerdes.keyFrom(pair.key.key().get());
        return KeyValue.pair(new Windowed<>(key, pair.key.window()), serdes.valueFrom(pair.value));
    }

    @Override
    public Windowed<K> deserializeStoreKey(Windowed<Bytes> key) {
        final K originalKey = rawSerdes.keyFrom(key.key().get());
        return new Windowed<K>(originalKey, key.window());
    }

    @Override
    public int compare(Bytes cacheKey, Windowed<Bytes> storeKey) {
        Bytes storeKeyBytes = SessionKeySerde.bytesToBinary(storeKey);
        return cacheKey.compareTo(storeKeyBytes);
    }
}

