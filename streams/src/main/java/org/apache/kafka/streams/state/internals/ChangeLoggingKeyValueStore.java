package org.apache.kafka.streams.state.internals;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateSerdes;

import java.util.ArrayList;
import java.util.List;

/**
 * Changelog-enabled {@link KeyValueStore} wrapper is used for recording operation metrics, and hence its
 * inner KeyValueStore implementation do not need to provide its own write-ahead-logging functionality.
 *
 * @param <K>
 * @param <V>
 */
class ChangeLoggingKeyValueStore<K, V> extends WrapperKeyValueStore.AbstractKeyValueStore<K, V> implements WrapperKeyValueStore<K, V> {

    private final String storeName;
    private final Serde<K> keySerde;
    private final Serde<V> valueSerde;
    protected final KeyValueStore<Bytes, byte[]> innerBytes;

    private StateSerdes<K, V> serdes;
    protected StoreChangeLogger<Bytes, byte[]> changeLogger;

    // this is optimizing the case when the underlying store is already bytes store, in which we can avoid Bytes.wrap() costs
    private static class ChangeLoggingBytesKeyValueStore extends ChangeLoggingKeyValueStore<Bytes, byte[]> {
        ChangeLoggingBytesKeyValueStore(final KeyValueStore<Bytes, byte[]> inner, final String storeName) {
            super(inner, storeName, Serdes.Bytes(), Serdes.ByteArray());
        }

        @Override
        public byte[] get(Bytes key) {
            return this.innerBytes.get(key);
        }

        @Override
        public void put(Bytes key, byte[] value) {
            this.innerBytes.put(key, value);
            this.changeLogger.logChange(key, value);
        }

        @Override
        public void putAll(List<KeyValue<Bytes, byte[]>> entries) {
            this.innerBytes.putAll(entries);

            for (KeyValue<Bytes, byte[]> entry : entries) {
                this.changeLogger.logChange(entry.key, entry.value);
            }
        }

        @Override
        public byte[] delete(Bytes key) {
            return this.innerBytes.delete(key);
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> range(Bytes from, Bytes to) {
            return SerializedKeyValueStoreIterator.bytesIterator(this.innerBytes.range(from, to));
        }

        @Override
        public KeyValueIterator<Bytes, byte[]> all() {
            return SerializedKeyValueStoreIterator.bytesIterator(this.innerBytes.all());
        }
    }

    static ChangeLoggingKeyValueStore<Bytes, byte[]> bytesStore(final KeyValueStore<Bytes, byte[]> inner, final String storeName) {
        return new ChangeLoggingBytesKeyValueStore(inner, storeName);
    }

    ChangeLoggingKeyValueStore(final KeyValueStore<Bytes, byte[]> inner, final String storeName, final Serde<K> keySerde, final Serde<V> valueSerde) {
        super(inner);
        this.innerBytes = inner;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.storeName = storeName;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void init(ProcessorContext context, StateStore root) {
        innerBytes.init(context, root);

        this.serdes = new StateSerdes<>(innerBytes.name(),
                keySerde == null ? (Serde<K>) context.keySerde() : keySerde,
                valueSerde == null ? (Serde<V>) context.valueSerde() : valueSerde);

        this.changeLogger = new StoreChangeLogger<>(storeName, context, WindowStoreUtils.INNER_SERDES);
    }

    @Override
    public V get(K key) {
        Bytes rawKey = Bytes.wrap(serdes.rawKey(key));

        return serdes.valueFrom(innerBytes.get(rawKey));
    }

    @Override
    public void put(K key, V value) {
        final Bytes rawKey = Bytes.wrap(serdes.rawKey(key));
        final byte[] rawValue = serdes.rawValue(value);

        innerBytes.put(rawKey, rawValue);
        changeLogger.logChange(rawKey, rawValue);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        final V v = get(key);
        if (v == null) {
            put(key, value);
        }
        return v;
    }

    @Override
    public void putAll(List<KeyValue<K, V>> entries) {
        List<KeyValue<Bytes, byte[]>> entriesBytes = new ArrayList<>();

        for (KeyValue<K, V> entry : entries) {
            final Bytes rawKey = Bytes.wrap(serdes.rawKey(entry.key));
            final byte[] rawValue = entry.value == null ? null : serdes.rawValue(entry.value);

            entriesBytes.add(KeyValue.pair(rawKey, rawValue));
        }

        this.innerBytes.putAll(entriesBytes);

        for (KeyValue<Bytes, byte[]> entry : entriesBytes) {
            changeLogger.logChange(entry.key, entry.value);
        }
    }

    @Override
    public V delete(K key) {
        Bytes rawKey = Bytes.wrap(serdes.rawKey(key));

        return serdes.valueFrom(innerBytes.delete(rawKey));
    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        Bytes fromBytes = Bytes.wrap(serdes.rawKey(from));
        Bytes toBytes = Bytes.wrap(serdes.rawKey(to));

        return new SerializedKeyValueStoreIterator<>(this.innerBytes.range(fromBytes, toBytes), serdes);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return new SerializedKeyValueStoreIterator<>(this.innerBytes.all(), serdes);
    }
}
