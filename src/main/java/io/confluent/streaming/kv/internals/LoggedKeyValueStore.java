package io.confluent.streaming.kv.internals;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.RecordCollector;
import io.confluent.streaming.kv.Entry;
import io.confluent.streaming.kv.KeyValueIterator;
import io.confluent.streaming.kv.KeyValueStore;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Created by guozhang on 7/30/15.
 */
public class LoggedKeyValueStore<K, V> implements KeyValueStore<K, V> {

    protected final KeyValueStore<K,V> inner;

    private final String topic;
    private final int partition;
    private final KStreamContext context;

    private final Set<K> dirty;
    private final int maxDirty;


    public LoggedKeyValueStore(String topic, KeyValueStore<K,V> inner, KStreamContext context) {
        this.inner = inner;
        this.context = context;

        this.topic = topic;
        this.partition = context.id();

        this.dirty = new HashSet<K>();
        this.maxDirty = 100;        // TODO: this needs to be configurable
    }

    @Override
    public String name() {
        return inner.name();
    }

    @Override
    public boolean persistent() {
        return inner.persistent();
    }

    @Override
    public void restore() {
        final Deserializer<K> keyDeserializer = (Deserializer<K>) context.keySerializer();
        final Deserializer<V> valDeserializer = (Deserializer<V>) context.valueSerializer();

        context.restore(this, new RestoreFunc () {
            @Override
            public void apply(byte[] key, byte[] value) {
                inner.put(keyDeserializer.deserialize(topic, key),
                    valDeserializer.deserialize(topic, value));
            }

            @Override
            public void load() {
                inner.restore();
            }
        });
    }

    @Override
    public V get(K key) {
        return inner.get(key);
    }

    @Override
    public void put(K key, V value) {
        inner.put(key, value);

        this.dirty.add(key);
        if (this.dirty.size() > this.maxDirty)
            log();
    }

    @Override
    public void putAll(List<Entry<K, V>> entries) {
        inner.putAll(entries);

        for (Entry<K, V> entry : entries) {
            this.dirty.add(entry.key());
        }

        if (this.dirty.size() > this.maxDirty)
            log();
    }

    @Override
    public void delete(K key) {
        inner.delete(key);

        this.dirty.add(key);
        if (this.dirty.size() > this.maxDirty)
            log();

    }

    @Override
    public KeyValueIterator<K, V> range(K from, K to) {
        return inner.range(from, to);
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return inner.all();
    }

    @Override
    public void close() {}

    @SuppressWarnings("unchecked")
    @Override
    public void flush() {
        // TODO: these two operations should be done atomically
        log();
        inner.flush();
    }

    private void log() {
        RecordCollector collector = context.recordCollector();
        Serializer<K> keySerializer = (Serializer<K>) context.keySerializer();
        Serializer<V> valueSerializer = (Serializer<V>) context.valueSerializer();

        if(collector != null) {
            for (K k : this.dirty) {
                V v = this.inner.get(k);
                collector.send(new ProducerRecord<>(this.topic, this.partition, k, v), keySerializer, valueSerializer);
            }
            this.dirty.clear();
        }
    }
}
