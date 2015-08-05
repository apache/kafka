package org.apache.kafka.stream.kv;

/**
 * Created by yasuhiro on 6/26/15.
 */
public class Entry<K, V> {

    private final K key;
    private final V value;

    public Entry(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K key() {
        return key;
    }

    public V value() {
        return value;
    }

    public String toString() {
        return "Entry(" + key() + ", " + value() + ")";
    }

}
