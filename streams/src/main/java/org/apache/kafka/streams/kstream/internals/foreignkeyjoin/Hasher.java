package org.apache.kafka.streams.kstream.internals.foreignkeyjoin;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Murmur3;

public class Hasher<V> {
    private final Serializer<V> serializer;
    private final String topic;

    public Hasher(String topic, Serializer<V> serializer) {
        this.topic = topic;
        this.serializer = serializer;
    }

    public long[] generateHash(V value) {
        final long[] currentHash = value == null ?
                //Murmur3.hash128(new byte[]{}) :
                Murmur3.hash128(null) :
                Murmur3.hash128(serializer.serialize(topic, value));

        return currentHash;
    }
}
