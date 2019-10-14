package org.apache.kafka.streams.state;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

public class MockStoreFactory {

    public final Map<String, StoreBuilder> stateStores = new LinkedHashMap<>();

    public MockStoreFactory () {
    }

    public <K, V> KeyValueStoreBuilder createKeyValueStoreBuilder(KeyValueBytesStoreSupplier keyValueBytesStoreSupplier,
                                                                  final Serde<K> keySerde,
                                                                  final Serde<V> valueSerde,
                                                                  boolean persistent,
                                                                  final Time time){
        String storeName = keyValueBytesStoreSupplier.name();
        stateStores.put(storeName, new MockKeyValueStoreBuilder<>(keyValueBytesStoreSupplier, keySerde, valueSerde, persistent, time));
        return (KeyValueStoreBuilder)stateStores.get(storeName);
    }

    public StoreBuilder getStore(String storeName) {
        return stateStores.get(storeName);
    }
}
