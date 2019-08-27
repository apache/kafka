package org.apache.kafka.streams.internals;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.KeyValueBytesStoreSupplier;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

public class MockStoreFactory {

    public final Map<String, StoreBuilder> stateStores = new LinkedHashMap<>();

    public MockStoreFactory () {
    }

    public <K, V> KeyValueStoreBuilder createKeyValueStoreBuilder(KeyValueBytesStoreSupplier keyValueBytesStoreSupplier,
                                                           final Serde<K> keySerde,
                                                           final Serde<V> valueSerde,
                                                           boolean persistent){
        String storeName = keyValueBytesStoreSupplier.name();
        stateStores.put(storeName, new MockKeyValueStoreBuilder<>(keyValueBytesStoreSupplier, keySerde, valueSerde, persistent));
        return (KeyValueStoreBuilder)stateStores.get(storeName);
    }

    public StoreBuilder getStore(String storeName) {
        return stateStores.get(storeName);
    }
}
