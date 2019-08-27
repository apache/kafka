package org.apache.kafka.streams.internals;

import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.internals.KeyValueStoreBuilder;

public class MockStoreFactory<K, V> {

    public final Map<String, StoreBuilder> stateStores = new LinkedHashMap<>();

    public MockStoreFactory () {

    }

    public KeyValueStoreBuilder createKeyValueStoreBuilder(String storeName, boolean persistent){
        stateStores.put(storeName, new MockKeyValueStoreBuilder(storeName, persistent));
        return (KeyValueStoreBuilder)stateStores.get(storeName);
    }

    public KeyValueStoreBuilder createKeyValueStoreBuilder(String storeName,
                                                           KeyValueStore keyValueStore,
                                                           boolean persistent){
        stateStores.put(storeName, new MockKeyValueStoreBuilder(storeName, keyValueStore, persistent));
        return (KeyValueStoreBuilder)stateStores.get(storeName);
    }

    public StoreBuilder getStore(String storeName) {
        return stateStores.get(storeName);
    }



}
