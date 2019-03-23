package org.apache.kafka.streams.kstream;

import org.apache.kafka.streams.state.StoreBuilder;

import java.util.Collection;
import java.util.Collections;

public interface StateStoresSupplier {
    default Collection<StoreBuilder> stateStores() {
        return Collections.emptyList();
    }
}
