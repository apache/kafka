// Creating a persistent key-value store:
// here, we create a `KeyValueStore<String, Long>` named "persistent-counts".
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

// Note: The `Stores` factory returns a supplier for the state store,
// because that's what you typically need to pass as API parameter.
StateStoreSupplier countStoreSupplier =
  Stores.create("persistent-counts")
    .withKeys(Serdes.String())
    .withValues(Serdes.Long())
    .persistent()
    .build();
