// Creating an in-memory key-value store:
// here, we create a `KeyValueStore<String, Long>` named "inmemory-counts".
import org.apache.kafka.streams.processor.StateStoreSupplier;
import org.apache.kafka.streams.state.Stores;

// Note: The `Stores` factory returns a supplier for the state store,
// because that's what you typically need to pass as API parameter.
StateStoreSupplier countStoreSupplier =
  Stores.create("inmemory-counts")
    .withKeys(Serdes.String())
    .withValues(Serdes.Long())
    .inMemory()
    .build();
