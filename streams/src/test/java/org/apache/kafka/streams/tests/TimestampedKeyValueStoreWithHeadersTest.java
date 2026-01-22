package org.apache.kafka.streams.tests;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.streams.state.TimestampedKeyValueStoreWithHeaders;
import org.apache.kafka.streams.state.ValueTimestampHeaders;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class TimestampedKeyValueStoreWithHeadersTest {

  @Test
  public void shouldPutAndGetUsingRecordContext() {
    // 1. Define the Store Builder
    StoreBuilder<TimestampedKeyValueStoreWithHeaders<String, String>> storeBuilder =
        Stores.timestampedKeyValueStoreBuilderWithHeaders(
            Stores.persistentTimestampedKeyValueStoreWithHeaders("test-store"),
            Serdes.String(),
            Serdes.String()
        );

    // 2. Setup the topology with a dummy connection
    Topology topology = new Topology();
    topology.addSource("source", "input-topic")
        .addProcessor("processor", () -> new Processor<Object, Object, Object, Object>() {
          @Override
          public void process(Record<Object, Object> record) {}
        }, "source")
        .addStateStore(storeBuilder, "processor");

    // 3. Setup Driver Properties (Including the missing Serdes)
    Properties props = new Properties();
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
    props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
    // These two lines fix the ConfigException:
    props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

    try (TopologyTestDriver driver = new TopologyTestDriver(topology, props)) {

      // 4. Retrieve the store
      KeyValueStore<String, ValueTimestampHeaders<String>> store =
          driver.getTimestampedKeyValueStoreWithHeaders("test-store");

      assertNotNull(store, "Store 'test-store' should not be null");

      // 5. Create the incoming Record
      long now = System.currentTimeMillis();
      final Record<String, String> record = new Record<>(
          "user-123",
          "active-status",
          now
      );
      record.headers().add("source", "unit-test".getBytes());

      // 6. Store and Verify
      store.put(
          record.key(),
          ValueTimestampHeaders.make(record.value(), record.timestamp(), record.headers())
      );

      final ValueTimestampHeaders<String> result = store.get(record.key());

      assertNotNull(result);
      assertEquals(record.value(), result.value());
      assertEquals(record.timestamp(), result.timestamp());
      assertEquals(record.headers(), result.headers());
    }
  }
}