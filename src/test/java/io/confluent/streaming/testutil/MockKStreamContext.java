package io.confluent.streaming.testutil;


import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.RecordCollector;
<<<<<<< HEAD
import io.confluent.streaming.StorageEngine;
=======
import io.confluent.streaming.StateStore;
import io.confluent.streaming.Coordinator;
>>>>>>> wip
import io.confluent.streaming.internal.StreamGroup;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.File;
import java.util.Map;

/**
 * Created by guozhang on 7/11/15.
 */
public class MockKStreamContext implements KStreamContext {

  Serializer serializer;
  Deserializer deserializer;

  public MockKStreamContext(Serializer<?> serializer, Deserializer<?> deserializer) {
    this.serializer = serializer;
    this.deserializer = deserializer;
  }

  @Override
  public int id() { return -1; }

  @Override
  public Serializer<?> keySerializer() { return serializer; }

  @Override
  public Serializer<?> valueSerializer() { return serializer; }

  @Override
  public Deserializer<?> keyDeserializer() { return deserializer; }

  @Override
  public Deserializer<?> valueDeserializer() { return deserializer; }

  @Override
  public KStream<?, ?> from(String... topic) { throw new UnsupportedOperationException("from() not supported."); }

  @Override
  public <K, V> KStream<K, V> from(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topic) { throw new UnsupportedOperationException("from() not supported."); }

  @Override
  public RecordCollector recordCollector() { throw new UnsupportedOperationException("recordCollector() not supported."); }

  @Override
  public Map<String, Object> getContext() { throw new UnsupportedOperationException("getContext() not supported."); }

  @Override
  public File stateDir() { throw new UnsupportedOperationException("stateDir() not supported."); }

  @Override
  public Metrics metrics() { throw new UnsupportedOperationException("metrics() not supported."); }

  @Override
  public StreamGroup streamGroup(String name) { throw new UnsupportedOperationException("streamGroup() not supported."); }

  @Override
  public StreamGroup roundRobinStreamGroup(String name) { throw new UnsupportedOperationException("roundRobinStreamGroup() not supported."); }

  @Override
  public void restore(StateStore engine) throws Exception { throw new UnsupportedOperationException("restore() not supported."); }

  @Override
  public void ensureInitialization() {}

  @Override
  public void flush() { throw new UnsupportedOperationException("flush() not supported."); }
}
