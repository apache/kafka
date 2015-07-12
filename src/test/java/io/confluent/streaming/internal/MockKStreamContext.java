package io.confluent.streaming.internal;


import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.RecordCollector;
import io.confluent.streaming.StorageEngine;
import io.confluent.streaming.SyncGroup;
import io.confluent.streaming.Coordinator;
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
  public KStream<?, ?> from(String topic) { throw new UnsupportedOperationException("from() not supported."); }

  @Override
  public KStream<?, ?> from(String topic, SyncGroup syncGroup) { throw new UnsupportedOperationException("from() not supported."); }

  @Override
  public KStream<?, ?> from(String topic, Deserializer<?> keyDeserializer, Deserializer<?> valDeserializer) { throw new UnsupportedOperationException("from() not supported."); }

  @Override
  public KStream<?, ?> from(String topic, SyncGroup syncGroup, Deserializer<?> keyDeserializer, Deserializer<?> valDeserializer) { throw new UnsupportedOperationException("from() not supported."); }

  @Override
  public RecordCollector<Object, Object> recordCollector() { throw new UnsupportedOperationException("recordCollector() not supported."); }

  @Override
  public Coordinator coordinator() { throw new UnsupportedOperationException("coordinator() not supported."); }

  @Override
  public Map<String, Object> getContext() { throw new UnsupportedOperationException("getContext() not supported."); }

  @Override
  public File stateDir() { throw new UnsupportedOperationException("stateDir() not supported."); }

  @Override
  public Metrics metrics() { throw new UnsupportedOperationException("metrics() not supported."); }

  @Override
  public SyncGroup syncGroup(String name) { throw new UnsupportedOperationException("syncGroup() not supported."); }

  @Override
  public SyncGroup roundRobinSyncGroup(String name) { throw new UnsupportedOperationException("roundRobinSyncGroup() not supported."); }

  @Override
  public void restore(StorageEngine engine) throws Exception { throw new UnsupportedOperationException("restore() not supported."); }
}
