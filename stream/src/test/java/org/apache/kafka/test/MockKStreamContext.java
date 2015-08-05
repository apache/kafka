package org.apache.kafka.test;


import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.Processor;
<<<<<<< HEAD
<<<<<<< HEAD
=======
import io.confluent.streaming.PunctuationScheduler;
>>>>>>> new api model
=======
>>>>>>> removed ProcessorContext
import io.confluent.streaming.RecordCollector;
<<<<<<< HEAD
import io.confluent.streaming.StorageEngine;
=======
import io.confluent.streaming.StateStore;
<<<<<<< HEAD
<<<<<<< HEAD
import io.confluent.streaming.Coordinator;
>>>>>>> wip
import io.confluent.streaming.internal.StreamGroup;
=======
import io.confluent.streaming.internal.PunctuationQueue;
=======
import io.confluent.streaming.internal.PunctuationQueue;
<<<<<<< HEAD
import io.confluent.streaming.internal.PunctuationSchedulerImpl;
>>>>>>> new api model
=======
>>>>>>> removed ProcessorContext
import io.confluent.streaming.kv.internals.RestoreFunc;
>>>>>>> new api model
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
  private final PunctuationQueue punctuationQueue = new PunctuationQueue();

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
  public RecordCollector recordCollector() { throw new UnsupportedOperationException("recordCollector() not supported."); }

  @Override
  public Map<String, Object> getContext() { throw new UnsupportedOperationException("getContext() not supported."); }

  @Override
  public File stateDir() { throw new UnsupportedOperationException("stateDir() not supported."); }

  @Override
  public Metrics metrics() { throw new UnsupportedOperationException("metrics() not supported."); }

  @Override
  public void restore(StateStore store, RestoreFunc func) { throw new UnsupportedOperationException("restore() not supported."); }
<<<<<<< HEAD

  public void register(StateStore store) { throw new UnsupportedOperationException("restore() not supported."); }
=======
>>>>>>> new api model

  public void register(StateStore store) { throw new UnsupportedOperationException("restore() not supported."); }

  @Override
  public void flush() { throw new UnsupportedOperationException("flush() not supported."); }

  @Override
<<<<<<< HEAD
<<<<<<< HEAD
  public void restore(StateStore engine) throws Exception { throw new UnsupportedOperationException("restore() not supported."); }
=======
  public void send(String topic, Object key, Object value) { throw new UnsupportedOperationException("send() not supported."); }
>>>>>>> new api model
=======
  public void send(String topic, Object key, Object value) { throw new UnsupportedOperationException("send() not supported."); }
>>>>>>> new api model

  @Override
  public void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer) { throw new UnsupportedOperationException("send() not supported."); }

  @Override
<<<<<<< HEAD
<<<<<<< HEAD
  public void schedule(Processor processor, long interval) {
    throw new UnsupportedOperationException("schedule() not supported");
=======
  public PunctuationScheduler getPunctuationScheduler(Processor processor) {
    return new PunctuationSchedulerImpl(punctuationQueue, processor);
>>>>>>> new api model
=======
  public void schedule(Processor processor, long interval) {
    throw new UnsupportedOperationException("schedule() not supported");
>>>>>>> removed ProcessorContext
  }

  @Override
  public void commit() { throw new UnsupportedOperationException("commit() not supported."); }

  @Override
  public String topic() { throw new UnsupportedOperationException("topic() not supported."); }

  @Override
  public int partition() { throw new UnsupportedOperationException("partition() not supported."); }

  @Override
  public long offset() { throw new UnsupportedOperationException("offset() not supported."); }

  @Override
  public long timestamp() { throw new UnsupportedOperationException("timestamp() not supported."); }

}
