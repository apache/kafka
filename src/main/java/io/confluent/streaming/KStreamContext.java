package io.confluent.streaming;

import io.confluent.streaming.kv.internals.RestoreFunc;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.File;
import java.util.Map;

/**
 * KStreamContext is access to the system resources for a stream processing job.
 * An instance of KStreamContext is created for each partition group.
 */
public interface KStreamContext {

  /**
   * Returns the partition group id
   * @return partition group id
   */
  int id();

  /**
   * Returns the key serializer
   * @return the key serializer
   */
  Serializer<?> keySerializer();

  /**
   * Returns the value serializer
   * @return the value serializer
   */
  Serializer<?> valueSerializer();

  /**
   * Returns the key deserializer
   * @return the key deserializer
   */
  Deserializer<?> keyDeserializer();

  /**
   * Returns the value deserializer
   * @return the value deserializer
   */
  Deserializer<?> valueDeserializer();

  /**
   * Returns a RecordCollector
   * @return RecordCollector
   */
  RecordCollector recordCollector();

  /**
   * Returns an application context registered to {@link StreamingConfig}.
   * @return an application context
   */
  Map<String, Object> getContext();

  /**
   * Returns the state directory for the partition.
   * @return the state directory
   */
  File stateDir();

  /**
   * Returns Metrics instance
   * @return Metrics
   */
  Metrics metrics();

  /**
   * Restores the specified storage engine.
   * @param store the storage engine
   */
  void restore(StateStore store, RestoreFunc restoreFunc);

  /**
<<<<<<< HEAD
   * Registers the specified storage enging.
   * @param store the storage engine
   */
  void register(StateStore store);

  /**
   * Ensures that the context is in the initialization phase where KStream topology can be constructed
<<<<<<< HEAD
=======
   * Flush the local state of this context
>>>>>>> new api model
=======
   *
   * Flush the local state of this context
>>>>>>> new api model
   */
  void flush();


  void send(String topic, Object key, Object value);

  void send(String topic, Object key, Object value, Serializer<Object> keySerializer, Serializer<Object> valSerializer);

<<<<<<< HEAD
<<<<<<< HEAD
  void schedule(Processor processor, long interval);
=======
  PunctuationScheduler getPunctuationScheduler(Processor processor);
>>>>>>> new api model
=======
  void schedule(Processor processor, long interval);
>>>>>>> removed ProcessorContext

  void commit();

  String topic();

  int partition();

  long offset();

  long timestamp();

}
