package io.confluent.streaming;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.File;
import java.util.Map;

/**
 * KStreamContext is the interface that allows an implementation of {@link KStreamJob#init(KStreamContext)} to create KStream instances.
 * It also provides access to the system resources for a stream processing job.
 * An instance of KStreamContext is created for each partition.
 */
public interface KStreamContext {

  String DEFAULT_SYNCHRONIZATION_GROUP = "defaultSynchronizationGroup";

  /**
   * Returns the partition id
   * @return partition id
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

  // TODO: support regex topic matching in from() calls, for example:
  // context.from("Topic*PageView")

  /**
   * Creates a KStream instance for the specified topics. The stream is added to the default synchronization group.
   * @param topics the topic name
   * @return KStream
   */
  KStream<?, ?> from(String... topics);

  /**
   * Creates a KStream instance for the specified topics. The stream is added to the specified synchronization group.
   * @param syncGroup the synchronization group
   * @param topics the topic name
   * @return KStream
   */
  KStream<?, ?> from(SyncGroup syncGroup, String... topics);


  /**
   * Creates a KStream instance for the specified topic. The stream is added to the default synchronization group.
   * @param keyDeserializer key deserializer used to read this source KStream,
   *                        if not specified the default deserializer defined in the configs will be used
   * @param valDeserializer value deserializer used to read this source KStream,
   *                        if not specified the default deserializer defined in the configs will be used
   * @param topics the topic name
   * @return KStream
   */
  <K, V> KStream<K, V> from(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics);

  /**
   * Creates a KStream instance for the specified topic. The stream is added to the specified synchronization group.
   * @param syncGroup the synchronization group
   * @param keyDeserializer key deserializer used to read this source KStream,
   *                        if not specified the default deserializer defined in the configs will be used
   * @param valDeserializer value deserializer used to read this source KStream,
   *                        if not specified the default deserializer defined in the configs will be used
   * @param topics the topic name
   * @return KStream
   */
  <K, V> KStream<K, V> from(SyncGroup syncGroup, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics);

  /**
   * Returns a RecordCollector which applies the serializer to key and value.
   * @return RecordCollector
   */
  RecordCollector<Object, Object> recordCollector();

  /**
   * Returns {@link Coordinator}.
   * @return Coordinator
   */
  Coordinator coordinator();

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
   * Creates a synchronization group with the given name.
   * @param name the synchronization group name
   * @return a synchronization group
   */
  SyncGroup syncGroup(String name);

  /**
   * Creates a round robin synchronization group with the given name.
   * @param name the synchronization group name
   * @return a round robin synchronization group
   */
  SyncGroup roundRobinSyncGroup(String name);

  /**
   * Restores the specified storage engine.
   * @param engine the storage engine
   * @throws Exception an exception thrown by the engine
   */
  void restore(StorageEngine engine) throws Exception;

}
