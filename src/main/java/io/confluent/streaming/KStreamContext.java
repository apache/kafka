package io.confluent.streaming;

import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.io.File;
import java.util.Map;

/**
 * Created by yasuhiro on 6/19/15.
 */
public interface KStreamContext {

  String DEFAULT_SYNCHRONIZATION_GROUP = "defaultSynchronizationGroup";

  int id();

  Serializer<?> keySerializer();

  Serializer<?> valueSerializer();

  Deserializer<?> keyDeserializer();

  Deserializer<?> valueDeserializer();

  KStream<?, ?> from(String topic);

  KStream<?, ?> from(String topic, SyncGroup syncGroup);

  RecordCollector<byte[], byte[]> simpleRecordCollector();

  RecordCollector<Object, Object> recordCollector();

  Coordinator coordinator();

  Map<String, Object> getContext();

  File stateDir();

  Metrics metrics();

  SyncGroup syncGroup(String name);

  SyncGroup roundRobinSyncGroup(String name);

  void restore(StorageEngine engine) throws Exception;

}
