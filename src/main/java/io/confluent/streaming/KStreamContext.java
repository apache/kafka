package io.confluent.streaming;

import org.apache.kafka.common.metrics.Metrics;

import java.io.File;
import java.util.Map;

/**
 * Created by yasuhiro on 6/19/15.
 */
public interface KStreamContext {

  String DEFAULT_SYNCHRONIZATION_GROUP = "defaultSynchronizationGroup";

  int id();

  StreamingConfig streamingConfig();

  <K, V> KStream<K, V> from(String topic);

  <K, V> KStream<K, V> from(String topic, SyncGroup syncGroup);

  RecordCollector<byte[], byte[]> simpleRecordCollector();

  RecordCollector<Object, Object> recordCollector();

  Coordinator coordinator();

  Map<String, Object> getContext();

  File stateDir();

  Metrics metrics();

  SyncGroup syncGroup(String name);

  void restore(StorageEngine engine) throws Exception;

  void schedule(Processor<?, ?> processor, long time);
}
