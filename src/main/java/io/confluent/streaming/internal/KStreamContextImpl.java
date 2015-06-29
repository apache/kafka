package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class KStreamContextImpl implements KStreamContext {

  private static final Logger log = LoggerFactory.getLogger(KStreamContextImpl.class);

  public final int id;
  private final Set<String> topics;

  private final RegulatedConsumer<Object, Object> regulatedConsumer;
  private final RecordCollectors.SimpleRecordCollector simpleCollector;
  private final RecordCollector<Object, Object> collector;

  private final Coordinator coordinator;
  private final HashMap<String, KStreamSource<?, ?>> sourceStreams = new HashMap<String, KStreamSource<?, ?>>();
  private final HashMap<String, PartitioningInfo> partitioningInfos = new HashMap<String, PartitioningInfo>();
  private final StreamSynchronizerFactory<Object, Object> streamSynchronizerFactory;
  private final HashMap<String, SyncGroup> syncGroups = new HashMap<String, SyncGroup>();
  private final StreamingConfig streamingConfig;
  private final ProcessorConfig processorConfig;
  private final Metrics metrics;
  private final File stateDir;
  private final ProcessorContext processorContext;
  private final ProcessorStateManager state;
  private final Map<String, StorageEngine> stores = new HashMap<String, StorageEngine>();
  private Consumer<byte[], byte[]> restoreConsumer;

  public KStreamContextImpl(int id,
                            RegulatedConsumer<?, ?> regulatedConsumer,
                            Producer<byte[], byte[]> producer,
                            Coordinator coordinator,
                            StreamingConfig streamingConfig,
                            ProcessorConfig processorConfig,
                            File stateDir,
                            Metrics metrics) {
    this.id = id;
    this.topics = streamingConfig.topics();
    this.regulatedConsumer = (RegulatedConsumer<Object, Object>)regulatedConsumer;

    this.simpleCollector = new RecordCollectors.SimpleRecordCollector(producer);
    this.collector = new RecordCollectors.SerializingRecordCollector<Object, Object>(
      simpleCollector, (Serializer<Object>) streamingConfig.keySerializer(), (Serializer<Object>) streamingConfig.valueSerializer());

    this.coordinator = coordinator;
    this.streamingConfig = streamingConfig;
    this.processorConfig = processorConfig;

    this.streamSynchronizerFactory = (StreamSynchronizerFactory<Object, Object>)this.streamingConfig.streamSynchronizerFactory();
    if (this.streamSynchronizerFactory == null) throw new NullPointerException();

    this.stateDir = stateDir;
    this.state = new ProcessorStateManager(id, stateDir);
    this.processorContext = new ProcessorContext(id, streamingConfig, stateDir, metrics);

    this.metrics = metrics;
  }

  @Override
  public <K, V> KStream<K, V> from(String topic) {
    return from(topic, syncGroup(DEFAULT_SYNCHRONIZATION_GROUP));
  }

  @Override
  public <K, V> KStream<K, V> from(String topic, SyncGroup syncGroup) {
    if (syncGroup == null) throw new NullPointerException();

    synchronized (this) {
      if (!topics.contains(topic))
        throw new IllegalArgumentException("topic not subscribed: " + topic);

      KStreamSource<K, V> stream = (KStreamSource<K, V>)sourceStreams.get(topic);

      if (stream == null) {
        PartitioningInfo partitioningInfo = partitioningInfos.get(topic);

        if (partitioningInfo == null) {
          // TODO: use the consumer to get partitioning info
          int numPartitions = 1;

          partitioningInfo = new PartitioningInfo(syncGroup, numPartitions);
          partitioningInfos.put(topic, partitioningInfo);
        }

        stream = new KStreamSource<K, V>(partitioningInfo, this);
        sourceStreams.put(topic, stream);

        syncGroup.streamSynchronizer.addPartition(new TopicPartition(topic, id), (Receiver<Object, Object>)stream);
      }
      else {
        if (stream.partitioningInfo.syncGroup == syncGroup)
          throw new IllegalStateException("topic is already assigned a different synchronization group");
      }

      return stream;
    }
  }

  @Override
  public RecordCollector<byte[], byte[]> simpleRecordCollector() {
    return simpleCollector;
  }

  @Override
  public RecordCollector<Object, Object> recordCollector() {
    return collector;
  }

  @Override
  public Coordinator coordinator() {
    return coordinator;
  }

  @Override
  public Map<String, Object> getContext() {
    return streamingConfig.context();
  }

  @Override
  public File stateDir() {
    return stateDir;
  }

  @Override
  public Metrics metrics() {
    return metrics;
  }

  @Override
  public SyncGroup syncGroup(String name) {
    synchronized (this) {
      SyncGroup syncGroup = syncGroups.get(name);
      if (syncGroup == null) {
        StreamSynchronizer<?, ?> streamSynchronizer =
          streamSynchronizerFactory.create(name, regulatedConsumer, processorConfig.bufferedRecordsPerPartition);
        syncGroup = new SyncGroup(name, streamSynchronizer);
        syncGroups.put(name, syncGroup);
      }
      return syncGroup;
    }
  }

  @Override
  public void restore(StorageEngine engine) throws Exception {
    if (restoreConsumer == null) throw new IllegalStateException();

    state.registerAndRestore(simpleCollector, restoreConsumer, engine);
  }

  public Collection<SyncGroup> syncGroups() {
    return syncGroups.values();
  }

  public void init(Consumer<byte[], byte[]> restoreConsumer, KStreamJob job) {
    this.restoreConsumer = restoreConsumer;

    job.build(this);

    this.restoreConsumer = null;
  }

  public void punctuate(long timestamp) {
    for (KStreamSource<?, ?> stream : sourceStreams.values()) {
      stream.punctuate(timestamp);
    }
  }

  public void flush() {
    for (KStreamSource<?, ?> stream : sourceStreams.values()) {
      stream.flush();
    }
    state.flush();
  }

  public void close() throws Exception {
    state.close(simpleCollector.offsets());
  }

}
