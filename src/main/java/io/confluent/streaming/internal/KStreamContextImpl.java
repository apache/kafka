package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.util.Util;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class KStreamContextImpl implements KStreamContext {

  private static final Logger log = LoggerFactory.getLogger(KStreamContextImpl.class);

  public final int id;
  private final KStreamJob job;
  private final Set<String> topics;

  private final Ingestor ingestor;
  private final RecordCollectors.SimpleRecordCollector simpleCollector;
  private final RecordCollector<Object, Object> collector;

  private final Coordinator coordinator;
  private final HashMap<String, KStreamSource<?, ?>> sourceStreams = new HashMap<String, KStreamSource<?, ?>>();
  private final HashMap<String, PartitioningInfo> partitioningInfos = new HashMap<String, PartitioningInfo>();
  private final TimestampExtractor<Object, Object> timestampExtractor;
  private final HashMap<String, SyncGroup> syncGroups = new HashMap<String, SyncGroup>();
  private final StreamingConfig streamingConfig;
  private final ProcessorConfig processorConfig;
  private final Metrics metrics;
  private final File stateDir;
  private final ProcessorStateManager stateMgr;
  private Consumer<byte[], byte[]> restoreConsumer;

  @SuppressWarnings("unchecked")
  public KStreamContextImpl(int id,
                            KStreamJob job,
                            Set<String> topics,
                            Ingestor ingestor,
                            Producer<byte[], byte[]> producer,
                            Coordinator coordinator,
                            StreamingConfig streamingConfig,
                            ProcessorConfig processorConfig,
                            Metrics metrics) {
    this.id = id;
    this.job = job;
    this.topics = topics;
    this.ingestor = ingestor;

    this.simpleCollector = new RecordCollectors.SimpleRecordCollector(producer);
    this.collector = new RecordCollectors.SerializingRecordCollector<Object, Object>(
      simpleCollector, (Serializer<Object>) streamingConfig.keySerializer(), (Serializer<Object>) streamingConfig.valueSerializer());

    this.coordinator = coordinator;
    this.streamingConfig = streamingConfig;
    this.processorConfig = processorConfig;

    this.timestampExtractor = (TimestampExtractor<Object, Object>)this.streamingConfig.timestampExtractor();
    if (this.timestampExtractor == null) throw new NullPointerException("timestamp extractor is  missing");

    this.stateDir = new File(processorConfig.stateDir, Integer.toString(id));
    this.stateMgr = new ProcessorStateManager(id, stateDir);
    this.metrics = metrics;
  }

  @Override
  public int id() {
    return id;
  }

  @Override
  public Serializer<?> keySerializer() {
    return streamingConfig.keySerializer();
  }

  @Override
  public Serializer<?> valueSerializer() {
    return streamingConfig.valueSerializer();
  }

  @Override
  public Deserializer<?> keyDeserializer() {
    return streamingConfig.keyDeserializer();
  }

  @Override
  public Deserializer<?> valueDeserializer() {
    return streamingConfig.valueDeserializer();
  }

  @Override
  public KStream<?, ?> from(String topic) {
    return from(topic, syncGroup(DEFAULT_SYNCHRONIZATION_GROUP));
  }

  @Override
  @SuppressWarnings("unchecked")
  public KStream<?, ?> from(String topic, SyncGroup syncGroup) {
    if (syncGroup == null) throw new NullPointerException();

    synchronized (this) {
      if (!topics.contains(topic))
        throw new IllegalArgumentException("topic not subscribed: " + topic);

      KStreamSource<?, ?> stream = sourceStreams.get(topic);

      if (stream == null) {
        PartitioningInfo partitioningInfo = partitioningInfos.get(topic);

        if (partitioningInfo == null) {
          partitioningInfo = new PartitioningInfo(syncGroup, ingestor.numPartitions(topic));
          partitioningInfos.put(topic, partitioningInfo);
        }

        stream = new KStreamSource<Object, Object>(partitioningInfo, this);
        sourceStreams.put(topic, stream);

        TopicPartition partition = new TopicPartition(topic, id);
        syncGroup.streamSynchronizer.addPartition(partition, (Receiver<Object, Object>)stream);
        ingestor.addStreamSynchronizerForPartition(syncGroup.streamSynchronizer, partition);
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
    return syncGroup(name, new TimeBasedChooser<Object, Object>());
  }

  @Override
  public SyncGroup roundRobinSyncGroup(String name) {
    return syncGroup(name, new RoundRobinChooser<Object, Object>());
  }

  private SyncGroup syncGroup(String name, Chooser<Object, Object> chooser) {
    int desiredUnprocessedPerPartition = processorConfig.bufferedRecordsPerPartition;

    synchronized (this) {
      SyncGroup syncGroup = syncGroups.get(name);
      if (syncGroup == null) {
        StreamSynchronizer<?, ?> streamSynchronizer =
          new StreamSynchronizer<Object, Object>(name, ingestor, chooser, timestampExtractor, desiredUnprocessedPerPartition);
        syncGroup = new SyncGroup(name, streamSynchronizer);
        syncGroups.put(name, syncGroup);
      }
      return syncGroup;
    }
  }


  @Override
  public void restore(StorageEngine engine) throws Exception {
    if (restoreConsumer == null) throw new IllegalStateException();

    stateMgr.registerAndRestore(simpleCollector, restoreConsumer, engine);
  }


  public Collection<SyncGroup> syncGroups() {
    return syncGroups.values();
  }

  public void init(Consumer<byte[], byte[]> restoreConsumer) throws IOException {
    stateMgr.init();
    try {
      this.restoreConsumer = restoreConsumer;
      job.init(this);
    }
    finally {
      this.restoreConsumer = null;
    }

    if (!topics.equals(sourceStreams.keySet())) {
      LinkedList<String> unusedTopics = new LinkedList<String>();
      for (String topic : topics) {
        if (!sourceStreams.containsKey(topic))
          unusedTopics.add(topic);
      }
      throw new KStreamException("unused topics: " + Util.mkString(unusedTopics));
    }
  }

  public void flush() {
    stateMgr.flush();
  }

  public void close() throws Exception {
    stateMgr.close(simpleCollector.offsets());
    job.close();
  }

}
