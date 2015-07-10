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
  private final TimestampExtractor timestampExtractor;
  private final HashMap<String, StreamSynchronizer> streamSynchronizerMap = new HashMap<>();
  private final StreamingConfig streamingConfig;
  private final ProcessorConfig processorConfig;
  private final Metrics metrics;
  private final File stateDir;
  private final ProcessorStateManager stateMgr;
  private Consumer<byte[], byte[]> restoreConsumer;

  public KStreamContextImpl(int id,
                            KStreamJob job,
                            Set<String> topics,
                            Ingestor ingestor,
                            Producer<byte[], byte[]> producer,
                            Coordinator coordinator,
                            StreamingConfig streamingConfig,
                            ProcessorConfig processorConfig,
                            Metrics metrics) {

    this(id, job, topics, ingestor,
        new RecordCollectors.SimpleRecordCollector(producer),
        coordinator, streamingConfig, processorConfig,
        new ProcessorStateManager(id, new File(processorConfig.stateDir, Integer.toString(id))),
        metrics);
  }

  @SuppressWarnings("unchecked")
  public KStreamContextImpl(int id,
                            KStreamJob job,
                            Set<String> topics,
                            Ingestor ingestor,
                            RecordCollectors.SimpleRecordCollector simpleCollector,
                            Coordinator coordinator,
                            StreamingConfig streamingConfig,
                            ProcessorConfig processorConfig,
                            ProcessorStateManager stateMgr,
                            Metrics metrics) {
    this.id = id;
    this.job = job;
    this.topics = topics;
    this.ingestor = ingestor;

    this.simpleCollector = simpleCollector;
    this.collector = new RecordCollectors.SerializingRecordCollector<Object, Object>(
        simpleCollector, (Serializer<Object>) streamingConfig.keySerializer(), (Serializer<Object>) streamingConfig.valueSerializer());

    this.coordinator = coordinator;
    this.streamingConfig = streamingConfig;
    this.processorConfig = processorConfig;

    this.timestampExtractor = this.streamingConfig.timestampExtractor();
    if (this.timestampExtractor == null) throw new NullPointerException("timestamp extractor is  missing");

    this.stateMgr = stateMgr;
    this.stateDir = this.stateMgr.baseDir();
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
  public KStream<?, ?> from(String topic, Deserializer<?> keyDeserializer, Deserializer<?> valDeserializer) {
    return from(topic, syncGroup(DEFAULT_SYNCHRONIZATION_GROUP), keyDeserializer, valDeserializer);
  }

  @Override
  @SuppressWarnings("unchecked")
  public KStream<?, ?> from(String topic, SyncGroup syncGroup, Deserializer<?> keyDeserializer, Deserializer<?> valDeserializer) {
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

        // override the deserializer classes if specified
        if (keyDeserializer == null && valDeserializer == null) {
          stream = new KStreamSource<Object, Object>(partitioningInfo, this);
        } else {
          StreamingConfig newConfig = this.streamingConfig.clone();
          if (keyDeserializer != null)
            newConfig.keyDeserializer(keyDeserializer);
          if (valDeserializer != null)
            newConfig.valueDeserializer(valDeserializer);

          KStreamContextImpl newContext = new KStreamContextImpl(
              this.id, this.job, this.topics, this.ingestor,
              this.simpleCollector, this.coordinator,
              newConfig, this.processorConfig,
              this.stateMgr, this.metrics);
          stream = new KStreamSource<Object, Object>(partitioningInfo, newContext);
        }

        sourceStreams.put(topic, stream);

        TopicPartition partition = new TopicPartition(topic, id);
        StreamSynchronizer streamSynchronizer = (StreamSynchronizer)syncGroup;
        streamSynchronizer.addPartition(partition, stream);
        ingestor.addStreamSynchronizerForPartition(streamSynchronizer, partition);
      }
      else {
        if (stream.partitioningInfo.syncGroup == syncGroup)
          throw new IllegalStateException("topic is already assigned a different synchronization group");

        // with this constraint we will not allow users to create KStream with different deser from the same topic,
        // this constraint may better be relaxed later.
        if (keyDeserializer != null && !keyDeserializer.getClass().equals(this.keyDeserializer().getClass()))
          throw new IllegalStateException("another source stream with the same topic but different key deserializer is already created");
        if (valDeserializer != null && !valDeserializer.getClass().equals(this.valueDeserializer().getClass()))
          throw new IllegalStateException("another source stream with the same topic but different value deserializer is already created");
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
    return syncGroup(name, new TimeBasedChooser());
  }

  @Override
  public SyncGroup roundRobinSyncGroup(String name) {
    return syncGroup(name, new RoundRobinChooser());
  }

  private SyncGroup syncGroup(String name, Chooser chooser) {
    int desiredUnprocessedPerPartition = processorConfig.bufferedRecordsPerPartition;

    synchronized (this) {
      StreamSynchronizer streamSynchronizer = streamSynchronizerMap.get(name);
      if (streamSynchronizer == null) {
        streamSynchronizer =
          new StreamSynchronizer(name, ingestor, chooser, timestampExtractor, desiredUnprocessedPerPartition);
        streamSynchronizerMap.put(name, streamSynchronizer);
      }
      return streamSynchronizer;
    }
  }


  @Override
  public void restore(StorageEngine engine) throws Exception {
    if (restoreConsumer == null) throw new IllegalStateException();

    stateMgr.registerAndRestore(simpleCollector, restoreConsumer, engine);
  }


  public Collection<StreamSynchronizer> streamSynchronizers() {
    return streamSynchronizerMap.values();
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
