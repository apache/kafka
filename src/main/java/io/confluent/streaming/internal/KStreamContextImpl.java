package io.confluent.streaming.internal;

import io.confluent.streaming.Coordinator;
import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamException;
import io.confluent.streaming.KStreamJob;
import io.confluent.streaming.RecordCollector;
import io.confluent.streaming.StorageEngine;
import io.confluent.streaming.StreamingConfig;
import io.confluent.streaming.SyncGroup;
import io.confluent.streaming.TimestampExtractor;
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
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

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
  private final HashMap<String, KStreamSource<?, ?>> sourceStreams = new HashMap<>();
  private final HashMap<String, PartitioningInfo> partitioningInfos = new HashMap<>();
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
    this.collector = new RecordCollectors.SerializingRecordCollector(
        simpleCollector, streamingConfig.keySerializer(), streamingConfig.valueSerializer());

    this.coordinator = coordinator;
    this.streamingConfig = streamingConfig;
    this.processorConfig = processorConfig;

    this.timestampExtractor = this.streamingConfig.timestampExtractor();
    if (this.timestampExtractor == null) throw new NullPointerException("timestamp extractor is  missing");

    this.stateMgr = stateMgr;
    this.stateDir = this.stateMgr.baseDir();
    this.metrics = metrics;
  }

  public RecordCollectors.SimpleRecordCollector simpleRecordCollector() { return this.simpleCollector; }

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
  public KStream<?, ?> from(String... topics) {
    return from(syncGroup(DEFAULT_SYNCHRONIZATION_GROUP), null, null, topics);
  }

  @Override
  public KStream<?, ?> from(SyncGroup syncGroup, String... topics) {
    return from(syncGroup, null, null, topics);
  }

  @Override
  public <K, V> KStream<K, V> from(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
    return from(syncGroup(DEFAULT_SYNCHRONIZATION_GROUP), keyDeserializer, valDeserializer, topics);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <K, V> KStream<K, V> from(SyncGroup syncGroup, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
    if (syncGroup == null) throw new NullPointerException();

    KStreamSource<?, ?> stream = null;

    synchronized (this) {
      // iterate over the topics and check if the stream has already been created for them
      for (String topic : topics) {
        if (!this.topics.contains(topic))
          throw new IllegalArgumentException("topic not subscribed: " + topic);

        KStreamSource<?, ?> streamForTopic = sourceStreams.get(topic);

        if (stream == null) {
          if (streamForTopic != null)
            stream = streamForTopic;
        } else {
          if (streamForTopic != null) {
            if (!stream.equals(streamForTopic))
              throw new IllegalArgumentException("another stream created with the same topic " + topic);
          } else {
            sourceStreams.put(topic, stream);
          }
        }
      }

      // if there is no stream for any of the topics, create one
      if (stream == null) {
        // create stream metadata
        Map<String, PartitioningInfo> topicPartitionInfos = new HashMap<>();
        for (String topic : topics) {
          PartitioningInfo partitioningInfo = this.partitioningInfos.get(topic);

          if (partitioningInfo == null) {
            partitioningInfo = new PartitioningInfo(ingestor.numPartitions(topic));
            this.partitioningInfos.put(topic, partitioningInfo);
          }

          topicPartitionInfos.put(topic, partitioningInfo);
        }

        KStreamMetadata streamMetadata = new KStreamMetadata(syncGroup, topicPartitionInfos);

        // override the deserializer classes if specified
        if (keyDeserializer == null && valDeserializer == null) {
          stream = new KStreamSource<Object, Object>(streamMetadata, this);
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
          stream = new KStreamSource<Object, Object>(streamMetadata, newContext);
        }

        // update source stream map
        for (String topic : topics) {
          if (sourceStreams.containsKey(topic))
            sourceStreams.put(topic, stream);

          TopicPartition partition = new TopicPartition(topic, id);
          StreamSynchronizer streamSynchronizer = (StreamSynchronizer)syncGroup;
          streamSynchronizer.addPartition(partition, stream);
          ingestor.addStreamSynchronizerForPartition(streamSynchronizer, partition);
        }
      } else {
        if (stream.metadata.syncGroup == syncGroup)
          throw new IllegalStateException("topic is already assigned a different synchronization group");

        // TODO: with this constraint we will not allow users to create KStream with different
        // deser from the same topic, this constraint may better be relaxed later.
        if (keyDeserializer != null && !keyDeserializer.getClass().equals(this.keyDeserializer().getClass()))
          throw new IllegalStateException("another source stream with the same topic but different key deserializer is already created");
        if (valDeserializer != null && !valDeserializer.getClass().equals(this.valueDeserializer().getClass()))
          throw new IllegalStateException("another source stream with the same topic but different value deserializer is already created");
      }

      return (KStream<K, V>) stream;
    }
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
