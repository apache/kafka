package io.confluent.streaming.internal;

import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamException;
import io.confluent.streaming.KStreamJob;
import io.confluent.streaming.RecordCollector;
import io.confluent.streaming.StorageEngine;
import io.confluent.streaming.StreamingConfig;
import io.confluent.streaming.TimestampExtractor;
import io.confluent.streaming.util.Util;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
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
  private final Ingestor ingestor;
  private final RecordCollectorImpl collector;

  private final HashMap<String, KStreamSource<?, ?>> sourceStreams = new HashMap<>();
  private final HashMap<String, PartitioningInfo> partitioningInfos = new HashMap<>();
  private final TimestampExtractor timestampExtractor;
  private final HashMap<String, StreamGroup> streamGroups = new HashMap<>();
  private final StreamingConfig streamingConfig;
  private final ProcessorConfig processorConfig;
  private final Metrics metrics;
  private final File stateDir;
  private final ProcessorStateManager stateMgr;
  private Consumer<byte[], byte[]> restoreConsumer;

  @SuppressWarnings("unchecked")
  public KStreamContextImpl(int id,
                            KStreamJob job,
                            Ingestor ingestor,
                            RecordCollectorImpl collector,
                            StreamingConfig streamingConfig,
                            ProcessorConfig processorConfig,
                            Metrics metrics) {
    this.id = id;
    this.job = job;
    this.ingestor = ingestor;
    this.collector = collector;
    this.streamingConfig = streamingConfig;
    this.processorConfig = processorConfig;

    this.timestampExtractor = this.streamingConfig.timestampExtractor();
    if (this.timestampExtractor == null) throw new NullPointerException("timestamp extractor is missing");

    this.stateMgr = new ProcessorStateManager(id, new File(processorConfig.stateDir, Integer.toString(id)));
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
  public KStream<?, ?> from(String... topics) {
    return from(streamGroup(getNextGroupName()), null, null, topics);
  }

  @Override
  public <K, V> KStream<K, V> from(Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
    return from(streamGroup(getNextGroupName()), keyDeserializer, valDeserializer, topics);
  }

  private String getNextGroupName() {
    return "StreamGroup-" + STREAM_GROUP_INDEX.getAndIncrement();
  }

  @SuppressWarnings("unchecked")
  private <K, V> KStream<K, V> from(StreamGroup streamGroup, Deserializer<K> keyDeserializer, Deserializer<V> valDeserializer, String... topics) {
    ensureInitialization();

    if (streamGroup == null) throw new IllegalArgumentException("unspecified stream group");

    Set<String> fromTopics;

    synchronized (this) {
      // if topics not specified, use all the topics be default
<<<<<<< HEAD
      if (topics == null) {
        fromTopics = ingestor.topics();
=======
      if (topics == null || topics.length == 0) {
        fromTopics = this.topics;
>>>>>>> fix vararg in KStreamContextImpl.from()
      } else {
        fromTopics = Collections.unmodifiableSet(Util.mkSet(topics));
      }

      // iterate over the topics and check if the stream has already been created for them
      for (String topic : fromTopics) {
        if (!ingestor.topics().contains(topic))
          throw new IllegalArgumentException("topic not subscribed: " + topic);

        if (sourceStreams.containsKey(topic))
          throw new IllegalArgumentException("another stream created with the same topic " + topic);
      }

      // create stream metadata
      Map<String, PartitioningInfo> topicPartitionInfos = new HashMap<>();
      for (String topic : fromTopics) {
        PartitioningInfo partitioningInfo = this.partitioningInfos.get(topic);

        if (partitioningInfo == null) {
          partitioningInfo = new PartitioningInfo(ingestor.numPartitions(topic));
          this.partitioningInfos.put(topic, partitioningInfo);
        }

        topicPartitionInfos.put(topic, partitioningInfo);
      }
      KStreamMetadata streamMetadata = new KStreamMetadata(streamGroup, topicPartitionInfos);

      // override the deserializer classes if specified
      KStreamSource<K, V> stream = new KStreamSource<>(
        streamMetadata,
        this,
        (Deserializer<K>) (keyDeserializer == null ? keyDeserializer() : keyDeserializer),
        (Deserializer<V>) (valDeserializer == null ? valueDeserializer() : valDeserializer)
      );

      // update source stream map
      for (String topic : fromTopics) {
        sourceStreams.put(topic, stream);

        TopicPartition partition = new TopicPartition(topic, id);
        streamGroup.addPartition(partition, stream);
      }

      return stream;
    }
  }

  @Override
  public RecordCollector recordCollector() {
    return collector;
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
  public StreamGroup streamGroup(String name) {
    return streamGroup(name, new TimeBasedChooser());
  }

  @Override
  public StreamGroup roundRobinStreamGroup(String name) {
    return streamGroup(name, new RoundRobinChooser());
  }

  private StreamGroup streamGroup(String name, Chooser chooser) {
    int desiredUnprocessedPerPartition = processorConfig.bufferedRecordsPerPartition;

    synchronized (this) {
      StreamGroup streamGroup = streamGroups.get(name);
      if (streamGroup == null) {
        streamGroup =
          new StreamGroup(name, ingestor, chooser, timestampExtractor, desiredUnprocessedPerPartition);
        streamGroups.put(name, streamGroup);
      }
      return streamGroup;
    }
  }


  @Override
  public void restore(StorageEngine engine) throws Exception {
    ensureInitialization();

    stateMgr.registerAndRestore(collector, restoreConsumer, engine);
  }

  @Override
  public void ensureInitialization() {
    if (restoreConsumer != null)
      throw new IllegalStateException("context initialization is already finished");
  }

  public Collection<StreamGroup> streamSynchronizers() {
    return streamGroups.values();
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

    // add partition -> stream group mappings to the ingestor
    for (Map.Entry<String, KStreamSource<?,?>> entry : sourceStreams.entrySet()) {
      TopicPartition partition = new TopicPartition(entry.getKey(), id);
      StreamGroup streamGroup = entry.getValue().metadata.streamGroup;
      ingestor.addPartitionStreamToGroup(streamGroup, partition);
    }

    if (!ingestor.topics().equals(sourceStreams.keySet())) {
      LinkedList<String> unusedTopics = new LinkedList<>();
      for (String topic : ingestor.topics()) {
        if (!sourceStreams.containsKey(topic))
          unusedTopics.add(topic);
      }
      throw new KStreamException("unused topics: " + Util.mkString(unusedTopics));
    }
  }

  @Override
  public void flush() {
    stateMgr.flush();
  }

  public void close() throws Exception {
    stateMgr.close(collector.offsets());
    job.close();
  }

}
