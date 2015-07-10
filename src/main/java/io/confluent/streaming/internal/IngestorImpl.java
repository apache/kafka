package io.confluent.streaming.internal;

import io.confluent.streaming.util.FilteredIterator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IngestorImpl implements Ingestor {

  private static final Logger log = LoggerFactory.getLogger(IngestorImpl.class);

  private final Consumer<byte[], byte[]> consumer;
  private final Set<TopicPartition> unpaused = new HashSet<>();
  private final Set<TopicPartition> toBePaused = new HashSet<>();
  private final Deserializer<Object> keyDeserializer;
  private final Deserializer<Object> valueDeserializer;
  private final long pollTimeMs;
  private final Map<TopicPartition, StreamSynchronizer> streamSynchronizers = new HashMap<>();

  public IngestorImpl(Consumer<byte[], byte[]> consumer,
                      Deserializer<Object> keyDeserializer,
                      Deserializer<Object> valueDeserializer,
                      long pollTimeMs) {
    this.consumer = consumer;
    this.keyDeserializer = keyDeserializer;
    this.valueDeserializer = valueDeserializer;
    this.pollTimeMs = pollTimeMs;
  }

  public void init() {
    unpaused.clear();
    unpaused.addAll(consumer.subscriptions());
  }

  @Override
  public void poll() {
    poll(pollTimeMs);
  }

  @Override
  public void poll(long timeoutMs) {
    ConsumerRecords<byte[], byte[]> records;

    synchronized (this) {
      for (TopicPartition partition : toBePaused) {
        doPause(partition);
      }
      toBePaused.clear();

      records = consumer.poll(timeoutMs);
    }

    for (TopicPartition partition : unpaused) {
      StreamSynchronizer streamSynchronizer = streamSynchronizers.get(partition);

      if (streamSynchronizer != null)
        streamSynchronizer.addRecords(partition, records.records(partition).iterator());
      else
        log.warn("unused topic: " + partition.topic());
    }
  }

  @Override
  public void pause(TopicPartition partition) {
    toBePaused.add(partition);
  }

  private void doPause(TopicPartition partition) {
    consumer.seek(partition, Long.MAX_VALUE); // hack: stop consuming from this partition by setting a big offset
    unpaused.remove(partition);
  }

  @Override
  public void unpause(TopicPartition partition, long lastOffset) {
    synchronized (this) {
      consumer.seek(partition, lastOffset);
      unpaused.add(partition);
    }
  }

  @Override
  public int numPartitions(String topic) {
    return consumer.partitionsFor(topic).size();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void addStreamSynchronizerForPartition(StreamSynchronizer streamSynchronizer, TopicPartition partition) {
    synchronized (this) {
      streamSynchronizers.put(partition, streamSynchronizer);
      unpaused.add(partition);
    }
  }

  @Override
  public void removeStreamSynchronizerForPartition(TopicPartition partition) {
    synchronized (this) {
      streamSynchronizers.remove(partition);
      unpaused.remove(partition);
      toBePaused.remove(partition);
    }
  }

  public void clear() {
    unpaused.clear();
    toBePaused.clear();
    streamSynchronizers.clear();
  }
}
