package io.confluent.streaming.internal;

import org.apache.kafka.clients.consumer.CommitType;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class IngestorImpl implements Ingestor {

  private static final Logger log = LoggerFactory.getLogger(IngestorImpl.class);

  private final Consumer<byte[], byte[]> consumer;
  private final Set<TopicPartition> unpaused = new HashSet<>();
  private final Set<TopicPartition> toBePaused = new HashSet<>();
  private final Map<TopicPartition, StreamGroup> streamSynchronizers = new HashMap<>();

  public IngestorImpl(Consumer<byte[], byte[]> consumer) {
    this.consumer = consumer;
  }

  public void init() {
    unpaused.clear();
    unpaused.addAll(consumer.subscriptions());
  }

  @Override
  public void poll(long timeoutMs) {
    synchronized (this) {
      for (TopicPartition partition : toBePaused) {
        doPause(partition);
      }
      toBePaused.clear();

      if (!unpaused.isEmpty()) {
        ConsumerRecords<byte[], byte[]> records = consumer.poll(timeoutMs);

        for (TopicPartition partition : unpaused) {
          StreamGroup streamGroup = streamSynchronizers.get(partition);

          if (streamGroup != null)
            streamGroup.addRecords(partition, records.records(partition).iterator());
          else
            log.warn("unused topic: " + partition.topic());
        }
      }
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
  public void commit(Map<TopicPartition, Long> offsets) {
    synchronized (this) {
      consumer.commit(offsets, CommitType.SYNC);
    }
  }

  @Override
  public int numPartitions(String topic) {
    return consumer.partitionsFor(topic).size();
  }

  @SuppressWarnings("unchecked")
  @Override
  public void addPartitionStreamToGroup(StreamGroup streamGroup, TopicPartition partition) {
    synchronized (this) {
      streamSynchronizers.put(partition, streamGroup);
      unpaused.add(partition);
    }
  }

  public void clear() {
    unpaused.clear();
    toBePaused.clear();
    streamSynchronizers.clear();
  }
}
