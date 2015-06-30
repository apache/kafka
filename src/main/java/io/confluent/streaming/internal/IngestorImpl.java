package io.confluent.streaming.internal;

import io.confluent.streaming.StreamSynchronizer;
import io.confluent.streaming.util.FilteredIterator;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.*;

public class IngestorImpl<K, V> implements Ingestor {

  private final Consumer<byte[], byte[]> consumer;
  private final Set<TopicPartition> unpaused = new HashSet<TopicPartition>();
  private final Set<TopicPartition> toBePaused = new HashSet<TopicPartition>();
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final long pollTimeMs;
  private final Map<TopicPartition, StreamSynchronizer<K, V>> streamSynchronizers =
    new HashMap<TopicPartition, StreamSynchronizer<K, V>>();

  public IngestorImpl(Consumer<byte[], byte[]> consumer,
                      Deserializer<K> keyDeserializer,
                      Deserializer<V> valueDeserializer,
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

  public void poll() {
    poll(pollTimeMs);
  }

  public void poll(long timeoutMs) {
    for (TopicPartition partition : toBePaused) {
      doPause(partition);
    }
    toBePaused.clear();

    ConsumerRecords<byte[], byte[]> records = consumer.poll(timeoutMs);

    for (TopicPartition partition : unpaused) {
      streamSynchronizers.get(partition).addRecords(partition, new DeserializingIterator(records.records(partition).iterator()));
    }
  }

  public void pause(TopicPartition partition) {
    toBePaused.add(partition);
  }

  private void doPause(TopicPartition partition) {
    consumer.seek(partition, Long.MAX_VALUE); // hack: stop consuming from this partition by setting a big offset
    unpaused.remove(partition);
  }

  public void unpause(TopicPartition partition, long lastOffset) {
    consumer.seek(partition, lastOffset);
    unpaused.add(partition);
  }

  public void clear() {
    unpaused.clear();
    toBePaused.clear();
    streamSynchronizers.clear();
  }

  private class DeserializingIterator extends FilteredIterator<ConsumerRecord<K, V>, ConsumerRecord<byte[], byte[]>> {

    DeserializingIterator(Iterator<ConsumerRecord<byte[], byte[]>> inner) {
      super(inner);
    }

    protected ConsumerRecord<K, V> filter(ConsumerRecord<byte[], byte[]> record) {
      K key = keyDeserializer.deserialize(record.topic(), record.key());
      V value = valueDeserializer.deserialize(record.topic(), record.value());
      return new ConsumerRecord<K, V>(record.topic(), record.partition(), record.offset(), key, value);
    }

  }

}
