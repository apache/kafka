package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.util.MinTimestampTracker;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by yasuhiro on 6/23/15.
 */
public class StreamSynchronizer<K, V> {

  public final String name;
  private final Ingestor ingestor;
  private final Chooser<K, V> chooser;
  private final TimestampExtractor<K, V> timestampExtractor;
  private final Map<TopicPartition, RecordQueue<K, V>> stash = new HashMap<TopicPartition, RecordQueue<K, V>>();
  private final int desiredUnprocessed;
  private final Map<TopicPartition, Long> consumedOffsets;
  private final PunctuationQueue punctuationQueue = new PunctuationQueue();

  private long streamTime = -1;
  private volatile int buffered = 0;

  StreamSynchronizer(String name,
                     Ingestor ingestor,
                     Chooser<K, V> chooser,
                     TimestampExtractor<K, V> timestampExtractor,
                     int desiredUnprocessedPerPartition) {
    this.name = name;
    this.ingestor = ingestor;
    this.chooser = chooser;
    this.timestampExtractor = timestampExtractor;
    this.desiredUnprocessed = desiredUnprocessedPerPartition;
    this.consumedOffsets = new HashMap<TopicPartition, Long>();
  }

  @SuppressWarnings("unchecked")
  public void addPartition(TopicPartition partition, Receiver<Object, Object> receiver) {
    synchronized (this) {
      RecordQueue<K, V> recordQueue = stash.get(partition);

      if (recordQueue == null) {
        stash.put(partition, createRecordQueue(partition, (Receiver<K, V>) receiver));
      } else {
        throw new IllegalStateException("duplicate partition");
      }
    }
  }

  @SuppressWarnings("unchecked")
  public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<K, V>> iterator) {
    synchronized (this) {
      RecordQueue recordQueue = stash.get(partition);
      if (recordQueue != null) {
        boolean wasEmpty = (recordQueue.size() == 0);

        while (iterator.hasNext()) {
          ConsumerRecord<K, V> record = iterator.next();
          recordQueue.add(record, timestampExtractor.extract(record.topic(), record.key(), record.value()));
          buffered++;
        }

        if (wasEmpty && recordQueue.size() > 0) chooser.add(recordQueue);

        // if we have buffered enough for this partition, pause
        if (recordQueue.size() > this.desiredUnprocessed) {
          ingestor.pause(partition);
        }
      }
    }
  }

  public PunctuationScheduler getPunctuationScheduler(Processor<?, ?> processor) {
    return new PunctuationSchedulerImpl(punctuationQueue, processor);
  }

  @SuppressWarnings("unchecked")
  public void process() {
    synchronized (this) {
      RecordQueue recordQueue = chooser.next();

      if (recordQueue == null) {
        ingestor.poll();
        return;
      }

      if (recordQueue.size() == this.desiredUnprocessed) {
        ingestor.unpause(recordQueue.partition(), recordQueue.offset());
      }

      if (recordQueue.size() == 0) return;

      long timestamp = recordQueue.currentStreamTime();
      ConsumerRecord<K, V> record = recordQueue.next();

      if (streamTime < timestamp) streamTime = timestamp;

      recordQueue.receiver.receive(record.key(), record.value(), streamTime);
      consumedOffsets.put(recordQueue.partition(), record.offset());

      if (recordQueue.size() > 0) chooser.add(recordQueue);

      buffered--;

      punctuationQueue.mayPunctuate(streamTime);
    }
  }

  public Map<TopicPartition, Long> consumedOffsets() {
    return this.consumedOffsets;
  }

  public int buffered() {
    return buffered;
  }

  public void close() {
    chooser.close();
    stash.clear();
  }

  protected RecordQueue<K, V> createRecordQueue(TopicPartition partition, Receiver<K, V> receiver) {
    return new RecordQueue<K, V>(partition, receiver, new MinTimestampTracker<ConsumerRecord<K, V>>());
  }

}
