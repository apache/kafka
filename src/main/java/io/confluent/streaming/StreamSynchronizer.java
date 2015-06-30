package io.confluent.streaming;

import io.confluent.streaming.internal.*;
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
  private final RegulatedConsumer<K, V> consumer;
  private final Chooser<K, V> chooser;
  private final TimestampExtractor<K, V> timestampExtractor;
  private final Map<TopicPartition, RecordQueueWrapper> stash = new HashMap<TopicPartition, RecordQueueWrapper>();
  private final int desiredUnprocessed;
  private final Map<TopicPartition, Long> consumedOffsets;
  private final PunctuationQueue punctuationQueue = new PunctuationQueue();

  private long streamTime = -1;
  private volatile int buffered = 0;

  public StreamSynchronizer(String name,
                            RegulatedConsumer<K, V> consumer,
                            Chooser<K, V> chooser,
                            TimestampExtractor<K, V> timestampExtractor,
                            int desiredNumberOfUnprocessedRecords) {
    this.name = name;
    this.consumer = consumer;
    this.chooser = chooser;
    this.timestampExtractor = timestampExtractor;
    this.desiredUnprocessed = desiredNumberOfUnprocessedRecords;
    this.consumedOffsets = new HashMap<TopicPartition, Long>();

  }

  public void addPartition(TopicPartition partition, final Receiver<Object, Object> receiver) {
    synchronized (this) {
      RecordQueueWrapper queue = stash.get(partition);

      if (queue == null) {
        stash.put(partition, new RecordQueueWrapper(createRecordQueue(partition), receiver));
      } else {
        throw new IllegalStateException("duplicate partition");
      }
    }
  }

  public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<K, V>> iterator) {
    synchronized (this) {
      RecordQueueWrapper queue = stash.get(partition);
      if (queue != null) {
        boolean wasEmpty = (queue.size() == 0);

        while (iterator.hasNext()) {
          ConsumerRecord<K, V> record = iterator.next();
          queue.add(record, timestampExtractor.extract(record.topic(), record.key(), record.value()));
          buffered++;
        }

        if (wasEmpty && queue.size() > 0) chooser.add(queue);

        // if we have buffered enough for this partition, pause
        if (queue.size() > this.desiredUnprocessed) {
          consumer.pause(partition);
        }
      }
    }
  }

  public PunctuationScheduler getPunctuationScheduler(Processor<?, ?> processor) {
    return new PunctuationSchedulerImpl(punctuationQueue, processor);
  }

  public void process() {
    synchronized (this) {
      RecordQueueWrapper recordQueue = (RecordQueueWrapper)chooser.next();

      if (recordQueue == null) {
        consumer.poll();
        return;
      }

      if (recordQueue.size() == this.desiredUnprocessed) {
        consumer.unpause(recordQueue.partition(), recordQueue.offset());
      }

      if (recordQueue.size() == 0) return;

      recordQueue.process();

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

  protected RecordQueue<K, V> createRecordQueue(TopicPartition partition) {
    return new RecordQueueImpl<K, V>(partition, new MinTimestampTracker<ConsumerRecord<K, V>>());
  }

  private class RecordQueueWrapper implements RecordQueue<K, V> {

    private final RecordQueue<K, V> queue;
    private final Receiver<Object, Object> receiver;

    RecordQueueWrapper(RecordQueue<K, V> queue, Receiver<Object, Object> receiver) {
      this.queue = queue;
      this.receiver = receiver;
    }

    void process() {
      long timestamp = queue.currentStreamTime();
      ConsumerRecord<K, V> record = queue.next();

      if (streamTime < timestamp) streamTime = timestamp;

      receiver.receive(record.key(), record.value(), streamTime);
      consumedOffsets.put(queue.partition(), record.offset());
    }

    @Override
    public TopicPartition partition() {
      return queue.partition();
    }

    @Override
    public void add(ConsumerRecord<K, V> value, long timestamp) {
      queue.add(value, timestamp);
    }

    public ConsumerRecord<K, V> next() {
      return queue.next();
    }

    @Override
    public long offset() {
      return queue.offset();
    }

    @Override
    public int size() {
      return queue.size();
    }

    @Override
    public long currentStreamTime() {
      return queue.currentStreamTime();
    }

  }

}
