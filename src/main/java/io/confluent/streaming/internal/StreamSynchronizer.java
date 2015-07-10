package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.util.MinTimestampTracker;
import io.confluent.streaming.util.ParallelExecutor;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by yasuhiro on 6/23/15.
 */
<<<<<<< HEAD
public class StreamSynchronizer<K, V> implements ParallelExecutor.Task {

  public static class Status {
    private AtomicBoolean pollRequired = new AtomicBoolean();

    public void pollRequired(boolean flag) {
      pollRequired.set(flag);
    }

    public boolean pollRequired() {
      return pollRequired.get();
    }
  }

  public final String name;
  private final Ingestor ingestor;
  private final Chooser<K, V> chooser;
  private final TimestampExtractor<K, V> timestampExtractor;
  private final Map<TopicPartition, RecordQueue<K, V>> stash = new HashMap<>();
=======
public class StreamSynchronizer implements SyncGroup {

  public final String name;
  private final Ingestor ingestor;
  private final Chooser chooser;
  private final TimestampExtractor timestampExtractor;
  private final Map<TopicPartition, RecordQueue> stash = new HashMap<>();
>>>>>>> removed some generics
  private final int desiredUnprocessed;
  private final Map<TopicPartition, Long> consumedOffsets;
  private final PunctuationQueue punctuationQueue = new PunctuationQueue();
  private final ArrayDeque<NewRecords<K, V>> newRecordBuffer = new ArrayDeque<>();

  private long streamTime = -1;
  private volatile int buffered = 0;

  StreamSynchronizer(String name,
                     Ingestor ingestor,
                     Chooser chooser,
                     TimestampExtractor timestampExtractor,
                     int desiredUnprocessedPerPartition) {
    this.name = name;
    this.ingestor = ingestor;
    this.chooser = chooser;
    this.timestampExtractor = timestampExtractor;
    this.desiredUnprocessed = desiredUnprocessedPerPartition;
    this.consumedOffsets = new HashMap<>();
<<<<<<< HEAD
=======
  }

  @Override
  public String name() {
    return name;
>>>>>>> removed some generics
  }

  @SuppressWarnings("unchecked")
  public void addPartition(TopicPartition partition, KStreamSource source) {
    synchronized (this) {
      RecordQueue recordQueue = stash.get(partition);

      if (recordQueue == null) {
        stash.put(partition, createRecordQueue(partition, source));
      } else {
        throw new IllegalStateException("duplicate partition");
      }
    }
  }

<<<<<<< HEAD
  public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<K, V>> iterator) {
=======
  @SuppressWarnings("unchecked")
<<<<<<< HEAD
  public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<Object, Object>> iterator) {
>>>>>>> removed some generics
=======
  public void addRecords(TopicPartition partition, Iterator<ConsumerRecord<byte[], byte[]>> iterator) {
>>>>>>> allow deserializer override at KStream construction
    synchronized (this) {
      newRecordBuffer.addLast(new NewRecords(partition, iterator));
    }
  }

  private void ingestNewRecords() {
    for (NewRecords<K, V> newRecords : newRecordBuffer) {
      TopicPartition partition = newRecords.partition;
<<<<<<< HEAD
      Iterator<ConsumerRecord<K, V>> iterator = newRecords.iterator;
=======
      Iterator<ConsumerRecord<byte[], byte[]>> iterator = newRecords.iterator;
>>>>>>> allow deserializer override at KStream construction

      RecordQueue recordQueue = stash.get(partition);
      if (recordQueue != null) {
        boolean wasEmpty = recordQueue.isEmpty();

        while (iterator.hasNext()) {
<<<<<<< HEAD
          ConsumerRecord<Object, Object> record = iterator.next();
          long timestamp = timestampExtractor.extract(record.topic(), record.key(), record.value());
<<<<<<< HEAD
          recordQueue.add(new StampedRecord<>(record, timestamp));
=======
          recordQueue.add(new StampedRecord(record, timestamp));
>>>>>>> removed some generics
=======
          ConsumerRecord<byte[], byte[]> record = iterator.next();

          // deserialize the raw record, extract the timestamp and put into the queue
          Deserializer<?> keyDeserializer = recordQueue.source.context().keyDeserializer();
          Deserializer<?> valDeserializer = recordQueue.source.context().valueDeserializer();

          Object key = keyDeserializer.deserialize(record.topic(), record.key());
          Object value = valDeserializer.deserialize(record.topic(), record.value());
          ConsumerRecord deserializedRecord = new ConsumerRecord<>(record.topic(), record.partition(), record.offset(), key, value);

          long timestamp = timestampExtractor.extract(record.topic(), key, value);
          recordQueue.add(new StampedRecord(deserializedRecord, timestamp));
>>>>>>> allow deserializer override at KStream construction
          buffered++;
        }

        int queueSize = recordQueue.size();
        if (wasEmpty && queueSize > 0) chooser.add(recordQueue);

        // if we have buffered enough for this partition, pause
        if (queueSize >= this.desiredUnprocessed) {
          ingestor.pause(partition);
        }
      }
    }
    newRecordBuffer.clear();
  }

  public PunctuationScheduler getPunctuationScheduler(Processor<?, ?> processor) {
    return new PunctuationSchedulerImpl(punctuationQueue, processor);
  }

  @SuppressWarnings("unchecked")
  public void process(Object context) {
    Status status = (Status) context;
    synchronized (this) {
      ingestNewRecords();

      RecordQueue recordQueue = chooser.next();
      if (recordQueue == null) {
        status.pollRequired(true);
        return;
      }

      if (recordQueue.size() == 0) throw new IllegalStateException("empty record queue");

      if (recordQueue.size() == this.desiredUnprocessed) {
        ingestor.unpause(recordQueue.partition(), recordQueue.offset());
      }

      long trackedTimestamp = recordQueue.trackedTimestamp();
      StampedRecord record = recordQueue.next();

      if (recordQueue.size() < this.desiredUnprocessed)
        status.pollRequired(true);

      if (streamTime < trackedTimestamp) streamTime = trackedTimestamp;

      recordQueue.source.receive(record.key(), record.value(), record.timestamp, streamTime);
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

  protected RecordQueue createRecordQueue(TopicPartition partition, KStreamSource source) {
    return new RecordQueue(partition, source, new MinTimestampTracker<ConsumerRecord<Object, Object>>());
  }

  private static class NewRecords {
    final TopicPartition partition;
    final Iterator<ConsumerRecord<byte[], byte[]>> iterator;

    NewRecords(TopicPartition partition, Iterator<ConsumerRecord<byte[], byte[]>> iterator) {
      this.partition = partition;
      this.iterator = iterator;
    }
  }
}
