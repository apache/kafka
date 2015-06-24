package io.confluent.streaming.internal;

import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.RecordCollector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.HashMap;

/**
 * Created by yasuhiro on 6/19/15.
 */
public class KStreamContextImpl implements KStreamContext {

  private final RecordCollector collector;
  private final HashMap<String, KStreamSource<?, ?>> sourceStreams = new HashMap<String, KStreamSource<?, ?>>();
  private final HashMap<String, PartitioningInfo> partitioningInfos;

  KStreamContextImpl(/* TODO: pass in consumer */ RecordCollector collector) {
    this(new HashMap<String, PartitioningInfo>(), collector);
  }

  KStreamContextImpl(/* TODO: pass in consumer */ HashMap<String, PartitioningInfo> partitioningInfos, RecordCollector collector) {
    this.collector = collector;
    this.partitioningInfos = partitioningInfos;
  }

  public <K, V> KStream<K, V> from(String topic) {
    synchronized (this) {
      KStreamSource<K, V> stream = (KStreamSource<K, V>)sourceStreams.get(topic);

      if (stream == null) {
        PartitioningInfo partitioningInfo = partitioningInfos.get(topic);

        if (partitioningInfo == null) {
          // TODO: use the consumer to get partitioning info
          int numPartitions = 1;

          partitioningInfo = new PartitioningInfo(null, numPartitions);
          partitioningInfos.put(topic, partitioningInfo);
        }

        stream = new KStreamSource<K, V>(partitioningInfo, this);
        sourceStreams.put(topic, stream);
      }

      return stream;
    }
  }

  public RecordCollector getRecordCollector() {
    return collector;
  }

  <K, V> void process(ConsumerRecord<K, V> record, long timestamp) {
    String topic = record.topic();

    KStreamSource<K, V> stream = (KStreamSource<K, V>)sourceStreams.get(record.topic());

    if (stream == null)
      throw new IllegalStateException("a stream for a topic not found: topic=" + topic);

    stream.receive(record.key(), record.value(), timestamp);
  }

  void punctuate(long timestamp) {
    for (KStreamSource<?, ?> stream : sourceStreams.values()) {
      stream.punctuate(timestamp);
    }
  }

  void flush() {
    for (KStreamSource<?, ?> stream : sourceStreams.values()) {
      stream.flush();
    }
  }

}
