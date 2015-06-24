package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;

/**
 * Created by yasuhiro on 6/17/15.
 */
abstract class KStreamImpl<K,V, K1, V1> implements KStream<K, V>, Receiver<K1, V1> {

  private final ArrayList<Receiver<K, V>> nextReceivers = new ArrayList<Receiver<K, V>>(1);
  private long punctuatedAt = 0L;
  final PartitioningInfo partitioningInfo;
  final KStreamContextImpl context;

  protected KStreamImpl(PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    this.partitioningInfo = partitioningInfo;
    this.context = context;
  }

  public KStream<K, V> filter(Predicate<K, V> predicate) {
    return chain(new KStreamFilter<K, V>(predicate, partitioningInfo, context));
  }

  public KStream<K, V> filterOut(final Predicate<K, V> predicate) {
    return filter(new Predicate<K, V>() {
      public boolean apply(K key, V value) {
        return !predicate.apply(key, value);
      }
    });
  }

  public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K1, V1, K, V> mapper) {
    return chain(new KStreamMap<K1, V1, K, V>(mapper, context));
  }

  public <V1> KStream<K, V1> mapValues(ValueMapper<V1, V> mapper) {
    return chain(new KStreamMapValues<K, V1, V>(mapper, partitioningInfo, context));
  }

  public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K1, ? extends Iterable<V1>, K, V> mapper) {
    return chain(new KStreamFlatMap<K1, V1, K, V>(mapper, context));
  }

  public <V1> KStream<K, V1> flatMapValues(ValueMapper<? extends Iterable<V1>, V> mapper) {
    return chain(new KStreamFlatMapValues<K, V1, V>(mapper, partitioningInfo, context));
  }

  public KStreamWindowed<K, V> with(Window<K, V> window) {
    return (KStreamWindowed)chain(new KStreamWindowedImpl(window, partitioningInfo, context));
  }

  public <V1, V2> KStream<K, V2> nestedLoop(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor)
    throws NotCopartitionedException {

    KStreamWindowedImpl<K, V1> otherImpl = (KStreamWindowedImpl<K, V1>) other;

    if (!partitioningInfo.isJoinCompatibleWith(otherImpl.partitioningInfo)) throw new NotCopartitionedException();

    return chain(new KStreamNestedLoop<K, V2, V, V1>(otherImpl.window, processor, partitioningInfo, context));
  }

  public KStream<K, V>[] branch(Predicate... predicates) {
    KStreamBranch branch = new KStreamBranch<K, V>(predicates, partitioningInfo, context);
    registerReceiver(branch);
    return branch.branches;
  }

  public KStream<K, V> through(String topic) {
    process(this.<K, V>getSendProcessor(topic));
    return context.from(topic);
  }

  public void sendTo(String topic) {
    process(this.<K, V>getSendProcessor(topic));
  }

  private <K, V> Processor<K, V> getSendProcessor(final String topic) {
    final RecordCollector collector = context.getRecordCollector();

    return new Processor<K, V>() {
      public void apply(K key, V value) {
        collector.send(new ProducerRecord<K, V>(topic, key, value));
      }
      public void punctuate(long timestamp) {}
      public void flush() {}
    };
  }

  public void process(final Processor<K, V> processor) {
    Receiver<K, V> receiver = new Receiver<K, V>() {
      public void receive(K key, V value, long timestamp) {
        processor.apply(key, value);
      }
      public void punctuate(long timestamp) {
        processor.punctuate(timestamp);
      }
      public void flush() {
        processor.flush();
      }
    };
    registerReceiver(receiver);
  }

  public void punctuate(long timestamp) {
    if (timestamp != punctuatedAt) {
      punctuatedAt = timestamp;

      int numReceivers = nextReceivers.size();
      for (int i = 0; i < numReceivers; i++) {
        nextReceivers.get(i).punctuate(timestamp);
      }
    }
  }

  public void flush() {
    int numReceivers = nextReceivers.size();
    for (int i = 0; i < numReceivers; i++) {
      nextReceivers.get(i).flush();
    }
  }

  void registerReceiver(Receiver<K, V> receiver) {
    nextReceivers.add(receiver);
  }

  protected void forward(K key, V value, long timestamp) {
    int numReceivers = nextReceivers.size();
    for (int i = 0; i < numReceivers; i++) {
      nextReceivers.get(i).receive(key, value, timestamp);
    }
  }

  protected <K1, V1> KStream<K1, V1> chain(KStreamImpl<K1, V1, K, V> kstream) {
    synchronized(this) {
      nextReceivers.add(kstream);
      return kstream;
    }
  }

}
