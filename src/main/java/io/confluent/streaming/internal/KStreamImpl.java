package io.confluent.streaming.internal;

import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamWindowed;
import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.Predicate;
import io.confluent.streaming.Processor;
import io.confluent.streaming.PunctuationScheduler;
import io.confluent.streaming.RecordCollector;
import io.confluent.streaming.SyncGroup;
import io.confluent.streaming.ValueMapper;
import io.confluent.streaming.Window;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;

/**
 * Created by yasuhiro on 6/17/15.
 */
abstract class KStreamImpl<K, V> implements KStream<K, V>, Receiver {

  private final ArrayList<Receiver> nextReceivers = new ArrayList<>(1);
  final PartitioningInfo partitioningInfo;
  final KStreamContext context;

  protected KStreamImpl(PartitioningInfo partitioningInfo, KStreamContext context) {
    this.partitioningInfo = partitioningInfo;
    this.context = context;
  }

  @Override
  public KStreamContext context() {
    return this.context;
  }

  @Override
  public KStream<K, V> filter(Predicate<K, V> predicate) {
    return chain(new KStreamFilter<K, V>(predicate, partitioningInfo, context));
  }

  @Override
  public KStream<K, V> filterOut(final Predicate<K, V> predicate) {
    return filter(new Predicate<K, V>() {
      public boolean apply(K key, V value) {
        return !predicate.apply(key, value);
      }
    });
  }


  @Override
  public <K1, V1> KStream<K1, V1> map(KeyValueMapper<K1, V1, K, V> mapper) {
    return chain(new KStreamMap<K1, V1, K, V>(mapper, partitioningInfo.syncGroup, context));
  }

  @Override
  public <V1> KStream<K, V1> mapValues(ValueMapper<V1, V> mapper) {
    return chain(new KStreamMapValues<K, V1, V>(mapper, partitioningInfo, context));
  }

  @Override
  public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K1, ? extends Iterable<V1>, K, V> mapper) {
    return chain(new KStreamFlatMap<K1, V1, K, V>(mapper, partitioningInfo.syncGroup, context));
  }

  @Override
  public <V1> KStream<K, V1> flatMapValues(ValueMapper<? extends Iterable<V1>, V> mapper) {
    return chain(new KStreamFlatMapValues<K, V1, V>(mapper, partitioningInfo, context));
  }

  @Override
  public KStreamWindowed<K, V> with(Window<K, V> window) {
    return (KStreamWindowed<K, V>)chain(new KStreamWindowedImpl<K, V>(window, partitioningInfo, context));
  }

  @Override
  public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
    KStreamBranch<K, V> branch = new KStreamBranch<K, V>(predicates, partitioningInfo, context);
    registerReceiver(branch);
    return branch.branches;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KStream<K, V> through(String topic) {
    return through(topic, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public KStream<K, V> through(String topic, SyncGroup syncGroup) {
    return through(topic, syncGroup, null, null, null, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K1, V1> KStream<K1, V1> through(String topic, Serializer<?> keySerializer, Serializer<?> valSerializer, Deserializer<?> keyDeserializer, Deserializer<?> valDeserializer) {
    return through(topic, context.syncGroup(context.DEFAULT_SYNCHRONIZATION_GROUP), keySerializer, valSerializer, keyDeserializer, valDeserializer);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K1, V1> KStream<K1, V1> through(String topic, SyncGroup syncGroup, Serializer<?> keySerializer, Serializer<?> valSerializer, Deserializer<?> keyDeserializer, Deserializer<?> valDeserializer) {
    process(this.<K, V>getSendProcessor(topic, keySerializer, valSerializer));
    return (KStream<K1, V1>) context.from(topic, syncGroup, keyDeserializer, valDeserializer);
  }

  @Override
  public void sendTo(String topic) {
    process(this.<K, V>getSendProcessor(topic, null, null));
  }

  @Override
  public void sendTo(String topic, Serializer<?> keySerializer, Serializer<?> valSerializer) { process(this.<K, V>getSendProcessor(topic, keySerializer, valSerializer)); }

  @SuppressWarnings("unchecked")
  private <K, V> Processor<K, V> getSendProcessor(final String topic, Serializer<?> keySerializer, Serializer<?> valSerializer) {
    final RecordCollector<K, V> collector;
    if (keySerializer == null && valSerializer == null)
      collector = (RecordCollector<K, V>) context.recordCollector();
    else
      collector = (RecordCollector<K, V>) new RecordCollectors.SerializingRecordCollector(
          ((KStreamContextImpl)context).simpleRecordCollector(),
          keySerializer == null ? context.keySerializer() : keySerializer,
          valSerializer == null ? context.valueSerializer() : valSerializer);

    return new Processor<K, V>() {
      @Override
      public void apply(K key, V value) {
        collector.send(new ProducerRecord<K, V>(topic, key, value));
      }
      @Override
      public void init(PunctuationScheduler scheduler) {}
      @Override
      public void punctuate(long streamTime) {}
    };
  }

  @Override
  public void process(final Processor<K, V> processor) {
    Receiver receiver = new Receiver() {
      public void receive(Object key, Object value, long timestamp, long streamTime) {
        processor.apply((K)key, (V)value);
      }
    };
    registerReceiver(receiver);

    PunctuationScheduler scheduler = ((StreamSynchronizer)partitioningInfo.syncGroup).getPunctuationScheduler(processor);
    processor.init(scheduler);
  }

  void registerReceiver(Receiver receiver) {
    nextReceivers.add(receiver);
  }

  protected void forward(Object key, Object value, long timestamp, long streamTime) {
    int numReceivers = nextReceivers.size();
    for (int i = 0; i < numReceivers; i++) {
      nextReceivers.get(i).receive(key, value, timestamp, streamTime);
    }
  }

  protected <K1, V1> KStream<K1, V1> chain(KStreamImpl<K1, V1> kstream) {
    synchronized(this) {
      nextReceivers.add(kstream);
      return kstream;
    }
  }

}
