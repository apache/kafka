package io.confluent.streaming.internal;

import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamTopology;
import io.confluent.streaming.KStreamWindowed;
import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.Predicate;
import io.confluent.streaming.Processor;
import io.confluent.streaming.Transformer;
import io.confluent.streaming.ValueMapper;
import io.confluent.streaming.Window;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;

import java.util.ArrayList;

/**
 * Created by yasuhiro on 6/17/15.
 */
abstract class KStreamImpl<K, V> implements KStream<K, V>, Receiver {

  private final ArrayList<Receiver> nextReceivers = new ArrayList<>(1);
  protected KStreamTopology topology;
  protected KStreamContext context;
  protected KStreamMetadata metadata;

  protected KStreamImpl(KStreamTopology topology) {
    this.topology = topology;
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    if (this.context != null) throw new IllegalStateException("kstream topology is already bound");
    this.context = context;
    this.metadata = metadata;
    int numReceivers = nextReceivers.size();
    for (int i = 0; i < numReceivers; i++) {
      nextReceivers.get(i).bind(context, metadata);
    }
  }

  @Override
  public void close() {
    int numReceivers = nextReceivers.size();
    for (int i = 0; i < numReceivers; i++) {
      nextReceivers.get(i).close();
    }
  }

  @Override
  public KStream<K, V> filter(Predicate<K, V> predicate) {
    return chain(new KStreamFilter<K, V>(predicate, topology));
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
    return chain(new KStreamMap<K1, V1, K, V>(mapper, topology));
  }

  @Override
  public <V1> KStream<K, V1> mapValues(ValueMapper<V1, V> mapper) {
    return chain(new KStreamMapValues<K, V1, V>(mapper, topology));
  }

  @Override
  public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K1, ? extends Iterable<V1>, K, V> mapper) {
    return chain(new KStreamFlatMap<K1, V1, K, V>(mapper, topology));
  }

  @Override
  public <V1> KStream<K, V1> flatMapValues(ValueMapper<? extends Iterable<V1>, V> mapper) {
    return chain(new KStreamFlatMapValues<K, V1, V>(mapper, topology));
  }

  @Override
  public KStreamWindowed<K, V> with(Window<K, V> window) {
    return (KStreamWindowed<K, V>)chain(new KStreamWindowedImpl<>(window, topology));
  }

  @Override
  public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
    KStreamBranch<K, V> branch = new KStreamBranch<>(predicates, topology);
    registerReceiver(branch);
    return branch.branches;
  }

  @SuppressWarnings("unchecked")
  @Override
  public KStream<K, V> through(String topic) {
    return through(topic, null, null, null, null);
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K1, V1> KStream<K1, V1> through(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer, Deserializer<K1> keyDeserializer, Deserializer<V1> valDeserializer) {
    process(this.getSendProcessor(topic, keySerializer, valSerializer));
    return topology.from(keyDeserializer, valDeserializer, topic);
  }

  @Override
  public void sendTo(String topic) {
    process(this.<K, V>getSendProcessor(topic, null, null));
  }

  @Override
  public void sendTo(String topic, Serializer<K> keySerializer, Serializer<V> valSerializer) {
    process(this.getSendProcessor(topic, keySerializer, valSerializer));
  }

  @SuppressWarnings("unchecked")
  private <K, V> Processor<K, V> getSendProcessor(final String sendTopic, final Serializer<K> keySerializer, final Serializer<V> valSerializer) {
    return new Processor<K, V>() {
      private KStreamContext context;

      @Override
      public void init(KStreamContext context) {
        this.context = context;
      }
      @Override
      public void process(K key, V value) {
        this.context.send(sendTopic, key, value, (Serializer<Object>) keySerializer, (Serializer<Object>) valSerializer);
      }
      @Override
      public void punctuate(long streamTime) {}
      @Override
      public void close() {}
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(Processor<K, V> processor) {
    registerReceiver(new ProcessorNode<>(processor));
  }

  @SuppressWarnings("unchecked")
  @Override
  public <K1, V1> KStream<K1, V1> transform(Transformer<K1, V1, K, V> transformer) {
    return chain(new KStreamTransform<>(transformer, topology));
  }

  void registerReceiver(Receiver receiver) {
    nextReceivers.add(receiver);
  }

  protected void forward(Object key, Object value, long timestamp) {
    int numReceivers = nextReceivers.size();
    for (int i = 0; i < numReceivers; i++) {
      nextReceivers.get(i).receive(key, value, timestamp);
    }
  }

  protected <K1, V1> KStream<K1, V1> chain(KStreamImpl<K1, V1> kstream) {
    synchronized(this) {
      nextReceivers.add(kstream);
      return kstream;
    }
  }

}
