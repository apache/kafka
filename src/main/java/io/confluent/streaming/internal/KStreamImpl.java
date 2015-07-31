package io.confluent.streaming.internal;

import io.confluent.streaming.KStream;
import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamInitializer;
import io.confluent.streaming.KStreamWindowed;
import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.Predicate;
import io.confluent.streaming.Processor;
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
  protected KStreamMetadata metadata;
  protected KStreamInitializer initializer;

  protected KStreamImpl(KStreamInitializer initializer) {
    this.initializer = initializer;
  }

  @Override
  public void bind(KStreamContext context, KStreamMetadata metadata) {
    this.metadata = metadata;
    int numReceivers = nextReceivers.size();
    for (int i = 0; i < numReceivers; i++) {
      nextReceivers.get(i).bind(context, metadata);
    }
  }

  @Override
  public KStream<K, V> filter(Predicate<K, V> predicate) {
    return chain(new KStreamFilter<K, V>(predicate, initializer));
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
    return chain(new KStreamMap<K1, V1, K, V>(mapper, initializer));
  }

  @Override
  public <V1> KStream<K, V1> mapValues(ValueMapper<V1, V> mapper) {
    return chain(new KStreamMapValues<K, V1, V>(mapper, initializer));
  }

  @Override
  public <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K1, ? extends Iterable<V1>, K, V> mapper) {
    return chain(new KStreamFlatMap<K1, V1, K, V>(mapper, initializer));
  }

  @Override
  public <V1> KStream<K, V1> flatMapValues(ValueMapper<? extends Iterable<V1>, V> mapper) {
    return chain(new KStreamFlatMapValues<K, V1, V>(mapper, initializer));
  }

  @Override
  public KStreamWindowed<K, V> with(Window<K, V> window) {
    return (KStreamWindowed<K, V>)chain(new KStreamWindowedImpl<>(window, initializer));
  }

  @Override
  public KStream<K, V>[] branch(Predicate<K, V>... predicates) {
    KStreamBranch<K, V> branch = new KStreamBranch<>(predicates, initializer);
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
    return initializer.from(keyDeserializer, valDeserializer, topic);
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
      private ProcessorContext processorContext;

      @Override
      public void init(ProcessorContext processorContext) {
        this.processorContext = processorContext;
      }
      @Override
      public void process(K key, V value) {
        this.processorContext.send(sendTopic, key, value,
            (Serializer<Object>) keySerializer,
            (Serializer<Object>) valSerializer);
      }
      @Override
      public void punctuate(long streamTime) {}
    };
  }

  @SuppressWarnings("unchecked")
  @Override
  public void process(final Processor<K, V> processor) {
    Receiver receiver = new Receiver() {
      public void bind(KStreamContext context, KStreamMetadata metadata) {
        processor.init(new ProcessorContextImpl(context, context.getPunctuationScheduler(processor)));
      }
      public void receive(Object key, Object value, long timestamp, long streamTime) {
        processor.process((K) key, (V) value);
      }
    };
    registerReceiver(receiver);
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
