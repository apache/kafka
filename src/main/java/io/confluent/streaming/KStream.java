package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface KStream<K, V> {

  KStream<K, V> filter(Predicate<K, V> predicate);

  KStream<K, V> filterOut(Predicate<K, V> predicate);

  <K1, V1> KStream<K1, V1> map(KeyValueMapper<K1, V1, K, V> mapper);

  <V1> KStream<K, V1> mapValues(ValueMapper<V1, V> mapper);

  <K1, V1> KStream<K1, V1> flatMap(KeyValueMapper<K1, ? extends Iterable<V1>, K, V> mapper);

  <V1> KStream<K, V1> flatMapValues(ValueMapper<? extends Iterable<V1>, V> processor);

  KStreamWindowed<K, V> with(Window<K, V> window);

  <V1, V2> KStream<K, V2> nestedLoop(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor)
    throws NotCopartitionedException;

  KStream<K, V>[] branch(Predicate<K, V>... predicates);

  KStream<K, V> through(String topic);

  void sendTo(String topic);

  void process(Processor<K, V> processor);

}
