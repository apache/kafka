package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Processor<K, V>  {

  void apply(String topic, K key, V value, RecordCollector<K, V> collector, Coordinator coordinator);

  void init(PunctuationScheduler punctuationScheduler);

  void punctuate(long streamTime);

}
