package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface Processor<K,V>  {

  void apply(K key, V value);

  void init(PunctuationScheduler punctuationScheduler);

  void punctuate(long streamTime);

}
