package io.confluent.streaming.internal;

/**
 * Created by yasuhiro on 6/25/15.
 */
public interface Chooser<K, V> {

  void add(RecordQueue<K, V> queue);

  RecordQueue<K, V> next();

  void close();

}
