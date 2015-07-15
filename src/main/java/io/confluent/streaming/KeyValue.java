package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/17/15.
 */
public class KeyValue<K,V> {

  public final K key;
  public final V value;

  public KeyValue(K key, V value) {
    this.key = key;
    this.value = value;
  }

  public static <K, V> KeyValue<K,V> pair(K key, V value) {
    return new KeyValue<K, V>(key, value);
  }

}
