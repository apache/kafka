package io.confluent.streaming.kv.internals;

/**
 * Created by guozhang on 7/27/15.
 */

// TODO: this should be removed once we move to Java 8
public interface RestoreFunc {

  void apply(byte[] key, byte[] value);

  void load();
}
