package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface ValueMapper<R, V> {

  R apply(V value);

}
