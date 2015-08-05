package org.apache.kafka.stream.topology;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface ValueMapper<R, V> {

  R apply(V value);

}
