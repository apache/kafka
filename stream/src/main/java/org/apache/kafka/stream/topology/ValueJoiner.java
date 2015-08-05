package org.apache.kafka.stream.topology;

/**
 * Created by yasuhiro on 6/17/15.
 */
public interface ValueJoiner<R, V1, V2> {

  R apply(V1 value1, V2 value2);

}
