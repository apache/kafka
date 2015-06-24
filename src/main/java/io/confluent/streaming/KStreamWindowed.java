package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/18/15.
 */
public interface KStreamWindowed<K, V> extends KStream<K, V> {

  <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor)
    throws NotCopartitionedException;

}
