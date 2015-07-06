package io.confluent.streaming.internal;

import io.confluent.streaming.*;

/**
 * Created by yasuhiro on 6/18/15.
 */
public class KStreamWindowedImpl<K, V> extends KStreamImpl<K, V, K, V> implements KStreamWindowed<K, V> {

  final Window<K, V> window;

  KStreamWindowedImpl(Window<K, V> window, PartitioningInfo partitioningInfo, KStreamContextImpl context) {
    super(partitioningInfo, context);
    this.window = window;
  }

  @Override
  public void receive(K key, V value, long timestamp, long streamTime) {
    synchronized(this) {
      window.put(key, value, timestamp);
      forward(key, value, timestamp, streamTime);
    }
  }

  @Override
  public <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor) {
    return join(other, false, processor);
  }

  @Override
  public <V1, V2> KStream<K, V2> joinPrior(KStreamWindowed<K, V1> other, ValueJoiner<V2, V, V1> processor) {
    return join(other, true, processor);
  }

  private <V1, V2> KStream<K, V2> join(KStreamWindowed<K, V1> other, boolean prior, ValueJoiner<V2, V, V1> processor) {

    KStreamWindowedImpl<K, V1> otherImpl = (KStreamWindowedImpl<K, V1>) other;

    if (!partitioningInfo.isJoinCompatibleWith(otherImpl.partitioningInfo)) throw new NotCopartitionedException();

    KStreamJoin<K, V2, V, V1> stream =
      new KStreamJoin<K, V2, V, V1>(this.window, otherImpl.window, prior, processor, partitioningInfo, context);
    otherImpl.registerReceiver(stream.receiverForOtherStream);

    return chain(stream);
  }

}
