package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/22/15.
 */
public final class CopartitioningGroup {

  public final String name;
  public final int numPartitions;

  public final StreamSynchronizer streamSynchronizer;

  public CopartitioningGroup(String name, int numPartitions, StreamSynchronizer streamSynchronizer) {
    this.name = name;
    this.numPartitions = numPartitions;
    this.streamSynchronizer = streamSynchronizer;
  }

}
