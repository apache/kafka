package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/22/15.
 */
public final class CopartitioningGroup {

  public final String name;
  public final int numPartitions;

  public CopartitioningGroup(String name, int numPartitions) {
    this.name = name;
    this.numPartitions = numPartitions;
  }

}
