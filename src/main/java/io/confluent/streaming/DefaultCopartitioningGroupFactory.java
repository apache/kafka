package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/22/15.
 */
public class DefaultCopartitioningGroupFactory implements CopartitioningGroupFactory {

  private final TimestampExtractor timestampExtractor;

  public DefaultCopartitioningGroupFactory(TimestampExtractor timestampExtractor) {
    this.timestampExtractor = timestampExtractor;
  }

  public CopartitioningGroup create(String name, int numPartitions) {
    return new CopartitioningGroup(name, numPartitions, new TimeBasedStreamSynchronizer(timestampExtractor));
  }

}
