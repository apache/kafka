package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/22/15.
 */
public interface CopartitioningGroupFactory {

  CopartitioningGroup create(String name, int numPartitions);

}
