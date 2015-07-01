package io.confluent.streaming;

import io.confluent.streaming.internal.StreamSynchronizer;

/**
 * SyncGroup represents a group of streams synchronized together.
 */
public class SyncGroup {

  public final String name;
  public final StreamSynchronizer<?, ?> streamSynchronizer;

  public SyncGroup(String name, StreamSynchronizer<?, ?> streamSynchronizer) {
    this.name = name;
    this.streamSynchronizer = streamSynchronizer;
  }

}
