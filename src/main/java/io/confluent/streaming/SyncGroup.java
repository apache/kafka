package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/26/15.
 */
public class SyncGroup {

  public final String name;
  public final StreamSynchronizer<?, ?> streamSynchronizer;

  public SyncGroup(String name, StreamSynchronizer streamSynchronizer) {
    this.name = name;
    this.streamSynchronizer = streamSynchronizer;
  }

}
