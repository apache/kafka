package io.confluent.streaming;

import io.confluent.streaming.internal.StreamSynchronizer;

/**
 * SyncGroup represents a group of streams synchronized together.
 */
public interface SyncGroup {

  String name();

}
