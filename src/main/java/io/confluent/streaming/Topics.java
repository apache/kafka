package io.confluent.streaming;

import io.confluent.streaming.util.Util;

import java.util.Collections;
import java.util.Set;

/**
 * A class represents a set of topic names.
 */
public class Topics {

  public final Set<String> topics;

  Topics(String... topics) {
    this.topics = Collections.unmodifiableSet(Util.mkSet(topics));
  }

}
