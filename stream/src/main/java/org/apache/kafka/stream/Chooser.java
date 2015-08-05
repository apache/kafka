package org.apache.kafka.stream;

import org.apache.kafka.stream.internal.RecordQueue;

/**
 * Created by yasuhiro on 6/25/15.
 */
public interface Chooser {

  void add(RecordQueue queue);

  RecordQueue next();

  void close();

}
