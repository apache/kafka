package io.confluent.streaming.internal;

/**
 * Created by yasuhiro on 6/25/15.
 */
public interface Chooser {

  void add(RecordQueue queue);

  RecordQueue next();

  void close();

}
