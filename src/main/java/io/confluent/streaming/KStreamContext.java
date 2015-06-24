package io.confluent.streaming;

/**
 * Created by yasuhiro on 6/19/15.
 */
public interface KStreamContext {

  <K, V> KStream<K, V> from(String topic);

  RecordCollector getRecordCollector();

}
