package io.confluent.streaming.examples;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamJob;
import io.confluent.streaming.KafkaStreaming;
import io.confluent.streaming.KeyValue;
import io.confluent.streaming.KeyValueMapper;
import io.confluent.streaming.StreamingConfig;

import java.util.Properties;

/**
 * Created by guozhang on 7/14/15.
 */
public class MapKStreamJob implements KStreamJob {

  public String topic = "bla";

  @SuppressWarnings("unchecked")
  @Override
  public void init(KStreamContext context) {
    context.from(topic)
        .map(new KeyValueMapper() {
          @Override
          public KeyValue<Object, Integer> apply(Object key, Object value) {
            return new KeyValue<Object, Integer>(key, new Integer((String)value));
          }
        })
        .sendTo("bla-bla");
  }

  @Override
  public void close() {
    // do nothing
  }

  public static void main(String[] args) {
    KafkaStreaming kstream = new KafkaStreaming(MapKStreamJob.class, new StreamingConfig(new Properties()));
    kstream.run();
  }
}
