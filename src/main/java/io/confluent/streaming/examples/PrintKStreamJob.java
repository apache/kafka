package io.confluent.streaming.examples;

import io.confluent.streaming.KafkaStreaming;
import io.confluent.streaming.ProcessorKStreamJob;
import io.confluent.streaming.StreamingConfig;

import java.util.Properties;

/**
 * Created by guozhang on 7/14/15.
 */
public class PrintKStreamJob<K, V> extends ProcessorKStreamJob<K, V> {

  private ProcessorContext processorContext;

  @Override
  public void init(ProcessorContext context) {
    this.processorContext = context;
  }

  @Override
  public void process(K key, V value) {
    System.out.println("[" + key + ", " + value + "]");

    processorContext.commit();

    processorContext.send("topic", key, value);
  }

  @Override
  public void punctuate(long streamTime) {
    // do nothing
  }

  public static void main(String[] args) {
    KafkaStreaming kstream = new KafkaStreaming(PrintKStreamJob.class, new StreamingConfig(new Properties()));
    kstream.run();
  }
}
