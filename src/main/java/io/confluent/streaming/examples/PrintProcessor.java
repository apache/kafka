package io.confluent.streaming.examples;

import io.confluent.streaming.KafkaStreaming;
import io.confluent.streaming.Processor;
import io.confluent.streaming.ProcessorKStreamJob;
import io.confluent.streaming.PunctuationScheduler;
import io.confluent.streaming.StreamingConfig;

import java.util.Properties;

/**
 * Created by guozhang on 7/14/15.
 */
public class PrintProcessor<K, V> implements Processor<K, V> {

  @Override
  public void apply(String topic, K key, V value) {
    System.out.println(topic + ": [" + key + ", " + value + "]");
  }

  @Override
  public void init(PunctuationScheduler punctuationScheduler) {
    // do nothing
  }

  @Override
  public void punctuate(long streamTime) {
    // do nothing
  }

  public static void main(String[] args) {
    // put the Processor class into the context configs
    StreamingConfig configs = new StreamingConfig(new Properties());
    configs.addContextObject(ProcessorKStreamJob.PROCESSOR_CLASSNAME, PrintProcessor.class);

    KafkaStreaming kstream = new KafkaStreaming(ProcessorKStreamJob.class, configs);
    kstream.run();
  }
}
