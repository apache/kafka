package io.confluent.streaming.examples;

import io.confluent.streaming.KafkaStreaming;
import io.confluent.streaming.Processor;
import io.confluent.streaming.ProcessorKStreamJob;
import io.confluent.streaming.StreamingConfig;

import java.util.Properties;

/**
 * Created by guozhang on 7/14/15.
 */
public class PrintProcessor<K, V> implements Processor<K, V> {

  private ProcessorContext context;

  @Override
  public void init(ProcessorContext context) {
    this.context = context;
  }

  @Override
  public void process(K key, V value) {
    System.out.println("[" + key + ", " + value + "]");

    context.commit();

    context.send("topic", key, value);
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

    /*
     * another possible way is to relax the KafkaStreaming job class type, from
     *

     KafkaStreaming(Class<? extend KStreamJob> job ..)

     to

     KafkaStreaming(Class<?> job ..)

     so that we can skip the KStreamJob wrapper around the processor:

     KafkaStreaming kstream = new KafkaStreaming(PrintProcessor.class, configs);

     */
  }
}
