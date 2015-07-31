package io.confluent.streaming.examples;

import io.confluent.streaming.KafkaStreaming;
import io.confluent.streaming.Processor;
import io.confluent.streaming.SingleProcessorTopology;
import io.confluent.streaming.StreamingConfig;

import java.util.Properties;

/**
 * Created by guozhang on 7/14/15.
 */
public class PrintKStreamJob<K, V> implements Processor<K, V> {

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

  @Override
  public void close() {
    // do nothing
  }

  public static void main(String[] args) {
    KafkaStreaming streaming = new KafkaStreaming(
      new SingleProcessorTopology(PrintKStreamJob.class, args),
      new StreamingConfig(new Properties())
    );
    streaming.run();
  }
}
