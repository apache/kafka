package org.apache.kafka.stream.examples;

import org.apache.kafka.stream.topology.Processor;
import org.apache.kafka.stream.KStreamContext;
import org.apache.kafka.stream.KafkaStreaming;
import org.apache.kafka.stream.StreamingConfig;
import org.apache.kafka.stream.topology.SingleProcessorTopology;

import java.util.Properties;

/**
 * Created by guozhang on 7/14/15.
 */
public class PrintKStreamJob<K, V> implements Processor<K, V> {

  private KStreamContext context;

  @Override
  public void init(KStreamContext context) {
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

  @Override
  public void close() {
    // do nothing
  }

  public static void main(String[] args) {
<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> fix examples
    KafkaStreaming streaming = new KafkaStreaming(
      new SingleProcessorTopology(PrintKStreamJob.class, args),
      new StreamingConfig(new Properties())
    );
    streaming.run();
<<<<<<< HEAD
=======
    KafkaStreaming kstream = new KafkaStreaming(new PrintKStreamJob(), new StreamingConfig(new Properties()));
=======
    KafkaStreaming kstream = new KafkaStreaming(new PrintKStreamJob(args), new StreamingConfig(new Properties()));
>>>>>>> fix examples
    kstream.run();
>>>>>>> wip
=======
>>>>>>> fix examples
  }
}
