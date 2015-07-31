package io.confluent.streaming.examples;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KafkaStreaming;
import io.confluent.streaming.Processor;
import io.confluent.streaming.SingleProcessorTopology;
import io.confluent.streaming.StreamingConfig;
import io.confluent.streaming.kv.Entry;
import io.confluent.streaming.kv.InMemoryKeyValueStore;
import io.confluent.streaming.kv.KeyValueIterator;
import io.confluent.streaming.kv.KeyValueStore;

import java.util.Properties;

/**
 * Created by guozhang on 7/27/15.
 */

public class StatefulKStreamJob implements Processor<String, Integer> {

  private KStreamContext context;
  private KeyValueStore<String, Integer> kvStore;

  public StatefulKStreamJob(String... topics) {
    super(topics);
  }

  @Override
  public void init(KStreamContext context) {
    this.context = context;
    this.context.schedule(this, 1000);

<<<<<<< HEAD
    this.kvStore = new InMemoryKeyValueStore<>("local-state", context);
    this.kvStore.restore(); // call restore inside processor.init
=======
    this.kvStore = new InMemoryKeyValueStore<>("local-state", context.kstreamContext());
    this.kvStore.restore(); // call restore inside processor.bind
>>>>>>> new api model
  }

  @Override
  public void process(String key, Integer value) {
    Integer oldValue = this.kvStore.get(key);
    if (oldValue == null) {
      this.kvStore.put(key, value);
    } else {
      int newValue = oldValue + value;
      this.kvStore.put(key, newValue);
    }

    context.commit();
  }

  @Override
  public void punctuate(long streamTime) {
    KeyValueIterator<String, Integer> iter = this.kvStore.all();
    while (iter.hasNext()) {
      Entry<String, Integer> entry = iter.next();
      System.out.println("[" + entry.key() + ", " + entry.value() + "]");
    }
  }

  @Override
  public void close() {
    // do nothing
  }

  public static void main(String[] args) {
<<<<<<< HEAD
<<<<<<< HEAD
    KafkaStreaming streaming = new KafkaStreaming(
      new SingleProcessorTopology(StatefulKStreamJob.class, args),
      new StreamingConfig(new Properties())
    );
    streaming.run();
=======
    KafkaStreaming kstream = new KafkaStreaming(new StatefulKStreamJob(), new StreamingConfig(new Properties()));
=======
    KafkaStreaming kstream = new KafkaStreaming(new StatefulKStreamJob(args), new StreamingConfig(new Properties()));
>>>>>>> fix examples
    kstream.run();
>>>>>>> wip
  }
}
