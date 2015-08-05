package org.apache.kafka.stream;

import org.apache.kafka.stream.internal.PartitioningInfo;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.Transformer;
import org.apache.kafka.stream.topology.internal.KStreamMetadata;
import org.apache.kafka.stream.topology.internal.KStreamSource;
import org.apache.kafka.test.MockKStreamContext;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamTransformTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  @Test
  public void testTransform() {
    final int[] expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

    KStreamTopology topology = new MockKStreamTopology();
    KStreamSource<String, String> stream;
    MockProcessor<Integer, Integer> processor;
    Transformer<Integer, Integer, String, String> transformer;

    processor = new MockProcessor<>();
    transformer = new Transformer<Integer, Integer, String, String>() {
      KStreamContext context;
      Forwarder<Integer, Integer> forwarder;
      public void init(KStreamContext context) {
        this.context = context;
      }
      public void forwarder(Forwarder<Integer, Integer> forwarder) {
        this.forwarder = forwarder;
      }
      public void process(String key, String value) {
        forwarder.send(Integer.parseInt(value), 0, 0L);
      }
      public void punctuate(long timestamp) {}
      public void close() {}
    };

    stream = new KStreamSource<>(null, topology);

    stream.transform(transformer).process(processor);

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(null, Integer.toString(expectedKeys[i]), 0L);
    }

    assertEquals(expectedKeys.length, processor.processed.size());
    for (int i = 0; i < expectedKeys.length; i++) {
      assertEquals(expectedKeys[i]+ ":" + 0, processor.processed.get(i));
    }
  }

  @Test
  public void testTransformEmitOnPuncutation() {
    final int[] expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

    KStreamTopology topology = new MockKStreamTopology();
    KStreamSource<Integer, String> stream;
    MockProcessor<Integer, Integer> processor;
    Transformer<Integer, Integer, Integer, String> transformer;

    processor = new MockProcessor<>();
    transformer = new Transformer<Integer, Integer, Integer, String>() {
      KStreamContext context;
      Forwarder<Integer, Integer> forwarder;
      Integer currentKey;
      public void init(KStreamContext context) {
        this.context = context;
      }
      public void forwarder(Forwarder<Integer, Integer> forwarder) {
        this.forwarder = forwarder;
      }
      public void process(Integer key, String value) {
        currentKey = Integer.parseInt(value);
      }
      public void punctuate(long timestamp) {
        forwarder.send(currentKey, 0, 0L);
      }
      public void close() {}
    };

    stream = new KStreamSource<>(null, topology);

    stream.transform(transformer).process(processor);

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(null, Integer.toString(expectedKeys[i]), 0L);
      if (i % 3 == 2) transformer.punctuate(0L);
    }

    final int[] expected = new int[] { 3, 6 };
    assertEquals(2, processor.processed.size());
    for (int i = 0; i < 2; i++) {
      assertEquals(expected[i] + ":" + 0, processor.processed.get(i));
    }
  }

}
