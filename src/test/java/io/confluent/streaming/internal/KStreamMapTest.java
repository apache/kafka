package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.testutil.MockKStreamContext;
import io.confluent.streaming.testutil.MockKStreamTopology;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamMapTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  @Test
  public void testMap() {

    KeyValueMapper<String, Integer, Integer, String> mapper =
      new KeyValueMapper<String, Integer, Integer, String>() {
      @Override
      public KeyValue<String, Integer> apply(Integer key, String value) {
        return KeyValue.pair(value, key);
      }
    };

    final int[] expectedKeys = new int[] { 0, 1, 2, 3 };

<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
>>>>>>> new api model
    KStreamSource<Integer, String> stream;
    TestProcessor<String, Integer> processor;

    processor = new TestProcessor<>();
    stream = new KStreamSource<>(null, initializer);
    stream.map(mapper).process(processor);

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(4, processor.processed.size());

    String[] expected = new String[] { "V0:0", "V1:1", "V2:2", "V3:3" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
