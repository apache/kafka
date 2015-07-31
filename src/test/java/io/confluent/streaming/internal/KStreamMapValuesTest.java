package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.KStreamInitializer;
import io.confluent.streaming.ValueMapper;
import io.confluent.streaming.testutil.MockKStreamContext;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamMapValuesTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  @Test
  public void testFlatMapValues() {

    ValueMapper<Integer, String> mapper =
      new ValueMapper<Integer, String>() {
      @Override
      public Integer apply(String value) {
        return value.length();
      }
    };

    final int[] expectedKeys = new int[] { 1, 10, 100, 1000 };

    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
    KStreamSource<Integer, String> stream;
    TestProcessor<Integer, Integer> processor;

    processor = new TestProcessor<>();
    stream = new KStreamSource<>(null, initializer);
    stream.mapValues(mapper).process(processor);

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], Integer.toString(expectedKeys[i]), 0L, 0L);
    }

    assertEquals(4, processor.processed.size());

    String[] expected = new String[] { "1:1", "10:2", "100:3", "1000:4" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
