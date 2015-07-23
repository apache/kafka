package io.confluent.streaming.internal;

import io.confluent.streaming.KStreamContext;
import io.confluent.streaming.TimestampExtractor;
import io.confluent.streaming.ValueMapper;
import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.MockKStreamContext;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamMapValuesTest {

  private Ingestor ingestor = new MockIngestor();

  private StreamGroup streamGroup = new StreamGroup(
    "group",
    ingestor,
    new TimeBasedChooser(),
    new TimestampExtractor() {
      public long extract(String topic, Object key, Object value) {
        return 0L;
      }
    },
    10
  );

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(streamGroup, Collections.singletonMap(topicName, new PartitioningInfo(1)));

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

    KStreamContext context = new MockKStreamContext(null, null);
    KStreamSource<Integer, String> stream;
    TestProcessor<Integer, Integer> processor;

    processor = new TestProcessor<>();
    stream = new KStreamSource<>(streamMetadata, context, null, null);
    stream.mapValues(mapper).process(processor);

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
