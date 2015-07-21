package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamFilterTest {

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

  private Predicate<Integer, String> isMultipleOfThree = new Predicate<Integer, String>() {
    @Override
    public boolean apply(Integer key, String value) {
      return (key % 3) == 0;
    }
  };

  @Test
  public void testFilter() {
    final int[] expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

    KStreamSource<Integer, String> stream;
    TestProcessor<Integer, String> processor;

    processor = new TestProcessor<Integer, String>();
    stream = new KStreamSource<Integer, String>(streamMetadata, null);
    stream.filter(isMultipleOfThree).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L, 0L);
    }

    assertEquals(2, processor.processed.size());
  }

  @Test
  public void testFilterOut() {
    final int[] expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

    KStreamSource<Integer, String> stream;
    TestProcessor<Integer, String> processor;

    processor = new TestProcessor<Integer, String>();
    stream = new KStreamSource<Integer, String>(streamMetadata, null);
    stream.filterOut(isMultipleOfThree).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L, 0L);
    }

    assertEquals(5, processor.processed.size());
  }

}
