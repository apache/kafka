package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamFilterTest {

  private Ingestor ingestor = new MockIngestor();

  private StreamSynchronizer<String, String> streamSynchronizer = new StreamSynchronizer<String, String>(
    "group",
    ingestor,
    new TimeBasedChooser<String, String>(),
    new TimestampExtractor<String, String>() {
      public long extract(String topic, String key, String value) {
        return 0L;
      }
    },
    10
  );

  private PartitioningInfo partitioningInfo = new PartitioningInfo(new SyncGroup("group", streamSynchronizer), 1);

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
    stream = new KStreamSource<Integer, String>(partitioningInfo, null);
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
    stream = new KStreamSource<Integer, String>(partitioningInfo, null);
    stream.filterOut(isMultipleOfThree).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L, 0L);
    }

    assertEquals(5, processor.processed.size());
  }

}
