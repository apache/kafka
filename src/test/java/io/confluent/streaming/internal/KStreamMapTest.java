package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class KStreamMapTest {

  private Ingestor ingestor = new MockIngestor();

  private StreamSynchronizer streamSynchronizer = new StreamSynchronizer(
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

  private PartitioningInfo partitioningInfo = new PartitioningInfo(streamSynchronizer, 1);

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

    KStreamSource<Integer, String> stream;
    TestProcessor<String, Integer> processor;

    processor = new TestProcessor<String, Integer>();
    stream = new KStreamSource<Integer, String>(partitioningInfo, null);
    stream.map(mapper).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L, 0L);
    }

    assertEquals(4, processor.processed.size());

    String[] expected = new String[] { "V0:0", "V1:1", "V2:2", "V3:3" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
