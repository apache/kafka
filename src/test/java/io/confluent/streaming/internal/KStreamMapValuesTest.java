package io.confluent.streaming.internal;

import io.confluent.streaming.SyncGroup;
import io.confluent.streaming.TimestampExtractor;
import io.confluent.streaming.ValueMapper;
import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamMapValuesTest {

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

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(streamSynchronizer, Collections.singletonMap(topicName, new PartitioningInfo(1)));

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

    KStreamSource<Integer, String> stream;
    TestProcessor<Integer, Integer> processor;

    processor = new TestProcessor<Integer, Integer>();
    stream = new KStreamSource<Integer, String>(streamMetadata, null);
    stream.mapValues(mapper).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(topicName, expectedKeys[i], Integer.toString(expectedKeys[i]), 0L, 0L);
    }

    assertEquals(4, processor.processed.size());

    String[] expected = new String[] { "1:1", "10:2", "100:3", "1000:4" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
