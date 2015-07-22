package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamFlatMapTest {

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
  public void testFlatMap() {

    KeyValueMapper<String, Iterable<String>, Integer, String> mapper =
      new KeyValueMapper<String, Iterable<String>, Integer, String>() {
      @Override
      public KeyValue<String, Iterable<String>> apply(Integer key, String value) {
        ArrayList<String> result = new ArrayList<String>();
        for (int i = 0; i < key; i++) {
          result.add(value);
        }
        return KeyValue.pair(Integer.toString(key * 10), (Iterable<String>)result);
      }
    };

    final int[] expectedKeys = new int[] { 0, 1, 2, 3 };

    KStreamSource<Integer, String> stream;
    TestProcessor<String, String> processor;

    processor = new TestProcessor<>();
    stream = new KStreamSource<>(streamMetadata, null, null, null);
    stream.flatMap(mapper).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L, 0L);
    }

    assertEquals(6, processor.processed.size());

    String[] expected = new String[] { "10:V1", "20:V2", "20:V2", "30:V3", "30:V3", "30:V3" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
