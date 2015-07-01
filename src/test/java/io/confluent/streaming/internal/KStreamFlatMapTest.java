package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class KStreamFlatMapTest {

  private Ingestor ingestor = new NoopIngestor();

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

    processor = new TestProcessor<String, String>();
    stream = new KStreamSource<Integer, String>(partitioningInfo, null);
    stream.flatMap(mapper).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(6, processor.processed.size());

    String[] expected = new String[] { "10:V1", "20:V2", "20:V2", "30:V3", "30:V3", "30:V3" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
