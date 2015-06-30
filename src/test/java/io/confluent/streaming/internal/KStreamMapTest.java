package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class KStreamMapTest {

  private Ingestor ingestor = new Ingestor() {
    @Override
    public void poll() {}

    @Override
    public void poll(long timeoutMs) {}

    @Override
    public void pause(TopicPartition partition) {}

    @Override
    public void unpause(TopicPartition partition, long offset) {}
  };

  private StreamSynchronizer<String, String> streamSynchronizer = new StreamSynchronizer<String, String>(
    "group",
    ingestor,
    new ChooserImpl<String, String>(),
    new TimestampExtractor<String, String>() {
      public long extract(String topic, String key, String value) {
        return 0L;
      }
    },
    10
  );

  private PartitioningInfo partitioningInfo = new PartitioningInfo(new SyncGroup("group", streamSynchronizer), 1);

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
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(4, processor.processed.size());

    String[] expected = new String[] { "V0:0", "V1:1", "V2:2", "V3:3" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
