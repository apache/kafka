package io.confluent.streaming.internal;

import io.confluent.streaming.StreamSynchronizer;
import io.confluent.streaming.SyncGroup;
import io.confluent.streaming.TimestampExtractor;
import io.confluent.streaming.ValueMapper;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class KStreamMapValuesTest {

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
    stream = new KStreamSource<Integer, String>(partitioningInfo, null);
    stream.mapValues(mapper).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], Integer.toString(expectedKeys[i]), 0L);
    }

    assertEquals(4, processor.processed.size());

    String[] expected = new String[] { "1:1", "10:2", "100:3", "1000:4" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
