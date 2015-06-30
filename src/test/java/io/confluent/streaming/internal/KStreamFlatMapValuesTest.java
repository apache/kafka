package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.ArrayList;

import static org.junit.Assert.assertEquals;

public class KStreamFlatMapValuesTest {

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

    ValueMapper<Iterable<String>, String> mapper =
      new ValueMapper<Iterable<String>, String>() {
      @Override
      public Iterable<String> apply(String value) {
        ArrayList<String> result = new ArrayList<String>();
        result.add(value.toLowerCase());
        result.add(value);
        return result;
      }
    };

    final int[] expectedKeys = new int[] { 0, 1, 2, 3 };

    KStreamSource<Integer, String> stream;
    TestProcessor<Integer, String> processor;

    processor = new TestProcessor<Integer, String>();
    stream = new KStreamSource<Integer, String>(partitioningInfo, null);
    stream.flatMapValues(mapper).process(processor);

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(8, processor.processed.size());

    String[] expected = new String[] { "0:v0", "0:V0", "1:v1", "1:V1", "2:v2", "2:V2", "3:v3", "3:V3" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
