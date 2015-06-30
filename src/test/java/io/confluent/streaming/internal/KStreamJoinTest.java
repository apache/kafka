package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class KStreamJoinTest {

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
  public void testJoin() {

    ValueJoiner<String, String, String> joiner = new ValueJoiner<String, String, String>() {
      @Override
      public String apply(String value1, String value2) {
        return value1 + "+" + value2;
      }
    };

    final int[] expectedKeys = new int[] { 0, 1, 2, 3 };

    KStreamSource<Integer, String> stream1;
    KStreamSource<Integer, String> stream2;
    KStreamWindowed<Integer, String> windowed1;
    KStreamWindowed<Integer, String> windowed2;
    TestProcessor<Integer, String> processor;
    String[] expected;

    processor = new TestProcessor<Integer, String>();
    stream1 = new KStreamSource<Integer, String>(partitioningInfo, null);
    stream2 = new KStreamSource<Integer, String>(partitioningInfo, null);
    windowed1 = stream1.with(new UnlimitedWindow<Integer, String>());
    windowed2 = stream2.with(new UnlimitedWindow<Integer, String>());

    boolean exceptionRaised = false;

    try {
      windowed1.join(windowed2, joiner).process(processor);
    }
    catch (NotCopartitionedException e) {
      exceptionRaised = true;
    }

    assertFalse(exceptionRaised);

    // push two items to the main stream. the other stream's window is empty

    for (int i = 0; i < 2; i++) {
      stream1.receive(expectedKeys[i], "X" + expectedKeys[i], 0L);
    }

    assertEquals(0, processor.processed.size());

    // push two items to the other stream. the main stream's window has two items

    for (int i = 0; i < 2; i++) {
      stream2.receive(expectedKeys[i], "Y" + expectedKeys[i], 0L);
    }

    assertEquals(2, processor.processed.size());

    expected = new String[] { "0:X0+Y0", "1:X1+Y1" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }

    processor.processed.clear();

    // push all items to the main stream. this should produce two items.

    for (int i = 0; i < expectedKeys.length; i++) {
      stream1.receive(expectedKeys[i], "X" + expectedKeys[i], 0L);
    }

    assertEquals(2, processor.processed.size());

    expected = new String[] { "0:X0+Y0", "1:X1+Y1" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }

    processor.processed.clear();

    // there will be previous two items + all items in the main stream's window, thus two are duplicates.

    // push all items to the other stream. this should produce 6 items
    for (int i = 0; i < expectedKeys.length; i++) {
      stream2.receive(expectedKeys[i], "Y" + expectedKeys[i], 0L);
    }

    assertEquals(6, processor.processed.size());

    expected = new String[] { "0:X0+Y0", "0:X0+Y0", "1:X1+Y1", "1:X1+Y1", "2:X2+Y2", "3:X3+Y3" };

    for (int i = 0; i < expected.length; i++) {
      assertEquals(expected[i], processor.processed.get(i));
    }
  }

}
