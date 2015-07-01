package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import org.junit.Test;

import java.lang.reflect.Array;

import static org.junit.Assert.assertEquals;

public class KStreamBranchTest {

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

  @SuppressWarnings("unchecked")
  @Test
  public void testKStreamBranch() {

    Predicate<Integer, String> isEven = new Predicate<Integer, String>() {
      @Override
      public boolean apply(Integer key, String value) {
        return (key % 2) == 0;
      }
    };
    Predicate<Integer, String> isMultipleOfThree = new Predicate<Integer, String>() {
      @Override
      public boolean apply(Integer key, String value) {
        return (key % 3) == 0;
      }
    };
    Predicate<Integer, String> isOdd = new Predicate<Integer, String>() {
      @Override
      public boolean apply(Integer key, String value) {
        return (key % 2) != 0;
      }
    };

    final int[] expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

    KStreamSource<Integer, String> stream;
    KStream<Integer, String>[] branches;
    TestProcessor<Integer, String>[] processors;

    stream = new KStreamSource<Integer, String>(partitioningInfo, null);
    branches = stream.branch(isEven, isMultipleOfThree, isOdd);

    assertEquals(3, branches.length);

    processors = (TestProcessor<Integer, String>[]) Array.newInstance(TestProcessor.class, branches.length);
    for (int i = 0; i < branches.length; i++) {
      processors[i] = new TestProcessor<Integer, String>();
      branches[i].process(processors[i]);
    }

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(3, processors[0].processed.size());
    assertEquals(1, processors[1].processed.size());
    assertEquals(3, processors[2].processed.size());

    stream = new KStreamSource<Integer, String>(partitioningInfo, null);
    branches = stream.branch(isEven, isOdd, isMultipleOfThree);

    assertEquals(3, branches.length);

    processors = (TestProcessor<Integer, String>[]) Array.newInstance(TestProcessor.class, branches.length);
    for (int i = 0; i < branches.length; i++) {
      processors[i] = new TestProcessor<Integer, String>();
      branches[i].process(processors[i]);
    }

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(3, processors[0].processed.size());
    assertEquals(4, processors[1].processed.size());
    assertEquals(0, processors[2].processed.size());
  }

}
