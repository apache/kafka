package org.apache.kafka.stream;

import org.apache.kafka.stream.internal.PartitioningInfo;
import org.apache.kafka.stream.topology.KStreamTopology;
import org.apache.kafka.stream.topology.Predicate;
import org.apache.kafka.stream.topology.internal.KStreamMetadata;
import org.apache.kafka.stream.topology.internal.KStreamSource;
import org.apache.kafka.test.MockKStreamTopology;
import org.apache.kafka.test.MockProcessor;
import org.apache.kafka.test.MockKStreamContext;

import org.junit.Test;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class KStreamFilterTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  private Predicate<Integer, String> isMultipleOfThree = new Predicate<Integer, String>() {
    @Override
    public boolean apply(Integer key, String value) {
      return (key % 3) == 0;
    }
  };

  @Test
  public void testFilter() {
    final int[] expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
>>>>>>> new api model
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
=======
    KStreamTopology initializer = new MockKStreamTopology();
>>>>>>> wip
    KStreamSource<Integer, String> stream;
    MockProcessor<Integer, String> processor;

    processor = new MockProcessor<>();
    stream = new KStreamSource<>(null, initializer);
    stream.filter(isMultipleOfThree).process(processor);

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(2, processor.processed.size());
  }

  @Test
  public void testFilterOut() {
    final int[] expectedKeys = new int[] { 1, 2, 3, 4, 5, 6, 7 };

<<<<<<< HEAD
<<<<<<< HEAD
<<<<<<< HEAD
    KStreamTopology initializer = new MockKStreamTopology();
=======
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null);
>>>>>>> new api model
=======
    KStreamInitializer initializer = new KStreamInitializerImpl();
>>>>>>> wip
=======
    KStreamTopology initializer = new MockKStreamTopology();
>>>>>>> wip
    KStreamSource<Integer, String> stream;
    MockProcessor<Integer, String> processor;

    processor = new MockProcessor<>();
    stream = new KStreamSource<>(null, initializer);
    stream.filterOut(isMultipleOfThree).process(processor);

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L);
    }

    assertEquals(5, processor.processed.size());
  }

}
