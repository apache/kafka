package org.apache.kafka.stream;

import static org.junit.Assert.assertEquals;

import io.confluent.streaming.testutil.MockKStreamContext;
import io.confluent.streaming.testutil.MockKStreamTopology;
import io.confluent.streaming.testutil.TestProcessor;
import org.apache.kafka.clients.processor.KStreamContext;
import org.junit.Test;

import java.util.Collections;

public class KStreamSourceTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  @Test
  public void testKStreamSource() {

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
    TestProcessor<String, String> processor = new TestProcessor<>();

    KStreamSource<String, String> stream = new KStreamSource<>(null, initializer);
    stream.process(processor);

    final String[] expectedKeys = new String[] { "k1", "k2", "k3" };
    final String[] expectedValues = new String[] { "v1", "v2", "v3" };

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);
    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], expectedValues[i], 0L);
    }

    assertEquals(3, processor.processed.size());

    for (int i = 0; i < expectedKeys.length; i++) {
      assertEquals(expectedKeys[i] + ":" + expectedValues[i], processor.processed.get(i));
    }
  }

}
