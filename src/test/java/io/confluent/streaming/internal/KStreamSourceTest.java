package io.confluent.streaming.internal;

import static org.junit.Assert.assertEquals;

import io.confluent.streaming.*;

import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

import java.util.Collections;

public class KStreamSourceTest {

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
  public void testKStreamSource() {

    TestProcessor<String, String> processor = new TestProcessor<String, String>();

    KStreamSource<String, String> stream = new KStreamSource<String, String>(streamMetadata, null);
    stream.process(processor);

    final String[] expectedKeys = new String[] { "k1", "k2", "k3" };
    final String[] expectedValues = new String[] { "v1", "v2", "v3" };

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(topicName, expectedKeys[i], expectedValues[i], 0L, 0L);
    }

    assertEquals(3, processor.processed.size());

    for (int i = 0; i < expectedKeys.length; i++) {
      assertEquals(expectedKeys[i] + ":" + expectedValues[i], processor.processed.get(i));
    }
  }

}
