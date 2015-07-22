package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.testutil.MockIngestor;
import io.confluent.streaming.testutil.UnlimitedWindow;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class KStreamWindowedTest {

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
  public void testWindowedStream() {

    final int[] expectedKeys = new int[] { 0, 1, 2, 3 };

    KStreamSource<Integer, String> stream;
    Window<Integer, String> window;
    String[] expected;

    window = new UnlimitedWindow<>();
    stream = new KStreamSource<>(streamMetadata, null, null, null);
    stream.with(window);

    boolean exceptionRaised = false;

    // two items in the window

    for (int i = 0; i < 2; i++) {
      stream.receive(expectedKeys[i], "V" + expectedKeys[i], 0L, 0L);
    }

    assertEquals(1, countItem(window.find(0, 0L)));
    assertEquals(1, countItem(window.find(1, 0L)));
    assertEquals(0, countItem(window.find(2, 0L)));
    assertEquals(0, countItem(window.find(3, 0L)));

    // previous two items + all items, thus two are duplicates, in the window

    for (int i = 0; i < expectedKeys.length; i++) {
      stream.receive(expectedKeys[i], "Y" + expectedKeys[i], 0L, 0L);
    }

    assertEquals(2, countItem(window.find(0, 0L)));
    assertEquals(2, countItem(window.find(1, 0L)));
    assertEquals(1, countItem(window.find(2, 0L)));
    assertEquals(1, countItem(window.find(3, 0L)));
  }


  private <T> int countItem(Iterator<T> iter) {
    int i = 0;
    while (iter.hasNext()) {
      i++;
      iter.next();
    }
    return i;
  }
}
