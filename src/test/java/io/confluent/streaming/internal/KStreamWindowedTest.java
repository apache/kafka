package io.confluent.streaming.internal;

import io.confluent.streaming.*;
import io.confluent.streaming.testutil.MockKStreamContext;
import io.confluent.streaming.testutil.UnlimitedWindow;
import org.junit.Test;

import java.util.Collections;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;

public class KStreamWindowedTest {

  private String topicName = "topic";

  private KStreamMetadata streamMetadata = new KStreamMetadata(Collections.singletonMap(topicName, new PartitioningInfo(1)));

  @Test
  public void testWindowedStream() {

    final int[] expectedKeys = new int[] { 0, 1, 2, 3 };

    KStreamSource<Integer, String> stream;
    Window<Integer, String> window;
    KStreamInitializer initializer = new KStreamInitializerImpl(null, null, null, null) {
    };

    window = new UnlimitedWindow<>();
    stream = new KStreamSource<>(null, initializer);
    stream.with(window);

    boolean exceptionRaised = false;

    KStreamContext context = new MockKStreamContext(null, null);
    stream.bind(context, streamMetadata);

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
