package org.apache.kafka.clients;

import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class InFlightRequestsTest {

  private InFlightRequests inFlightRequests;
  @Before
  public void setup() {
    inFlightRequests = new InFlightRequests(12);
    NetworkClient.InFlightRequest ifr =
        new NetworkClient.InFlightRequest(null, 0, "dest", null, false, false, null, null, 0);
    inFlightRequests.add(ifr);
  }

  @Test
  public void checkIncrementAndDecrementOnLastSent() {
    assertEquals(1, inFlightRequests.count());

    inFlightRequests.completeLastSent("dest");
    assertEquals(0, inFlightRequests.count());
  }

  @Test
  public void checkDecrementOnClear() {
    inFlightRequests.clearAll("dest");
    assertEquals(0, inFlightRequests.count());
  }

  @Test
  public void checkDecrementOnCompleteNext() {
    inFlightRequests.completeNext("dest");
    assertEquals(0, inFlightRequests.count());
  }

  @Test(expected = IllegalStateException.class)
  public void throwExceptionOnNeverBeforeSeenNode() {
    inFlightRequests.completeNext("not-added");
  }
}
