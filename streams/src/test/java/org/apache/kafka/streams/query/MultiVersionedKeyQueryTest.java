package org.apache.kafka.streams.query;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import org.junit.Test;

public class MultiVersionedKeyQueryTest {

  @Test
  public void shouldThrowNPEWithNullKey() {
    final Exception exception = assertThrows(NullPointerException.class, () -> MultiVersionedKeyQuery.withKey(null));
    assertEquals("key cannot be null.", exception.getMessage());
  }

}
