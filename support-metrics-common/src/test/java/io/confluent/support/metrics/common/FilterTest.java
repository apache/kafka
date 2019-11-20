/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package io.confluent.support.metrics.common;

import org.junit.Test;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class FilterTest {

  @Test
  public void doesNotAcceptNullInput() {
    // Given
    Properties nullProperties = null;
    Filter f = new Filter();

    // When/Then
    try {
      f.apply(nullProperties);
      fail("IllegalArgumentException expected because input is null");
    } catch (IllegalArgumentException e) {
      // ignore
    }
  }

  @Test
  public void emptyInputResultsInEmptyOutput() {
    // Given
    Properties emptyProperties = new Properties();
    Filter f = new Filter();

    // When
    Properties filtered = f.apply(emptyProperties);

    // Then
    assertEquals(0, filtered.size());
  }

  @Test
  public void filtersNothingByDefault() {
    // Given
    Properties anyProperties = System.getProperties();
    Filter f = new Filter();

    // When
    Properties filtered = f.apply(anyProperties);

    // Then
    assertEquals(anyProperties, filtered);
    assertEquals(anyProperties.size(), filtered.size());
  }

  @Test
  public void filtersMatchingKey() {
    // Given
    Properties properties = new Properties();
    properties.put("one", 1);
    properties.put("two", 2);
    Set<String> removeOneKey = new HashSet<>();
    removeOneKey.add("one");
    Filter f = new Filter(removeOneKey);

    // When
    Properties filtered = f.apply(properties);

    // Then
    assertEquals(properties.size() - 1, filtered.size());
    assertFalse(filtered.containsKey("one"));
    assertTrue(filtered.containsKey("two"));
    assertEquals(properties.get("two"), filtered.get("two"));
  }

  @Test
  public void filtersMatchingKeys() {
    // Given
    Properties properties = new Properties();
    properties.put("one", 1);
    properties.put("two", 2);
    properties.put("three", 3);
    properties.put("four", 4);
    properties.put("five", 5);
    Set<String> removeAllKeys = new HashSet<>();
    for (Object key : properties.keySet()) {
      removeAllKeys.add(key.toString());
    }
    Filter f = new Filter(removeAllKeys);

    // When
    Properties filtered = f.apply(properties);

    // Then
    assertEquals(0, filtered.size());
  }

  @Test
  public void doesNotFilterMismatchingKeys() {
    // Given
    Properties properties = new Properties();
    properties.put("one", 1);
    properties.put("two", 2);
    Set<String> keysToRemove = new HashSet<>();
    keysToRemove.add("three");
    Filter f = new Filter(keysToRemove);

    // When
    Properties filtered = f.apply(properties);

    // Then
    assertEquals(properties.size(), filtered.size());
    assertTrue(filtered.containsKey("one"));
    assertTrue(filtered.containsKey("two"));
  }

}