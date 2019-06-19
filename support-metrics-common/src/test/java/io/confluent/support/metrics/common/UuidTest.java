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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class UuidTest {
  @Test
  public void generatesValidType4UUID() {
    // Given
    Uuid uuid = new Uuid();

    // When
    String generatedUUID = uuid.getUUID();

    // Then
    assertTrue(generatedUUID.matches("[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}"));
  }

  @Test
  public void stringRepresentationIsIdenticalToGeneratedUUID() {
    // Given
    Uuid uuid = new Uuid();

    // When/Then
    assertEquals(uuid.getUUID(), uuid.toString());
  }

  @Test
  public void uuidDoesNotChangeBetweenRuns() {
    // Given
    Uuid uuid = new Uuid();

    // When
    String firstUuid = uuid.getUUID();
    String secondUuid = uuid.getUUID();

    // Then
    assertEquals(secondUuid, firstUuid);
  }
}