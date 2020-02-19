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
package io.confluent.support.metrics.serde;

import org.apache.avro.generic.GenericContainer;
import org.junit.Test;

import java.io.IOException;

import io.confluent.support.metrics.serde.test.User;

import static org.junit.Assert.assertEquals;

public class AvroSerializerTest {
  @Test
  public void testSerializedDataIncludesAvroSchema() throws IOException {
    // Given
    GenericContainer anyValidRecord = new User("anyName");
    AvroDeserializer decoder = new AvroDeserializer();
    AvroSerializer encoder = new AvroSerializer();

    // When
    byte[] serializedRecord = encoder.serialize(anyValidRecord);

    // Then
    GenericContainer[] decodedRecords =
        decoder.deserialize(anyValidRecord.getSchema(), serializedRecord);
    assertEquals(1, decodedRecords.length);
    assertEquals(decodedRecords[0].getSchema(), anyValidRecord.getSchema());
  }
}