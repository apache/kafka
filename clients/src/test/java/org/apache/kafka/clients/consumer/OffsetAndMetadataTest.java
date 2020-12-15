/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.utils.Serializer;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

import static org.junit.Assert.assertEquals;

/**
 * This test case ensures OffsetAndMetadata class is serializable and is serialization compatible.
 * Note: this ensures that the current code can deserialize data serialized with older versions of the code, but not the reverse.
 * That is, older code won't necessarily be able to deserialize data serialized with newer code.
 */
public class OffsetAndMetadataTest {

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidNegativeOffset() {
        new OffsetAndMetadata(-239L, Optional.of(15), "");
    }

    @Test
    public void testSerializationRoundtrip() throws IOException, ClassNotFoundException {
        checkSerde(new OffsetAndMetadata(239L, Optional.of(15), "blah"));
        checkSerde(new OffsetAndMetadata(239L, "blah"));
        checkSerde(new OffsetAndMetadata(239L));
    }

    private void checkSerde(OffsetAndMetadata offsetAndMetadata) throws IOException, ClassNotFoundException {
        byte[] bytes =  Serializer.serialize(offsetAndMetadata);
        OffsetAndMetadata deserialized = (OffsetAndMetadata) Serializer.deserialize(bytes);
        assertEquals(offsetAndMetadata, deserialized);
    }

    @Test
    public void testDeserializationCompatibilityBeforeLeaderEpoch() throws IOException, ClassNotFoundException {
        String fileName = "serializedData/offsetAndMetadataBeforeLeaderEpoch";
        Object deserializedObject = Serializer.deserialize(fileName);
        assertEquals(new OffsetAndMetadata(10, "test commit metadata"), deserializedObject);
    }

    @Test
    public void testDeserializationCompatibilityWithLeaderEpoch() throws IOException, ClassNotFoundException {
        String fileName = "serializedData/offsetAndMetadataWithLeaderEpoch";
        Object deserializedObject = Serializer.deserialize(fileName);
        assertEquals(new OffsetAndMetadata(10, Optional.of(235), "test commit metadata"), deserializedObject);
    }

}
