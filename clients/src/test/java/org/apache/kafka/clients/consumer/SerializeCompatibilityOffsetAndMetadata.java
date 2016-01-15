/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.kafka.clients.consumer;

import org.apache.kafka.common.utils.Serializer;
import org.junit.Test;

import java.io.IOException;


import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * This test case ensures OffsetAndMetadata class is serializable and is serialization compatible.
 * Note: this ensures only serialization compatibility backwards but would not be able to ensure the reverse i.e.
 * deserialize a object of new version of the class into a Old version class will not be ensured.
 */
public class SerializeCompatibilityOffsetAndMetadata extends Serializer {
    private String metadata = "test commit metadata";
    private long    offset   = 10;
    private String fileName  = "serializedData/offsetAndMetadataSerializedfile";

    private void checkValues(OffsetAndMetadata deSerOAM) {
        //assert deserialized values are same as original
        //not using assertEquals for offset to ensure the type casting will catch any change in datatype
        assertTrue("Offset should be " + offset + " and of type long. Got " + deSerOAM.offset(), offset == (long) deSerOAM.offset());
        assertEquals("metadata should be " + metadata + " but got " + deSerOAM.metadata(), metadata, deSerOAM.metadata());
    }

    @Test
    public void testOffsetMetadataSerialization() throws IOException, ClassNotFoundException {
        //assert OffsetAndMetadata is serializable
        OffsetAndMetadata origOAM = new OffsetAndMetadata(offset, metadata);
        byte[] byteArray =  serialize(origOAM);

        //deserialize the byteArray and check if the values are same as original
        Object deserializedObject = deserialize(byteArray);

        assertTrue(deserializedObject instanceof OffsetAndMetadata);

        checkValues((OffsetAndMetadata) deserializedObject);
    }

    @Test
    public void testOffsetMetadataSerializationCompatibility() throws IOException, ClassNotFoundException {
        // assert serialized OffsetAndMetadata object in file (oamserializedfile under resources folder) is
        // deserializable into OffsetAndMetadata and is compatible
        Object deserializedObject = deserialize(fileName);

        assertTrue(deserializedObject instanceof OffsetAndMetadata);

        checkValues((OffsetAndMetadata) deserializedObject);
    }
}
