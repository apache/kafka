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

public class SerializeCompatibilityOffsetAndMetadata extends Serializer {
    public static String metadata = "test commit metadata";
    public static long    offset   = 10;
    public static String fileName  = "serializedData/offsetAndMetadataSerializedfile";

    @Test
    public void testOffsetMetadataSerialization() throws IOException, ClassNotFoundException {
        //assert OffsetAndMetadata is serializable
        OffsetAndMetadata origOAM = new OffsetAndMetadata(offset, metadata);

        serialize(origOAM);


        // assert serialized OffsetAndMetadata object in file (oamserializedfile under resources folder) is
        // de-serializable into OffsetAndMetadata and compatible
        Object deserializedObject = deSerialize(fileName);

        assertTrue(deserializedObject instanceof OffsetAndMetadata);

        if (deserializedObject instanceof OffsetAndMetadata) {
            OffsetAndMetadata deSerOAM = (OffsetAndMetadata) deserializedObject;
            //assert metadata is of type String
            assertTrue(deSerOAM.metadata() instanceof String);

            //assert de-serialized values are same as original
            //not using assertEquals for offset to ensure the type casting will catch any change in datatype
            assertTrue("Offset should be " + offset + " and of type long. Got " + deSerOAM.offset(), offset == (long) deSerOAM.offset());
            assertEquals("metadata should be " + metadata + " but got " + deSerOAM.metadata(), metadata, deSerOAM.metadata());
        }
    }
}
