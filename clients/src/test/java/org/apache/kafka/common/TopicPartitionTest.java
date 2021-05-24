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
package org.apache.kafka.common;

import org.apache.kafka.common.utils.Serializer;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * This test ensures TopicPartition class is serializable and is serialization compatible.
 * Note: this ensures that the current code can deserialize data serialized with older versions of the code, but not the reverse.
 * That is, older code won't necessarily be able to deserialize data serialized with newer code.
 */
public class TopicPartitionTest {
    private String topicName = "mytopic";
    private String fileName = "serializedData/topicPartitionSerializedfile";
    private int partNum = 5;

    private void checkValues(TopicPartition deSerTP) {
        //assert deserialized values are same as original
        assertEquals(partNum, deSerTP.partition(), "partition number should be " + partNum + " but got " + deSerTP.partition());
        assertEquals(topicName, deSerTP.topic(), "topic should be " + topicName + " but got " + deSerTP.topic());
    }

    @Test
    public void testSerializationRoundtrip() throws IOException, ClassNotFoundException {
        //assert TopicPartition is serializable and deserialization renders the clone of original properly
        TopicPartition origTp = new TopicPartition(topicName, partNum);
        byte[] byteArray = Serializer.serialize(origTp);

        //deserialize the byteArray and check if the values are same as original
        Object deserializedObject = Serializer.deserialize(byteArray);
        assertTrue(deserializedObject instanceof TopicPartition);
        checkValues((TopicPartition) deserializedObject);
    }

    @Test
    public void testTopiPartitionSerializationCompatibility() throws IOException, ClassNotFoundException {
        // assert serialized TopicPartition object in file (serializedData/topicPartitionSerializedfile) is
        // deserializable into TopicPartition and is compatible
        Object deserializedObject = Serializer.deserialize(fileName);
        assertTrue(deserializedObject instanceof TopicPartition);
        checkValues((TopicPartition) deserializedObject);
    }
}
