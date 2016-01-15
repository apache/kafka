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

package org.apache.kafka.common;

import org.apache.kafka.common.utils.Serializer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;

/**
 * This test ensures TopicPartition class is serializable and is serialization compatible.
 * Note: this ensures only serialization compatibility backwards but would not be able to ensure the reverse i.e.
 * deserialize a object of new version of the class into a Old version class will not be ensured.
 */
public class SerializeCompatibilityTopicPartition extends Serializer {

    private String topicName = "mytopic";
    private int    partNum   = 5;
    private String fileName  = "serializedData/topicPartitionSerializedfile";

    private void checkValues(TopicPartition deSerTP) {
        //assert deserialized values are same as original
        //not using assertEquals for partition number to ensure the type casting will catch any change in datatype
        assertTrue("partition number should be " + partNum + " and of type int. Got " + deSerTP.partition(), partNum == (int) deSerTP.partition());
        assertEquals("topic should be " + topicName + " but got " + deSerTP.topic(), topicName, deSerTP.topic());
    }

    @Test
    public void testTopicPartitionSerialization() throws IOException, ClassNotFoundException {
        //assert TopicPartition is serializable and deserialization renders the clone of original properly
        TopicPartition origTp = new TopicPartition(topicName, partNum);
        byte[] byteArray = serialize(origTp);


        //deserialize the byteArray and check if the values are same as original
        Object deserializedObject = deserialize(byteArray);

        assertTrue(deserializedObject instanceof TopicPartition);

        checkValues((TopicPartition) deserializedObject);
    }

    @Test
    public void testTopiPartitionSerializationCompatibility() throws IOException, ClassNotFoundException {
        // assert serialized TopicPartition object in file (serializedData/topicPartitionSerializedfile) is
        // deserializable into TopicPartition and is compatible
        Object deserializedObject = deserialize(fileName);

        assertTrue(deserializedObject instanceof TopicPartition);

        checkValues((TopicPartition) deserializedObject);
    }
}
