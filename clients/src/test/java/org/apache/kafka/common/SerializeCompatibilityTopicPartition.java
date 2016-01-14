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

public class SerializeCompatibilityTopicPartition extends Serializer {

    public static String topicName = "mytopic";
    public static int    partNum   = 5;
    public static String fileName  = "serializedData/topicPartitionSerializedfile";

    @Test
    public void testTopicPartitionSerialization() throws IOException, ClassNotFoundException {
        //assert TopicPartition is serializable and de-serialization renders the clone of original properly
        TopicPartition origTp = new TopicPartition(topicName, partNum);

        serialize(origTp);

        // assert serialized TopicPartition object in file (tpserializedfile under resources folder) is
        // de-serializable into TopicPartition and compatible
        Object deserializedObject = deSerialize(fileName);

        assertTrue(deserializedObject instanceof TopicPartition);

        if (deserializedObject instanceof TopicPartition) {
            TopicPartition deSerTP = (TopicPartition) deserializedObject;
            //assert topic is of type String
            assertTrue("topic should be of type String", deSerTP.topic() instanceof String);

            //assert de-serialized values are same as original
            //not using assertEquals for partition number to ensure the type casting will catch any change in datatype
            assertTrue("partition number should be " + partNum + " and of type int. Got " + deSerTP.partition(), partNum == (int) deSerTP.partition());
            assertEquals("topic should be " + topicName + " but got " + deSerTP.topic(), topicName, deSerTP.topic());
        }
    }
}
