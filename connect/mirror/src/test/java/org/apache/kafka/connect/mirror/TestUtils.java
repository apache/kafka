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
package org.apache.kafka.connect.mirror;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestUtils {

    static Map<String, String> makeProps(String... keyValues) {
        Map<String, String> props = new HashMap<>();
        props.put("name", "ConnectorName");
        props.put("connector.class", "ConnectorClass");
        props.put("source.cluster.alias", "source1");
        props.put("target.cluster.alias", "target2");
        for (int i = 0; i < keyValues.length; i += 2) {
            props.put(keyValues[i], keyValues[i + 1]);
        }
        return props;
    }

    /**
     * Assemble a collection of records arbitrarily distributed across all partitions of the specified topic
     * @param topicName Destination topic
     * @param numRecords count of records to produce to the topic in total
     * @return A batch of records that can be sent to a producer.
     */
    public static List<ProducerRecord<byte[], byte[]>> generateRecords(String topicName, int numRecords) {
        List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
        for (int i = 0; i < numRecords; i++) {
            String key = "key-" + i;
            String value = "message-" + i;
            records.add(new ProducerRecord<>(topicName, null, key.getBytes(), value.getBytes()));
        }
        return records;
    }

    /**
     * Assemble a collection of records evenly distributed across some partitions of the specified topic
     * @param topicName Destination topic
     * @param numRecords count of records to produce to each partition
     * @param numPartitions number of partitions within the topic to write records to.
     * @return A batch of records that can be sent to a producer.
     */
    public static List<ProducerRecord<byte[], byte[]>> generateRecords(String topicName, int numRecords, int numPartitions) {
        List<ProducerRecord<byte[], byte[]>> records = new ArrayList<>();
        int cnt = 0;
        for (int r = 0; r < numRecords; r++) {
            for (int p = 0; p < numPartitions; p++) {
                String value = "value-" + cnt;
                String key = "key-" + cnt;
                cnt++;
                records.add(new ProducerRecord<>(topicName, p, key.getBytes(), value.getBytes()));
            }
        }
        return records;
    }

    public static void assertEqualsExceptClientId(Map<String, Object> expected, Map<String, Object> actual) {
        Map<String, Object> expectedWithoutClientId = new HashMap<>(expected);
        expectedWithoutClientId.remove("client.id");
        Map<String, Object> actualWithoutClientId = new HashMap<>(actual);
        actualWithoutClientId.remove("client.id");
        assertEquals(expectedWithoutClientId, actualWithoutClientId);
    }
}
