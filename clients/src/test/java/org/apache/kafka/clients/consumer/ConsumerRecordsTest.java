/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package org.apache.kafka.clients.consumer;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.easymock.PowerMock;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(ConsumerRecord.class)
public class ConsumerRecordsTest {

    @Test
    @SuppressWarnings("unchecked")
    public void iterator() throws Exception {

        Map<TopicPartition, List<ConsumerRecord<Integer, String>>> records = new HashMap<>();
        ConsumerRecord<Integer, String> record1 = PowerMock.createMock(ConsumerRecord.class);
        ConsumerRecord<Integer, String> record2 = PowerMock.createMock(ConsumerRecord.class);
        records.put(new TopicPartition("topic", 0), new ArrayList<ConsumerRecord<Integer, String>>());
        records.put(new TopicPartition("topic", 1), Arrays.asList(record1, record2));

        ConsumerRecords<Integer, String> consumerRecords = new ConsumerRecords<>(records);
        Iterator<ConsumerRecord<Integer, String>> iter = consumerRecords.iterator();

        int c = 0;
        for (; iter.hasNext(); iter.next()) {
            c++;
        }
        assertEquals(2, c);
    }
}