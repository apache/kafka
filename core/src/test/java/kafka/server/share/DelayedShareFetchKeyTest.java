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
package kafka.server.share;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

public class DelayedShareFetchKeyTest {

    @Test
    public void testDelayedShareFetchEqualsAndHashcode() {
        TopicIdPartition tp0 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 0));
        TopicIdPartition tp1 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic", 1));
        TopicIdPartition tp2 = new TopicIdPartition(Uuid.randomUuid(), new TopicPartition("topic2", 0));

        Map<String, DelayedShareFetchKey> keyMap = new HashMap<>();
        keyMap.put("key0", new DelayedShareFetchKey(new HashSet<>(Collections.singletonList(tp0)), "grp", "member1"));
        keyMap.put("key1", new DelayedShareFetchKey(new HashSet<>(Collections.singletonList(tp1)), "grp", "member1"));
        keyMap.put("key2", new DelayedShareFetchKey(new HashSet<>(Collections.singletonList(tp2)), "grp", "member1"));
        keyMap.put("key3", new DelayedShareFetchKey(new HashSet<>(Arrays.asList(tp0, tp1)), "grp", "member1"));
        keyMap.put("key4", new DelayedShareFetchKey(new HashSet<>(Collections.singletonList(tp0)), "grp2", "member1"));
        keyMap.put("key5", new DelayedShareFetchKey(new HashSet<>(Collections.singletonList(tp1)), "grp", "member2"));

        keyMap.forEach((key1, value1) -> keyMap.forEach((key2, value2) -> {
            if (key1.equals(key2)) {
                assertEquals(value1, value2);
                assertEquals(value1.hashCode(), value2.hashCode());
            } else {
                assertNotEquals(value1, value2);
                assertNotEquals(value1.hashCode(), value2.hashCode());
            }
        }));
    }
}
