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
package org.apache.kafka.server.share;

import org.apache.kafka.common.Uuid;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class SharePartitionKeyTest {
    @Test
    public void testGetInstanceFromSimpleKey() {
        Uuid topicId = Uuid.randomUuid();
        String key = "my-group:" + topicId + ":3";
        SharePartitionKey spk = SharePartitionKey.getInstance(key);
        assertEquals("my-group", spk.groupId());
        assertEquals(topicId, spk.topicId());
        assertEquals(3, spk.partition());
    }

    @Test
    public void testGetInstanceFromKeyWithColonInGroupId() {
        Uuid topicId = Uuid.randomUuid();
        String key = "abc:de:" + topicId + ":0";
        SharePartitionKey spk = SharePartitionKey.getInstance(key);
        assertEquals("abc:de", spk.groupId());
        assertEquals(topicId, spk.topicId());
        assertEquals(0, spk.partition());
    }

    @Test
    public void testGetInstanceFromKeyWithMultipleColonsInGroupId() {
        Uuid topicId = Uuid.randomUuid();
        String key = "a:b:c:d:" + topicId + ":5";
        SharePartitionKey spk = SharePartitionKey.getInstance(key);
        assertEquals("a:b:c:d", spk.groupId());
        assertEquals(topicId, spk.topicId());
        assertEquals(5, spk.partition());
    }

    @Test
    public void testGetInstanceFromKeyWithGroupIdStartingWithColon() {
        Uuid topicId = Uuid.randomUuid();
        String key = ":mygroup:" + topicId + ":2";
        SharePartitionKey spk = SharePartitionKey.getInstance(key);
        assertEquals(":mygroup", spk.groupId());
        assertEquals(topicId, spk.topicId());
        assertEquals(2, spk.partition());
    }

    @Test
    public void testGetInstanceFromKeyWithGroupIdEndingWithColon() {
        Uuid topicId = Uuid.randomUuid();
        String key = "mygroup::" + topicId + ":4";
        SharePartitionKey spk = SharePartitionKey.getInstance(key);
        assertEquals("mygroup:", spk.groupId());
        assertEquals(topicId, spk.topicId());
        assertEquals(4, spk.partition());
    }

    @Test
    public void testValidateSimpleKey() {
        Uuid topicId = Uuid.randomUuid();
        SharePartitionKey.validate("group:" + topicId + ":0");
    }

    @Test
    public void testValidateKeyWithColonInGroupId() {
        Uuid topicId = Uuid.randomUuid();
        SharePartitionKey.validate("abc:dd:" + topicId + ":0");
    }

    @Test
    public void testValidateRejectsNull() {
        assertThrows(NullPointerException.class, () -> SharePartitionKey.validate(null));
    }

    @Test
    public void testValidateRejectsEmpty() {
        assertThrows(IllegalArgumentException.class, () -> SharePartitionKey.validate(""));
    }

    @Test
    public void testValidateRejectsTooFewSegments() {
        assertThrows(IllegalArgumentException.class, () -> SharePartitionKey.validate("onlytwo:parts"));
    }

    @Test
    public void testValidateRejectsEmptyGroupId() {
        assertThrows(IllegalArgumentException.class, () -> SharePartitionKey.validate(":" + Uuid.randomUuid() + ":0"));
    }

    @Test
    public void testValidateRejectsInvalidTopicId() {
        assertThrows(IllegalArgumentException.class, () -> SharePartitionKey.validate("group:not-a-uuid:0"));
    }

    @Test
    public void testValidateRejectsInvalidPartition() {
        assertThrows(IllegalArgumentException.class, () -> SharePartitionKey.validate("group:" + Uuid.randomUuid() + ":abc"));
    }

    @Test
    public void testRoundTripWithCoordinatorKey() {
        Uuid topicId = Uuid.randomUuid();
        String groupId = "my:group:with:colons";
        String coordinatorKey = SharePartitionKey.asCoordinatorKey(groupId, topicId, 3);
        SharePartitionKey spk = SharePartitionKey.getInstance(coordinatorKey);
        assertEquals(groupId, spk.groupId());
        assertEquals(topicId, spk.topicId());
        assertEquals(3, spk.partition());
    }
}
