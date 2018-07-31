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
package org.apache.kafka.connect.kafka;

import org.junit.Before;
import org.junit.Test;
import org.junit.After;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static org.junit.Assert.assertFalse;

import static org.junit.Assert.assertEquals;

@RunWith(PowerMockRunner.class)
@PrepareForTest({LeaderTopicPartition.class})
@PowerMockIgnore("javax.management.*")

public class LeaderTopicPartitionTest {

    private LeaderTopicPartition objectUnderTest;

    private static final int LEADER_ID = 0;
    private static final String TOPIC = "test.topic";
    private static final int PARTITION = 1;

    private static final String LEADER_TOPIC_PARTITION = "0:test.topic:1";
    private static final String TOPIC_PARTITION = "test.topic:1";

    @Before
    public void setup() {
        objectUnderTest = new LeaderTopicPartition(
                LEADER_ID,
                TOPIC,
                PARTITION
        );
    }

    @After
    public void teardown() {
        objectUnderTest = null;
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullTopicName() {
        objectUnderTest = new LeaderTopicPartition(
                LEADER_ID,
                null,
                PARTITION
        );
    }

    @Test
    public void testToString() {
        assertEquals(LEADER_TOPIC_PARTITION, objectUnderTest.toString());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testFromInvalidString() {
        String invalidString = "test";
        LeaderTopicPartition.fromString(invalidString);
    }

    @Test
    public void testFromString() {
        objectUnderTest = LeaderTopicPartition.fromString(LEADER_TOPIC_PARTITION);
        assertEquals(LEADER_TOPIC_PARTITION, objectUnderTest.toString());
    }

    @Test
    public void testToTopicPartitionString() {
        assertEquals(TOPIC_PARTITION, objectUnderTest.toTopicPartitionString());
    }


    @Test
    public void testEquals() {
        LeaderTopicPartition objectUnderTest1 = new LeaderTopicPartition(
                LEADER_ID,
                TOPIC,
                PARTITION
        );
        LeaderTopicPartition objectUnderTest2 = new LeaderTopicPartition(
                LEADER_ID,
                TOPIC,
                PARTITION
        );
        assert objectUnderTest1.equals(objectUnderTest2);
    }

    @Test
    public void testEqualsWithOtherInstance() {
        String unexpectedString = "NOT_LEADER_TOPIC_PARTITION";
        assertFalse(objectUnderTest.equals(unexpectedString));
    }

}
