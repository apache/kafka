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

package org.apache.kafka.tools;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.tools.filter.TopicPartitionFilter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class GetOffsetShellParsingTest {
    GetOffsetShell getOffsetShell = new GetOffsetShell();

    @Test
    public void testTopicPartitionFilterForTopicName() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList("test");

        assertTrue(topicPartitionFilter.isTopicAllowed("test"));
        assertFalse(topicPartitionFilter.isTopicAllowed("test1"));
        assertFalse(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 1)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 0)));
    }

    @Test
    public void testTopicPartitionFilterForInternalTopicName() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList("__consumer_offsets");

        assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));
        assertFalse(topicPartitionFilter.isTopicAllowed("test1"));
        assertFalse(topicPartitionFilter.isTopicAllowed("test2"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 1)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test2", 0)));
    }


    @Test
    public void testTopicPartitionFilterForTopicNameList() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList("test,test1,__consumer_offsets");

        assertTrue(topicPartitionFilter.isTopicAllowed("test"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test1"));
        assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));
        assertFalse(topicPartitionFilter.isTopicAllowed("test2"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test2", 0)));
    }


    @Test
    public void testTopicPartitionFilterForRegex() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList("test.*");

        assertTrue(topicPartitionFilter.isTopicAllowed("test"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test1"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test2"));
        assertFalse(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test2", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 0)));
    }

    @Test
    public void testTopicPartitionFilterForPartitionIndexSpec() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList(":0");

        assertTrue(topicPartitionFilter.isTopicAllowed("test"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test1"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test2"));
        assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test2", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 1)));
    }

    @Test
    public void testTopicPartitionFilterForPartitionRangeSpec() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList(":1-3");

        assertTrue(topicPartitionFilter.isTopicAllowed("test"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test1"));
        assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test2"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 2)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 2)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test2", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test2", 3)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 3)));
    }

    @Test
    public void testTopicPartitionFilterForPartitionLowerBoundSpec() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList(":1-");

        assertTrue(topicPartitionFilter.isTopicAllowed("test"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test1"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test2"));
        assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 2)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 2)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test2", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 0)));
    }

    @Test
    public void testTopicPartitionFilterForPartitionUpperBoundSpec() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList(":-3");
        assertTrue(topicPartitionFilter.isTopicAllowed("test"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test1"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test2"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test3"));
        assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test2", 2)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 2)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test3", 3)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 3)));
    }

    @Test
    public void testTopicPartitionFilterComplex() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList("test.*:0,__consumer_offsets:1-2,.*:3");

        assertTrue(topicPartitionFilter.isTopicAllowed("test"));
        assertTrue(topicPartitionFilter.isTopicAllowed("test1"));
        assertTrue(topicPartitionFilter.isTopicAllowed("custom"));
        assertTrue(topicPartitionFilter.isTopicAllowed("__consumer_offsets"));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 1)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test1", 1)));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("custom", 3)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("custom", 0)));

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 3)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("__consumer_offsets", 2)));
    }

    @Test
    public void testPartitionFilterForSingleIndex() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList(":1");
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 1)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 2)));
    }

    @Test
    public void testPartitionFilterForRange() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList(":1-3");
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 2)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 3)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 4)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 5)));
    }

    @Test
    public void testPartitionFilterForLowerBound() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList(":3-");

        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 1)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 2)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 3)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 4)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 5)));
    }

    @Test
    public void testPartitionFilterForUpperBound() {
        TopicPartitionFilter topicPartitionFilter = getOffsetShell.createTopicPartitionFilterWithPatternList(":-3");

        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 0)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 1)));
        assertTrue(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 2)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 3)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 4)));
        assertFalse(topicPartitionFilter.isTopicPartitionAllowed(getTopicPartition("test", 5)));
    }

    @Test
    public void testPartitionsSetFilter() throws TerseException {
        TopicPartitionFilter partitionsSetFilter = getOffsetShell.createTopicPartitionFilterWithTopicAndPartitionPattern("topic", "1,3,5");

        assertFalse(partitionsSetFilter.isTopicPartitionAllowed(getTopicPartition("topic", 0)));
        assertFalse(partitionsSetFilter.isTopicPartitionAllowed(getTopicPartition("topic", 2)));
        assertFalse(partitionsSetFilter.isTopicPartitionAllowed(getTopicPartition("topic", 4)));

        assertFalse(partitionsSetFilter.isTopicPartitionAllowed(getTopicPartition("topic1", 1)));
        assertFalse(partitionsSetFilter.isTopicAllowed("topic1"));

        assertTrue(partitionsSetFilter.isTopicPartitionAllowed(getTopicPartition("topic", 1)));
        assertTrue(partitionsSetFilter.isTopicPartitionAllowed(getTopicPartition("topic", 3)));
        assertTrue(partitionsSetFilter.isTopicPartitionAllowed(getTopicPartition("topic", 5)));
        assertTrue(partitionsSetFilter.isTopicAllowed("topic"));
    }

    @Test
    public void testInvalidTimeValue() {
        assertThrows(TerseException.class, () -> GetOffsetShell.execute("--bootstrap-server", "localhost:9092", "--time", "invalid"));
    }

    @Test
    public void testInvalidOffset() {
        assertEquals("Malformed time argument foo. " +
                        "Please use -1 or latest / -2 or earliest / -3 or max-timestamp / -4 or earliest-local / -5 or latest-tiered, or a specified long format timestamp",
                assertThrows(TerseException.class, () -> GetOffsetShell.parseOffsetSpec("foo")).getMessage());
    }

    private TopicPartition getTopicPartition(String topic, Integer partition) {
        return new TopicPartition(topic, partition);
    }
}
