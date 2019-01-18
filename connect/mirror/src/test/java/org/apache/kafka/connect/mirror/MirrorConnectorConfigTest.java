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

import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

import java.util.Collections;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.HashSet;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertEquals;

public class MirrorConnectorConfigTest {

    @Test
    public void testTaskConfigTopicPartitions() {
        List<TopicPartition> topicPartitions = Arrays.asList(new TopicPartition("topic-1", 2),
            new TopicPartition("topic-3", 4), new TopicPartition("topic-5", 6));
        MirrorConnectorConfig config = new MirrorConnectorConfig(Collections.emptyMap());
        Map<?, ?> props = config.taskConfigForTopicPartitions(topicPartitions);
        MirrorTaskConfig taskConfig = new MirrorTaskConfig(props);
        assertEquals(taskConfig.taskTopicPartitions(), new HashSet<>(topicPartitions));
    }

    @Test
    public void testTaskConfigConsumerGroups() {
        List<String> groups = Arrays.asList("consumer-1", "consumer-2", "consumer-3");
        MirrorConnectorConfig config = new MirrorConnectorConfig(Collections.emptyMap());
        Map<?, ?> props = config.taskConfigForConsumerGroups(groups);
        MirrorTaskConfig taskConfig = new MirrorTaskConfig(props);
        assertEquals(taskConfig.taskConsumerGroups(), new HashSet<>(groups));
    }

    @Test
    public void testTopicMatching() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(
            Collections.singletonMap("topics", "topic1"));
        assertTrue(config.topicsPattern().matcher("topic1").matches());
        assertFalse(config.topicsPattern().matcher("topic2").matches());
    }

    @Test
    public void testNoTopics() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(
            Collections.singletonMap("topics", ""));
        assertFalse(config.topicsPattern().matcher("topic1").matches());
        assertFalse(config.topicsPattern().matcher("topic2").matches());
        assertFalse(config.topicsPattern().matcher("").matches());
    }

    @Test
    public void testAllTopics() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(
            Collections.singletonMap("topics", ".*"));
        assertTrue(config.topicsPattern().matcher("topic1").matches());
        assertTrue(config.topicsPattern().matcher("topic2").matches());
    }

    @Test
    public void testListOfTopics() {
        MirrorConnectorConfig config = new MirrorConnectorConfig(
            Collections.singletonMap("topics", "topic1, topic2"));
        assertTrue(config.topicsPattern().matcher("topic1").matches());
        assertTrue(config.topicsPattern().matcher("topic2").matches());
        assertFalse(config.topicsPattern().matcher("topic3").matches());
    }
}
