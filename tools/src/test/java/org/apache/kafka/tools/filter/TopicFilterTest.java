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
package org.apache.kafka.tools.filter;

import org.apache.kafka.common.internals.Topic;
import org.apache.kafka.tools.filter.TopicFilter.IncludeList;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TopicFilterTest {
    @Test
    public void testIncludeLists() {
        IncludeList topicFilter1 = new TopicFilter.IncludeList("yes1,yes2");
        assertTrue(topicFilter1.isTopicAllowed("yes2", true));
        assertTrue(topicFilter1.isTopicAllowed("yes2", false));
        assertFalse(topicFilter1.isTopicAllowed("no1", true));
        assertFalse(topicFilter1.isTopicAllowed("no1", false));

        IncludeList topicFilter2 = new IncludeList(".+");
        assertTrue(topicFilter2.isTopicAllowed("alltopics", true));
        assertFalse(topicFilter2.isTopicAllowed(Topic.GROUP_METADATA_TOPIC_NAME, true));
        assertTrue(topicFilter2.isTopicAllowed(Topic.GROUP_METADATA_TOPIC_NAME, false));

        assertFalse(topicFilter2.isTopicAllowed(Topic.TRANSACTION_STATE_TOPIC_NAME, true));
        assertTrue(topicFilter2.isTopicAllowed(Topic.TRANSACTION_STATE_TOPIC_NAME, false));

        IncludeList topicFilter3 = new IncludeList("included-topic.+");
        assertTrue(topicFilter3.isTopicAllowed("included-topic1", true));
        assertFalse(topicFilter3.isTopicAllowed("no1", true));

        IncludeList topicFilter4 = new IncludeList("test-(?!bad\\b)[\\w]+");
        assertTrue(topicFilter4.isTopicAllowed("test-good", true));
        assertFalse(topicFilter4.isTopicAllowed("test-bad", true));
    }
}
