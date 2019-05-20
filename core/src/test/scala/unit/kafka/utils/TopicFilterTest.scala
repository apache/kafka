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
 */

package kafka.utils

import org.apache.kafka.common.internals.Topic
import org.junit.Assert._
import org.junit.Test

class TopicFilterTest {

  @Test
  def testWhitelists() {

    val topicFilter1 = Whitelist("white1,white2")
    assertTrue(topicFilter1.isTopicAllowed("white2", excludeInternalTopics = true))
    assertTrue(topicFilter1.isTopicAllowed("white2", excludeInternalTopics = false))
    assertFalse(topicFilter1.isTopicAllowed("black1", excludeInternalTopics = true))
    assertFalse(topicFilter1.isTopicAllowed("black1", excludeInternalTopics = false))

    val topicFilter2 = Whitelist(".+")
    assertTrue(topicFilter2.isTopicAllowed("alltopics", excludeInternalTopics = true))
    assertFalse(topicFilter2.isTopicAllowed(Topic.GROUP_METADATA_TOPIC_NAME, excludeInternalTopics = true))
    assertTrue(topicFilter2.isTopicAllowed(Topic.GROUP_METADATA_TOPIC_NAME, excludeInternalTopics = false))

    val topicFilter3 = Whitelist("white_listed-topic.+")
    assertTrue(topicFilter3.isTopicAllowed("white_listed-topic1", excludeInternalTopics = true))
    assertFalse(topicFilter3.isTopicAllowed("black1", excludeInternalTopics = true))

    val topicFilter4 = Whitelist("test-(?!bad\\b)[\\w]+")
    assertTrue(topicFilter4.isTopicAllowed("test-good", excludeInternalTopics = true))
    assertFalse(topicFilter4.isTopicAllowed("test-bad", excludeInternalTopics = true))
  }

}
