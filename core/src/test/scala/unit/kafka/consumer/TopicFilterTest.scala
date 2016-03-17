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

package kafka.consumer


import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import kafka.coordinator.GroupCoordinator
import org.apache.kafka.common.internals.TopicConstants


class TopicFilterTest extends JUnitSuite {

  @Test
  def testWhitelists() {

    val topicFilter1 = new Whitelist("white1,white2")
    assertTrue(topicFilter1.isTopicAllowed("white2", excludeInternalTopics = true))
    assertTrue(topicFilter1.isTopicAllowed("white2", excludeInternalTopics = false))
    assertFalse(topicFilter1.isTopicAllowed("black1", excludeInternalTopics = true))
    assertFalse(topicFilter1.isTopicAllowed("black1", excludeInternalTopics = false))

    val topicFilter2 = new Whitelist(".+")
    assertTrue(topicFilter2.isTopicAllowed("alltopics", excludeInternalTopics = true))
    assertFalse(topicFilter2.isTopicAllowed(TopicConstants.GROUP_METADATA_TOPIC_NAME, excludeInternalTopics = true))
    assertTrue(topicFilter2.isTopicAllowed(TopicConstants.GROUP_METADATA_TOPIC_NAME, excludeInternalTopics = false))

    val topicFilter3 = new Whitelist("white_listed-topic.+")
    assertTrue(topicFilter3.isTopicAllowed("white_listed-topic1", excludeInternalTopics = true))
    assertFalse(topicFilter3.isTopicAllowed("black1", excludeInternalTopics = true))

    val topicFilter4 = new Whitelist("test-(?!bad\\b)[\\w]+")
    assertTrue(topicFilter4.isTopicAllowed("test-good", excludeInternalTopics = true))
    assertFalse(topicFilter4.isTopicAllowed("test-bad", excludeInternalTopics = true))    
  }

  @Test
  def testBlacklists() {
    val topicFilter1 = new Blacklist("black1")
    assertTrue(topicFilter1.isTopicAllowed("white2", excludeInternalTopics = true))
    assertTrue(topicFilter1.isTopicAllowed("white2", excludeInternalTopics = false))
    assertFalse(topicFilter1.isTopicAllowed("black1", excludeInternalTopics = true))
    assertFalse(topicFilter1.isTopicAllowed("black1", excludeInternalTopics = false))

    assertFalse(topicFilter1.isTopicAllowed(TopicConstants.GROUP_METADATA_TOPIC_NAME, excludeInternalTopics = true))
    assertTrue(topicFilter1.isTopicAllowed(TopicConstants.GROUP_METADATA_TOPIC_NAME, excludeInternalTopics = false))
  }

  @Test
  def testWildcardTopicCountGetTopicCountMapEscapeJson() {
    def getTopicCountMapKey(regex: String): String = {
      val topicCount = new WildcardTopicCount(null, "consumerId", new Whitelist(regex), 1, true)
      topicCount.getTopicCountMap.head._1
    }
    //lets make sure that the JSON strings are escaping as we expect
    //if they are not then when they get saved to zookeeper and read back out they will be broken on parse
    assertEquals("-\\\"-", getTopicCountMapKey("-\"-"))
    assertEquals("-\\\\-", getTopicCountMapKey("-\\-"))
    assertEquals("-\\/-", getTopicCountMapKey("-/-"))
    assertEquals("-\\\\b-", getTopicCountMapKey("-\\b-"))
    assertEquals("-\\\\f-", getTopicCountMapKey("-\\f-"))
    assertEquals("-\\\\n-", getTopicCountMapKey("-\\n-"))
    assertEquals("-\\\\r-", getTopicCountMapKey("-\\r-"))
    assertEquals("-\\\\t-", getTopicCountMapKey("-\\t-"))
    assertEquals("-\\\\u0000-", getTopicCountMapKey("-\\u0000-"))
    assertEquals("-\\\\u001f-", getTopicCountMapKey("-\\u001f-"))
    assertEquals("-\\\\u007f-", getTopicCountMapKey("-\\u007f-"))
    assertEquals("-\\\\u009f-", getTopicCountMapKey("-\\u009f-"))
  }    
}