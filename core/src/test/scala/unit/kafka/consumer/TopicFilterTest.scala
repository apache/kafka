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


import junit.framework.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit.Test


class TopicFilterTest extends JUnitSuite {

  @Test
  def testWhitelists() {

    val topicFilter1 = new Whitelist("white1,white2")
    assertFalse(topicFilter1.requiresTopicEventWatcher)
    assertTrue(topicFilter1.isTopicAllowed("white2"))
    assertFalse(topicFilter1.isTopicAllowed("black1"))

    val topicFilter2 = new Whitelist(".+")
    assertTrue(topicFilter2.requiresTopicEventWatcher)
    assertTrue(topicFilter2.isTopicAllowed("alltopics"))
    
    val topicFilter3 = new Whitelist("white_listed-topic.+")
    assertTrue(topicFilter3.requiresTopicEventWatcher)
    assertTrue(topicFilter3.isTopicAllowed("white_listed-topic1"))
    assertFalse(topicFilter3.isTopicAllowed("black1"))
  }

  @Test
  def testBlacklists() {
    val topicFilter1 = new Blacklist("black1")
    assertTrue(topicFilter1.requiresTopicEventWatcher)
  }
}