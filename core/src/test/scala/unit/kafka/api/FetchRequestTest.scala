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
package kafka.api

import kafka.common.TopicAndPartition
import org.junit.Assert.{assertEquals, assertNotEquals}
import org.junit.Test

class FetchRequestTest {

  @Test
  def testShuffle() {
    val seq = (0 to 100).map { i =>
      val topic = s"topic${i % 10}"
      (TopicAndPartition(topic, i / 10), PartitionFetchInfo(i, 50))
    }
    val shuffled = FetchRequest.shuffle(seq)
    assertEquals(seq.size, shuffled.size)
    assertNotEquals(seq, shuffled)

    seq.foreach { case (tp1, fetchInfo1) =>
      shuffled.foreach { case (tp2, fetchInfo2) =>
        if (tp1 == tp2)
          assertEquals(fetchInfo1, fetchInfo2)
      }
    }

    val topics = seq.map { case (TopicAndPartition(t, _), _) => t }.distinct
    topics.foreach { topic =>
      val startIndex = shuffled.indexWhere { case (tp, _) => tp.topic == topic }
      val endIndex = shuffled.lastIndexWhere { case (tp, _) => tp.topic == topic }
      // all partitions for a given topic should appear in sequence
      assertEquals(Set(topic), shuffled.slice(startIndex, endIndex + 1).map { case (tp, _) => tp.topic }.toSet)
    }

    val shuffled2 = FetchRequest.shuffle(seq)
    assertNotEquals(shuffled, shuffled2)
    assertNotEquals(seq, shuffled2)
  }

  @Test
  def testShuffleWithSingleTopic() {
    val seq = (0 to 50).map(i => (TopicAndPartition("topic", i), PartitionFetchInfo(i, 70)))
    val shuffled = FetchRequest.shuffle(seq)
    assertEquals(seq.size, shuffled.size)
    assertNotEquals(seq, shuffled)
  }

}
