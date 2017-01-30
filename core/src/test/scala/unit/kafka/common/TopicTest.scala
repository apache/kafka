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

package kafka.common

import org.junit.Assert._
import collection.mutable.ArrayBuffer
import org.junit.Test

class TopicTest {

  @Test
  def testInvalidTopicNames() {
    val invalidTopicNames = new ArrayBuffer[String]()
    invalidTopicNames += ("", ".", "..")
    var longName = "ATCG"
    for (_ <- 1 to 6)
      longName += longName
    invalidTopicNames += longName
    invalidTopicNames += longName.drop(6)
    val badChars = Array('/', '\\', ',', '\u0000', ':', "\"", '\'', ';', '*', '?', ' ', '\t', '\r', '\n', '=')
    for (weirdChar <- badChars) {
      invalidTopicNames += "Is" + weirdChar + "illegal"
    }

    for (i <- invalidTopicNames.indices) {
      try {
        Topic.validate(invalidTopicNames(i))
        fail("Should throw InvalidTopicException.")
      }
      catch {
        case _: org.apache.kafka.common.errors.InvalidTopicException => // This is good.
      }
    }

    val validTopicNames = new ArrayBuffer[String]()
    validTopicNames += ("valid", "TOPIC", "nAmEs", "ar6", "VaL1d", "_0-9_.", longName.drop(7))
    for (i <- validTopicNames.indices) {
      try {
        Topic.validate(validTopicNames(i))
      }
      catch {
        case _: Exception => fail("Should not throw exception.")
      }
    }
  }

  @Test
  def testTopicHasCollisionChars() = {
    val falseTopics = List("start", "end", "middle", "many")
    val trueTopics = List(
      ".start", "end.", "mid.dle", ".ma.ny.",
      "_start", "end_", "mid_dle", "_ma_ny."
    )

    falseTopics.foreach( t =>
      assertFalse(Topic.hasCollisionChars(t))
    )

    trueTopics.foreach( t =>
      assertTrue(Topic.hasCollisionChars(t))
    )
  }

  @Test
  def testTopicHasCollision() = {
    val periodFirstMiddleLastNone = List(".topic", "to.pic", "topic.", "topic")
    val underscoreFirstMiddleLastNone = List("_topic", "to_pic", "topic_", "topic")

    // Self
    periodFirstMiddleLastNone.foreach { t =>
      assertTrue(Topic.hasCollision(t, t))
    }
    underscoreFirstMiddleLastNone.foreach { t =>
      assertTrue(Topic.hasCollision(t, t))
    }

    // Same Position
    periodFirstMiddleLastNone.zip(underscoreFirstMiddleLastNone).foreach { case (t1, t2) =>
      assertTrue(Topic.hasCollision(t1, t2))
    }

    // Different Position
    periodFirstMiddleLastNone.zip(underscoreFirstMiddleLastNone.reverse).foreach { case (t1, t2) =>
      assertFalse(Topic.hasCollision(t1, t2))
    }
  }
}
