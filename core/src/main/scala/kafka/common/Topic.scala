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

import util.matching.Regex

import scala.collection.immutable

object Topic {

  val GroupMetadataTopicName = "__consumer_offsets"
  val InternalTopics = immutable.Set(GroupMetadataTopicName)

  val legalChars = "[a-zA-Z0-9\\._\\-]"
  private val maxNameLength = 249
  private val rgx = new Regex(legalChars + "+")

  def validate(topic: String) {
    if (topic.length <= 0)
      throw new org.apache.kafka.common.errors.InvalidTopicException("topic name is illegal, can't be empty")
    else if (topic.equals(".") || topic.equals(".."))
      throw new org.apache.kafka.common.errors.InvalidTopicException("topic name cannot be \".\" or \"..\"")
    else if (topic.length > maxNameLength)
      throw new org.apache.kafka.common.errors.InvalidTopicException("topic name is illegal, can't be longer than " + maxNameLength + " characters")

    rgx.findFirstIn(topic) match {
      case Some(t) =>
        if (!t.equals(topic))
          throw new org.apache.kafka.common.errors.InvalidTopicException("topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, '.', '_' and '-'")
      case None => throw new org.apache.kafka.common.errors.InvalidTopicException("topic name " + topic + " is illegal,  contains a character other than ASCII alphanumerics, '.', '_' and '-'")
    }
  }

  /**
   * Due to limitations in metric names, topics with a period ('.') or underscore ('_') could collide.
   *
   * @param topic The topic to check for colliding character
   * @return true if the topic has collision characters
   */
  def hasCollisionChars(topic: String): Boolean = {
    topic.contains("_") || topic.contains(".")
  }

  /**
   * Returns true if the topicNames collide due to a period ('.') or underscore ('_') in the same position.
   *
   * @param topicA A topic to check for collision
   * @param topicB A topic to check for collision
   * @return true if the topics collide
   */
  def hasCollision(topicA: String, topicB: String): Boolean = {
    topicA.replace('.', '_') == topicB.replace('.', '_')
  }

  def isInternal(topic: String): Boolean =
    InternalTopics.contains(topic)

}
