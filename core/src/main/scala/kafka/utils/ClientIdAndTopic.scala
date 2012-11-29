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

import kafka.common.InvalidTopicException
import kafka.common.InvalidClientIdException
import util.matching.Regex

object ClientId {
  val legalChars = "[a-zA-Z0-9_-]"
  val maxNameLength = 200 // to prevent hitting filename max length limit
  private val rgx = new Regex(legalChars + "*")

  def validate(clientId: String) {
    if (clientId.length > maxNameLength)
      throw new InvalidClientIdException("ClientId is illegal, can't be longer than " + maxNameLength + " characters")

    rgx.findFirstIn(clientId) match {
      case Some(t) =>
        if (!t.equals(clientId))
          throw new InvalidClientIdException("ClientId " + clientId + " is illegal, contains a character other than ASCII alphanumerics, _ and -")
      case None => throw new InvalidClientIdException("ClientId " + clientId + " is illegal,  contains a character other than ASCII alphanumerics, _ and -")
    }
  }
}

object Topic {
  val legalChars = "[a-zA-Z0-9_-]"
  val maxNameLength = 255
  private val rgx = new Regex(legalChars + "+")

  def validate(topic: String) {
    if (topic.length <= 0)
      throw new InvalidTopicException("topic name is illegal, can't be empty")
    else if (topic.length > maxNameLength)
      throw new InvalidTopicException("topic name is illegal, can't be longer than " + maxNameLength + " characters")

    rgx.findFirstIn(topic) match {
      case Some(t) =>
        if (!t.equals(topic))
          throw new InvalidTopicException("topic name " + topic + " is illegal, contains a character other than ASCII alphanumerics, _ and -")
      case None => throw new InvalidTopicException("topic name " + topic + " is illegal,  contains a character other than ASCII alphanumerics, _ and -")
    }
  }
}

case class ClientIdAndTopic(clientId: String, topic:String) {
  override def toString = "%s-%s".format(clientId, topic)
}