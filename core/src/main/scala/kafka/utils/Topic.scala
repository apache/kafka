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
import util.matching.Regex

object Topic {
  val maxNameLength = 255
  val illegalChars = "/" + '\u0000' + '\u0001' + "-" + '\u001F' + '\u007F' + "-" + '\u009F' +
                     '\uD800' + "-" + '\uF8FF' + '\uFFF0' + "-" + '\uFFFF'
}

class TopicNameValidator(maxLen: Int) {
  // Regex checks for illegal chars and "." and ".." filenames
  private val rgx = new Regex("(^\\.{1,2}$)|[" + Topic.illegalChars + "]")

  def validate(topic: String) {
    if (topic.length <= 0)
      throw new InvalidTopicException("topic name is illegal, can't be empty")
    else if (topic.length > maxLen)
      throw new InvalidTopicException("topic name is illegal, can't be longer than " + maxLen + " characters")

    rgx.findFirstIn(topic) match {
      case Some(t) => throw new InvalidTopicException("topic name " + topic + " is illegal, doesn't match expected regular expression")
      case None =>
    }
  }
}
