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
import kafka.server.KafkaConfig

class TopicNameValidator(config: KafkaConfig) {
  private val illegalChars = "/" + '\u0000' + '\u0001' + "-" + '\u001F' + '\u007F' + "-" + '\u009F' +
                          '\uD800' + "-" + '\uF8FF' + '\uFFF0' + "-" + '\uFFFF'
  // Regex checks for illegal chars and "." and ".." filenames
  private val rgx = new Regex("(^\\.{1,2}$)|[" + illegalChars + "]")

  def validate(topic: String) {
    if (topic.length <= 0)
      throw new InvalidTopicException("topic name is illegal, can't be empty")
    else if (topic.length > config.maxTopicNameLength)
      throw new InvalidTopicException("topic name is illegal, can't be longer than " + config.maxTopicNameLength + " characters")

    rgx.findFirstIn(topic) match {
      case Some(t) => throw new InvalidTopicException("topic name " + topic + " is illegal, doesn't match expected regular expression")
      case None =>
    }
  }
}
