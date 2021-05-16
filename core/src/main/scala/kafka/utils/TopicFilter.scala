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

import java.util.regex.{Pattern, PatternSyntaxException}

import org.apache.kafka.common.internals.Topic

sealed abstract class TopicFilter(rawRegex: String) extends Logging {

  val regex = rawRegex
          .trim
          .replace(',', '|')
          .replace(" ", "")
          .replaceAll("""^["']+""","")
          .replaceAll("""["']+$""","") // property files may bring quotes

  try {
    Pattern.compile(regex)
  }
  catch {
    case _: PatternSyntaxException =>
      throw new RuntimeException(regex + " is an invalid regex.")
  }

  override def toString = regex

  def isTopicAllowed(topic: String, excludeInternalTopics: Boolean): Boolean
}

case class IncludeList(rawRegex: String) extends TopicFilter(rawRegex) {
  override def isTopicAllowed(topic: String, excludeInternalTopics: Boolean) = {
    val allowed = topic.matches(regex) && !(Topic.isInternal(topic) && excludeInternalTopics)

    debug("%s %s".format(
      topic, if (allowed) "allowed" else "filtered"))

    allowed
  }
}
