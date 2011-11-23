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

import scala.collection._
import scala.util.parsing.json.JSON
import kafka.utils.Logging

private[consumer] object TopicCount extends Logging {
  val myConversionFunc = {input : String => input.toInt}
  JSON.globalNumberParser = myConversionFunc

  def constructTopicCount(consumerIdSting: String, jsonString : String) : TopicCount = {
    var topMap : Map[String,Int] = null
    try {
      JSON.parseFull(jsonString) match {
        case Some(m) => topMap = m.asInstanceOf[Map[String,Int]]
        case None => throw new RuntimeException("error constructing TopicCount : " + jsonString)
      }
    }
    catch {
      case e =>
        error("error parsing consumer json string " + jsonString, e)
        throw e
    }

    new TopicCount(consumerIdSting, topMap)
  }

}

private[consumer] class TopicCount(val consumerIdString: String, val topicCountMap: Map[String, Int]) {

  def getConsumerThreadIdsPerTopic()
    : Map[String, Set[String]] = {
    val consumerThreadIdsPerTopicMap = new mutable.HashMap[String, Set[String]]()
    for ((topic, nConsumers) <- topicCountMap) {
      val consumerSet = new mutable.HashSet[String]
      assert(nConsumers >= 1)
      for (i <- 0 until nConsumers)
        consumerSet += consumerIdString + "-" + i
      consumerThreadIdsPerTopicMap.put(topic, consumerSet)
    }
    consumerThreadIdsPerTopicMap
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case n: TopicCount => consumerIdString == n.consumerIdString && topicCountMap == n.topicCountMap
      case _ => false
    }
  }

  /**
   *  return json of
   *  { "topic1" : 4,
   *    "topic2" : 4
   *  }
   */
  def toJsonString() : String = {
    val builder = new StringBuilder
    builder.append("{ ")
    var i = 0
    for ( (topic, nConsumers) <- topicCountMap) {
      if (i > 0)
        builder.append(",")
      builder.append("\"" + topic + "\": " + nConsumers)
      i += 1
    }
    builder.append(" }")
    builder.toString
  }
}
