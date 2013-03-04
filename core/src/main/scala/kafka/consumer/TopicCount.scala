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
import org.I0Itec.zkclient.ZkClient
import kafka.utils.{Json, ZKGroupDirs, ZkUtils, Logging}
import kafka.common.KafkaException

private[kafka] trait TopicCount {

  def getConsumerThreadIdsPerTopic: Map[String, Set[String]]
  def dbString: String
  def pattern: String
  
  protected def makeConsumerThreadIdsPerTopic(consumerIdString: String,
                                            topicCountMap: Map[String,  Int]) = {
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
}

private[kafka] object TopicCount extends Logging {
  val whiteListPattern = "white_list"
  val blackListPattern = "black_list"
  val staticPattern = "static"

  def constructTopicCount(group: String, consumerId: String, zkClient: ZkClient) : TopicCount = {
    val dirs = new ZKGroupDirs(group)
    val topicCountString = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId)._1
    var subscriptionPattern: String = null
    var topMap: Map[String, Int] = null
    try {
      Json.parseFull(topicCountString) match {
        case Some(m) =>
          val consumerRegistrationMap = m.asInstanceOf[Map[String, Any]]
          consumerRegistrationMap.get("pattern") match {
            case Some(pattern) => subscriptionPattern = pattern.asInstanceOf[String]
            case None => throw new KafkaException("error constructing TopicCount : " + topicCountString)
          }
          consumerRegistrationMap.get("subscription") match {
            case Some(sub) => topMap = sub.asInstanceOf[Map[String, Int]]
            case None => throw new KafkaException("error constructing TopicCount : " + topicCountString)
          }
        case None => throw new KafkaException("error constructing TopicCount : " + topicCountString)
      }
    } catch {
      case e =>
        error("error parsing consumer json string " + topicCountString, e)
        throw e
    }

    val hasWhiteList = whiteListPattern.equals(subscriptionPattern)
    val hasBlackList = blackListPattern.equals(subscriptionPattern)

    if (topMap.isEmpty || !(hasWhiteList || hasBlackList)) {
      new StaticTopicCount(consumerId, topMap)
    } else {
      val regex = topMap.head._1
      val numStreams = topMap.head._2
      val filter =
        if (hasWhiteList)
          new Whitelist(regex)
        else
          new Blacklist(regex)
      new WildcardTopicCount(zkClient, consumerId, filter, numStreams)
    }
  }

  def constructTopicCount(consumerIdString: String, topicCount: Map[String, Int]) =
    new StaticTopicCount(consumerIdString, topicCount)

  def constructTopicCount(consumerIdString: String, filter: TopicFilter, numStreams: Int, zkClient: ZkClient) =
    new WildcardTopicCount(zkClient, consumerIdString, filter, numStreams)

}

private[kafka] class StaticTopicCount(val consumerIdString: String,
                                val topicCountMap: Map[String, Int])
                                extends TopicCount {

  def getConsumerThreadIdsPerTopic = makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap)

  override def equals(obj: Any): Boolean = {
    obj match {
      case null => false
      case n: StaticTopicCount => consumerIdString == n.consumerIdString && topicCountMap == n.topicCountMap
      case _ => false
    }
  }

  /**
   *  return json of
   *  { "topic1" : 4,
   *    "topic2" : 4 }
   */
  def dbString = {
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
    builder.toString()
  }

  def pattern = TopicCount.staticPattern
}

private[kafka] class WildcardTopicCount(zkClient: ZkClient,
                                        consumerIdString: String,
                                        topicFilter: TopicFilter,
                                        numStreams: Int) extends TopicCount {
  def getConsumerThreadIdsPerTopic = {
    val wildcardTopics = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath).filter(topicFilter.isTopicAllowed(_))
    makeConsumerThreadIdsPerTopic(consumerIdString, Map(wildcardTopics.map((_, numStreams)): _*))
  }

  def dbString = "{ \"%s\" : %d }".format(topicFilter.regex, numStreams)

  def pattern: String = {
    topicFilter match {
      case wl: Whitelist => TopicCount.whiteListPattern
      case bl: Blacklist => TopicCount.blackListPattern
    }
  }

}

