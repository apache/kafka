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
import java.util.regex.Pattern
import kafka.utils.{SyncJSON, ZKGroupDirs, ZkUtils, Logging}


private[kafka] trait TopicCount {
  def getConsumerThreadIdsPerTopic: Map[String, Set[String]]

  def dbString: String
  
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

  /*
   * Example of whitelist topic count stored in ZooKeeper:
   * Topics with whitetopic as prefix, and four streams: *4*whitetopic.*
   *
   * Example of blacklist topic count stored in ZooKeeper:
   * Topics with blacktopic as prefix, and four streams: !4!blacktopic.*
   */

  val WHITELIST_MARKER = "*"
  val BLACKLIST_MARKER = "!"
  private val WHITELIST_PATTERN =
    Pattern.compile("""\*(\p{Digit}+)\*(.*)""")
  private val BLACKLIST_PATTERN =
    Pattern.compile("""!(\p{Digit}+)!(.*)""")

  def constructTopicCount(group: String,
                          consumerId: String,
                          zkClient: ZkClient) : TopicCount = {
    val dirs = new ZKGroupDirs(group)
    val topicCountString = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId)
    val hasWhitelist = topicCountString.startsWith(WHITELIST_MARKER)
    val hasBlacklist = topicCountString.startsWith(BLACKLIST_MARKER)

    if (hasWhitelist || hasBlacklist)
      info("Constructing topic count for %s from %s using %s as pattern."
        .format(consumerId, topicCountString,
          if (hasWhitelist) WHITELIST_PATTERN else BLACKLIST_PATTERN))

    if (hasWhitelist || hasBlacklist) {
      val matcher = if (hasWhitelist)
        WHITELIST_PATTERN.matcher(topicCountString)
      else
        BLACKLIST_PATTERN.matcher(topicCountString)
      require(matcher.matches())
      val numStreams = matcher.group(1).toInt
      val regex = matcher.group(2)
      val filter = if (hasWhitelist)
        new Whitelist(regex)
      else
        new Blacklist(regex)

      new WildcardTopicCount(zkClient, consumerId, filter, numStreams)
    }
    else {
      var topMap : Map[String,Int] = null
      try {
        SyncJSON.parseFull(topicCountString) match {
          case Some(m) => topMap = m.asInstanceOf[Map[String,Int]]
          case None => throw new RuntimeException("error constructing TopicCount : " + topicCountString)
        }
      }
      catch {
        case e =>
          error("error parsing consumer json string " + topicCountString, e)
          throw e
      }

      new StaticTopicCount(consumerId, topMap)
    }
  }

  def constructTopicCount(consumerIdString: String, topicCount: Map[String,  Int]) =
    new StaticTopicCount(consumerIdString, topicCount)

  def constructTopicCount(consumerIdString: String,
                          filter: TopicFilter,
                          numStreams: Int,
                          zkClient: ZkClient) =
    new WildcardTopicCount(zkClient, consumerIdString, filter, numStreams)

}

private[kafka] class StaticTopicCount(val consumerIdString: String,
                                val topicCountMap: Map[String, Int])
                                extends TopicCount {

  def getConsumerThreadIdsPerTopic =
    makeConsumerThreadIdsPerTopic(consumerIdString, topicCountMap)

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
   *    "topic2" : 4
   *  }
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
}

private[kafka] class WildcardTopicCount(zkClient: ZkClient,
                                        consumerIdString: String,
                                        topicFilter: TopicFilter,
                                        numStreams: Int) extends TopicCount {
  def getConsumerThreadIdsPerTopic = {
    val wildcardTopics = ZkUtils.getChildrenParentMayNotExist(
      zkClient, ZkUtils.BrokerTopicsPath).filter(topicFilter.isTopicAllowed(_))
    makeConsumerThreadIdsPerTopic(consumerIdString,
                                  Map(wildcardTopics.map((_, numStreams)): _*))
  }

  def dbString = {
    val marker = topicFilter match {
      case wl: Whitelist => TopicCount.WHITELIST_MARKER
      case bl: Blacklist => TopicCount.BLACKLIST_MARKER
    }

    "%s%d%s%s".format(marker, numStreams, marker, topicFilter.regex)
  }

}

