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

import kafka.controller.{LogDirEventNotificationListener}
import scala.collection.{Map, Seq, Set}

object DiskUtils extends Logging {

  private val LogDirEventNotificationPrefix = "log_dir_event_"
  val LogDirFailureEvent = 1

  def propagateLogDirEvent(zkUtils: ZkUtils, brokerId: Int) {
    val logDirEventNotificationPath: String = zkUtils.createSequentialPersistentPath(
      ZkUtils.LogDirEventNotificationPath + "/" + LogDirEventNotificationPrefix, logDirFailureEventZkData(brokerId))
    debug("Added " + logDirEventNotificationPath + " for broker " + brokerId)
  }

  private def logDirFailureEventZkData(brokerId: Int): String = {
    Json.encode(Map("version" -> LogDirEventNotificationListener.version, "broker" -> brokerId, "event" -> LogDirFailureEvent))
  }

  def getBrokerIdFromLogDirEvent(zkUtils: ZkUtils, child: String): Option[Int] = {
    val changeZnode = ZkUtils.LogDirEventNotificationPath + "/" + child
    val (jsonOpt, stat) = zkUtils.readDataMaybeNull(changeZnode)
    if (jsonOpt.isDefined) {
      val json = Json.parseFull(jsonOpt.get)

      json match {
        case Some(m) =>
          val brokerAndTopics = m.asInstanceOf[Map[String, Any]]
          val brokerId = brokerAndTopics.get("broker").get.asInstanceOf[Int]
          Some(brokerId)
        case None =>
          error("Invalid topic and partition JSON: " + jsonOpt.get + " in ZK: " + changeZnode)
          None
      }
    } else {
      None
    }
  }

}
