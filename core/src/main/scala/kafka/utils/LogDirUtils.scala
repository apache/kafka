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

import kafka.controller.LogDirEventNotificationHandler
import kafka.zk.KafkaZkClient
import scala.collection.Map

object LogDirUtils extends Logging {

  private val LogDirEventNotificationPrefix = "log_dir_event_"
  val LogDirFailureEvent = 1

  def propagateLogDirEvent(zkClient: KafkaZkClient, brokerId: Int) {
    val logDirEventNotificationPath: String = zkClient.createSequentialPersistentPath(
      ZkUtils.LogDirEventNotificationPath + "/" + LogDirEventNotificationPrefix, logDirFailureEventZkData(brokerId))
    debug(s"Added $logDirEventNotificationPath for broker $brokerId")
  }

  private def logDirFailureEventZkData(brokerId: Int): String = {
    Json.encode(Map("version" -> LogDirEventNotificationHandler.Version, "broker" -> brokerId, "event" -> LogDirFailureEvent))
  }

}
