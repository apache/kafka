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

package kafka.server

import util.parsing.json.JSON
import kafka.common.KafkaException
import kafka.utils.{Utils, Logging}
import collection.mutable.HashMap

object StateChangeCommand extends Logging {
  val State = "state"
  val Topic = "topic"
  val Partition = "partition"
  val Epoch = "epoch"
  val StartReplica = "start-replica"
  val CloseReplica = "close-replica"

  def getStateChangeRequest(requestJson: String): StateChangeCommand = {
    var topMap : Map[String, String] = null
    try {
      JSON.parseFull(requestJson) match {
        case Some(m) =>
          topMap = m.asInstanceOf[Map[String, String]]
          val topic = topMap.get(StateChangeCommand.Topic).getOrElse(null)
          val partition = topMap.get(StateChangeCommand.Partition).getOrElse("-1").toInt
          val epoch = topMap.get(StateChangeCommand.Epoch).getOrElse("-1").toInt
          val requestOpt = topMap.get(StateChangeCommand.State)
          requestOpt match {
            case Some(request) =>
              request match {
                case StartReplica => new StartReplica(topic, partition, epoch)
                case CloseReplica => new CloseReplica(topic, partition, epoch)
                case _ => throw new KafkaException("Unknown state change request " + request)
              }
            case None =>
              throw new KafkaException("Illegal state change request JSON " + requestJson)
          }
        case None => throw new RuntimeException("Error parsing state change request : " + requestJson)
      }
    } catch {
      case e =>
        error("Error parsing state change request JSON " + requestJson, e)
        throw e
    }
  }
}

sealed trait StateChangeCommand extends Logging {
  def state: String

  def topic: String

  def partition: Int

  def epoch: Int

  def toJson(): String = {
    val jsonMap = new HashMap[String, String]
    jsonMap.put(StateChangeCommand.State, state)
    jsonMap.put(StateChangeCommand.Topic, topic)
    jsonMap.put(StateChangeCommand.Partition, partition.toString)
    jsonMap.put(StateChangeCommand.Epoch, epoch.toString)
    Utils.stringMapToJsonString(jsonMap)
  }
}

/* The elected leader sends the start replica state change request to all the new replicas that have been assigned
* a partition. Note that the followers must act on this request only if the request epoch == latest partition epoch or -1 */
case class StartReplica(val topic: String, partition: Int, epoch: Int) extends StateChangeCommand {
  val state: String = StateChangeCommand.StartReplica
}

/* The elected leader sends the close replica state change request to all the replicas that have been un-assigned a partition
*  OR if a topic has been deleted. Note that the followers must act on this request even if the epoch has changed */
case class CloseReplica(topic: String, partition: Int, epoch: Int) extends StateChangeCommand {
  val state: String = StateChangeCommand.CloseReplica
}
