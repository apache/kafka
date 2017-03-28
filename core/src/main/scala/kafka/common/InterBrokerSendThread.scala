/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.common

import java.util.concurrent.BlockingQueue

import kafka.cluster.{Broker, BrokerEndPoint}
import kafka.utils.ShutdownableThread
import org.apache.kafka.clients.NetworkClient
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.AbstractResponse

import scala.collection.mutable.HashMap

object InterBrokerSendThread {

}

/**
 *  Abstract class for inter-broker send thread that utilize a non-blocking network client.
 */
abstract class InterBrokerSendThread(name: String,
                                     sourceBroker: BrokerEndPoint,
                                     isInterruptible: Boolean = true)
  extends ShutdownableThread(name, isInterruptible) {

  private val brokerStateInfo: HashMap[Int, ] = new HashMap[Int, ControllerBrokerStateInfo]
  private val brokerLock = new Object

  /* callbacks to be defined in subclass */

  // process response
  def processResponse(response: AbstractResponse)

  def addBroker(broker: Broker) {
    // be careful here. Maybe the startup() API has already started the request send thread
    brokerLock synchronized {
      if(!brokerStateInfo.contains(broker.id)) {
        addNewBroker(broker)
        startRequestSendThread(broker.id)
      }
    }
  }

  def removeBroker(brokerId: Int) {
    brokerLock synchronized {
      removeExistingBroker(brokerStateInfo(brokerId))
    }
  }

  override def doWork() {

    val fetchRequest = inLock(partitionMapLock) {
      val fetchRequest = buildFetchRequest(partitionStates.partitionStates.asScala.map { state =>
        state.topicPartition -> state.value
      })
      if (fetchRequest.isEmpty) {
        trace("There are no active partitions. Back off for %d ms before sending a fetch request".format(fetchBackOffMs))
        partitionMapCond.await(fetchBackOffMs, TimeUnit.MILLISECONDS)
      }
      fetchRequest
    }
    if (!fetchRequest.isEmpty)
      processFetchRequest(fetchRequest)
  }

  override def shutdown(){
    initiateShutdown()
    inLock(partitionMapLock) {
      partitionMapCond.signalAll()
    }
    awaitShutdown()

    // we don't need the lock since the thread has finished shutdown and metric removal is safe
    fetcherStats.unregister()
    fetcherLagStats.unregister()
  }
}

case class ControllerBrokerStateInfo(destBrokerNode: Node,
                                     messageQueue: BlockingQueue[QueueItem])