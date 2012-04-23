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

import kafka.utils.{ZkQueue, Logging}
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.CountDownLatch

class StateChangeCommandHandler(name: String, config: KafkaConfig, stateChangeQ: ZkQueue,
                                ensureStateChangeCommandValidityOnThisBroker: (StateChangeCommand) => Boolean,
                                ensureEpochValidity: (StateChangeCommand) => Boolean) extends Thread(name) with Logging {
  val isRunning: AtomicBoolean = new AtomicBoolean(true)
  private val shutdownLatch = new CountDownLatch(1)

  override def run() {
    try {
      while(isRunning.get()) {
        // get outstanding state change requests for this broker
        val command = stateChangeQ.take()
        val stateChangeCommand = StateChangeCommand.getStateChangeRequest(command._2)
        ensureStateChangeCommandValidityOnThisBroker(stateChangeCommand)

        stateChangeCommand match {
          case StartReplica(topic, partition, epoch) =>
            if(ensureEpochValidity(stateChangeCommand))
              handleStartReplica(topic, partition)
          case CloseReplica(topic, partition, epoch) =>
            /**
             * close replica requests are sent as part of delete topic or partition reassignment process
             * To ensure that a topic will be deleted even if the broker is offline, this state change should not
             * be protected with the epoch validity check
             */
            handleCloseReplica(topic, partition)
        }
        stateChangeQ.remove(command)
      }
    }catch {
      case e: InterruptedException => info("State change command handler interrupted. Shutting down")
      case e1 => error("Error in state change command handler. Shutting down due to ", e1)
    }
    shutdownComplete()
  }

  private def shutdownComplete() = shutdownLatch.countDown

  def shutdown() {
    isRunning.set(false)
    interrupt()
    shutdownLatch.await()
    info("State change command handler shutdown completed")
  }

  def handleStartReplica(topic: String, partition: Int) {
    info("Received start replica state change command for topic %s partition %d on broker %d"
      .format(topic, partition, config.brokerId))
    // TODO: implement this as part of create topic support or partition reassignment support. Until then, it is unused
  }

  def handleCloseReplica(topic: String, partition: Int) {
    info("Received close replica state change command for topic %s partition %d on broker %d"
      .format(topic, partition, config.brokerId))
    // TODO: implement this as part of delete topic support. Until then, it is unused
  }
}