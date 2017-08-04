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

package kafka.controller

import java.util.concurrent.LinkedBlockingQueue

import scala.collection._

import kafka.metrics.KafkaTimer
import kafka.utils.ShutdownableThread

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
}
class ControllerEventManager(rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventProcessedListener: ControllerEvent => Unit) {

  @volatile private var _state: ControllerState = ControllerState.Idle

  private val queue = new LinkedBlockingQueue[ControllerEvent]
  private val thread = new ControllerEventThread(ControllerEventManager.ControllerEventThreadName)

  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = thread.shutdown()

  def put(event: ControllerEvent): Unit = queue.put(event)

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name) {
    override def doWork(): Unit = {
      val controllerEvent = queue.take()
      _state = controllerEvent.state

      try {
        rateAndTimeMetrics(state).time {
          controllerEvent.process()
        }
      } catch {
        case e: Throwable => error(s"Error processing event $controllerEvent", e)
      }

      try eventProcessedListener(controllerEvent)
      catch {
        case e: Throwable => error(s"Error while invoking listener for processed event $controllerEvent", e)
      }

      _state = ControllerState.Idle
    }
  }

}
