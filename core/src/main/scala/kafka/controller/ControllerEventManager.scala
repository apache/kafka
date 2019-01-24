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
import java.util.concurrent.locks.ReentrantLock

import com.yammer.metrics.core.Gauge
import kafka.metrics.{KafkaMetricsGroup, KafkaTimer}
import kafka.utils.CoreUtils.inLock
import kafka.utils.ShutdownableThread
import org.apache.kafka.common.errors.ControllerMovedException
import org.apache.kafka.common.utils.Time

import scala.collection._
import scala.collection.JavaConverters._

object ControllerEventManager {
  val ControllerEventThreadName = "controller-event-thread"
}
class ControllerEventManager(controllerId: Int, rateAndTimeMetrics: Map[ControllerState, KafkaTimer],
                             eventProcessedListener: ControllerEvent => Unit,
                             controllerMovedListener: () => Unit) extends KafkaMetricsGroup {

  @volatile private var _state: ControllerState = ControllerState.Idle
  private val putLock = new ReentrantLock()
  private val queue = new LinkedBlockingQueue[ControllerEvent]
  // Visible for test
  private[controller] val thread = new ControllerEventThread(ControllerEventManager.ControllerEventThreadName)
  private val time = Time.SYSTEM

  private val eventQueueTimeHist = newHistogram("EventQueueTimeMs")

  newGauge(
    "EventQueueSize",
    new Gauge[Int] {
      def value: Int = {
        queue.size()
      }
    }
  )


  def state: ControllerState = _state

  def start(): Unit = thread.start()

  def close(): Unit = {
    clearAndPut(KafkaController.ShutdownEventThread)
    thread.awaitShutdown()
  }

  def put(event: ControllerEvent): Unit = inLock(putLock) {
    queue.put(event)
  }

  def clearAndPut(event: ControllerEvent): Unit = inLock(putLock) {
    queue.asScala.foreach(evt =>
      if (evt.isInstanceOf[PreemptableControllerEvent])
        evt.asInstanceOf[PreemptableControllerEvent].preempt()
    )
    queue.clear()
    put(event)
  }

  class ControllerEventThread(name: String) extends ShutdownableThread(name = name, isInterruptible = false) {
    logIdent = s"[ControllerEventThread controllerId=$controllerId] "

    override def doWork(): Unit = {
      queue.take() match {
        case KafkaController.ShutdownEventThread => initiateShutdown()
        case controllerEvent =>
          _state = controllerEvent.state

          eventQueueTimeHist.update(time.milliseconds() - controllerEvent.enqueueTimeMs)

          try {
            rateAndTimeMetrics(state).time {
              controllerEvent.process()
            }
          } catch {
            case e: ControllerMovedException =>
              info(s"Controller moved to another broker when processing $controllerEvent.", e)
              controllerMovedListener()
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

}
