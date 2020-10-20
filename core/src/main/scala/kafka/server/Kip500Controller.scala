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

import java.util.concurrent.locks.ReentrantLock

import kafka.metrics.{KafkaMetricsGroup, KafkaMetricsReporter}
import kafka.utils.Logging
import org.apache.kafka.common.utils.{LogContext, Time}

object Kip500Controller {
  sealed trait Kip500ControllerStatus
  case object SHUTDOWN extends Kip500ControllerStatus
  case object STARTING extends Kip500ControllerStatus
  case object STARTED extends Kip500ControllerStatus
  case object SHUTTING_DOWN extends Kip500ControllerStatus

  def validateConfig(config: KafkaConfig): Unit = {
    if (config.controllerId < 0) {
      throw new RuntimeException(s"You must configure ${KafkaConfig.ControllerIdProp} " +
        "in order to run the KIP-500 controller.")
    }
  }
}

/**
 * A KIP-500 Kafka controller.
 */
class Kip500Controller(val config: KafkaConfig,
                       val time: Time, 
                       val threadNamePrefix: Option[String],
                       val kafkaMetricsReporters: Seq[KafkaMetricsReporter])
                          extends Logging with KafkaMetricsGroup {
  import Kip500Controller._

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: Kip500ControllerStatus = SHUTDOWN

  private def maybeChangeStatus(from: Kip500ControllerStatus, to: Kip500ControllerStatus): Boolean = {
    lock.lock()
    try {
      if (status != from) return false
      status = to
      if (to == SHUTDOWN) awaitShutdownCond.signalAll()
    } finally {
      lock.unlock()
    }
    true
  }

  def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      info("starting")
      maybeChangeStatus(STARTING, STARTED)
      // TODO: initialize the log dir(s)
      //val logContext =
      new LogContext(s"[Kip500Controller id=${config.controllerId}] ")
      Kip500Controller.validateConfig(config)

    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during controller startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }

  def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
      info("shutting down")
    } catch {
      case e: Throwable =>
        fatal("Fatal error during controller shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  def awaitShutdown(): Unit = {
    lock.lock()
    try {
      while (true) {
        if (status == SHUTDOWN) return
        awaitShutdownCond.awaitUninterruptibly()
      }
    } finally {
      lock.unlock()
    }
  }
}
