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

import kafka.metrics.KafkaMetricsReporter
import kafka.server.metadata.BrokerMetadataListener
import org.apache.kafka.common.utils.{LogContext, Time}
import org.apache.kafka.controller.{LocalLogManager, MetaLogManager}
import org.apache.kafka.metadata.BrokerState

/**
 * A KIP-500 Kafka broker.
 */
class Kip500Broker(config: KafkaConfig,
                   time: Time,
                   threadNamePrefix: Option[String],
                   kafkaMetricsReporters: Seq[KafkaMetricsReporter]) extends KafkaBroker {
  import kafka.server.KafkaServerManager._

  val lock = new ReentrantLock()
  val awaitShutdownCond = lock.newCondition()
  var status: ProcessStatus = KafkaServerManager.SHUTDOWN
  var brokerMetadataListener: BrokerMetadataListener = null
  var metaLogManager: MetaLogManager = null

  private def maybeChangeStatus(from: ProcessStatus, to: ProcessStatus): Boolean = {
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

  override def startup(): Unit = {
    if (!maybeChangeStatus(SHUTDOWN, STARTING)) return
    try {
      // Start the broker metadata change listener
      // FIXME: Replace placeholders w/ actual instances
      //        Should be a merge conflict here for you @Ron
      // DEPENDS ON: github.com/confluentinc/kafka/pull/44
      brokerMetadataListener = new BrokerMetadataListener(
        config, time,
        BrokerMetadataListener.defaultProcessors(
          config,
          "CLUSTER_ID_PLACEHOLDER",
          null,
          null,
          null,
          null,
          null,
          null))
      brokerMetadataListener.start()

      // Initialize the metadata log manager
      // TODO: Replace w/ the raft log implementation
      metaLogManager = new LocalLogManager(new LogContext(),
        config.controllerId, config.metadataLogDir, "log-manager")
      metaLogManager.initialize(brokerMetadataListener)

      maybeChangeStatus(STARTING, STARTED)
    } catch {
      case e: Throwable =>
        maybeChangeStatus(STARTING, STARTED)
        fatal("Fatal error during controller startup. Prepare to shutdown", e)
        shutdown()
        throw e
    }
  }

  override def shutdown(): Unit = {
    if (!maybeChangeStatus(STARTED, SHUTTING_DOWN)) return
    try {
    } catch {
      case e: Throwable =>
        fatal("Fatal error during broker shutdown.", e)
        throw e
    } finally {
      maybeChangeStatus(SHUTTING_DOWN, SHUTDOWN)
    }
  }

  override def awaitShutdown(): Unit = {
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

  override def metrics() = null

  override def currentState(): BrokerState = null

  override def clusterId(): String = null
}
