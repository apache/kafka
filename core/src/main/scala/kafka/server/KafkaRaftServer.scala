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
package kafka.server

import kafka.metrics.{KafkaMetricsReporter, KafkaYammerMetrics}
import kafka.raft.KafkaRaftManager
import kafka.server.KafkaRaftServer.{BrokerRole, ControllerRole}
import kafka.utils.{CoreUtils, Logging, Mx4jLoader, VerifiableProperties}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.{AppInfoParser, Time}
import org.apache.kafka.raft.internals.StringSerde

/**
 * This class implements the KIP-500 server which relies on a self-managed
 * Raft quorum for maintaining cluster metadata. It is responsible for
 * constructing the controller and/or broker based on the `process.roles`
 * configuration and for managing their basic lifecycle (startup and shutdown).
 *
 * Note that this server is a work in progress and relies on stubbed
 * implementations of the controller [[ControllerServer]] and broker
 * [[BrokerServer]].
 */
class KafkaRaftServer(
  config: KafkaConfig,
  time: Time,
  threadNamePrefix: Option[String]
) extends Server with Logging {

  KafkaMetricsReporter.startReporters(VerifiableProperties(config.originals))
  KafkaYammerMetrics.INSTANCE.configure(config.originals)

  private val metrics = Server.initializeMetrics(
    config,
    time,
    clusterId = "FIXME"
  )

  private val raftManager = new KafkaRaftManager(
    config,
    config.logDirs.head,
    new StringSerde,
    KafkaRaftServer.MetadataPartition,
    time,
    metrics
  )

  private val broker: Option[BrokerServer] = if (config.processRoles.contains(BrokerRole)) {
    Some(new BrokerServer())
  } else {
    None
  }

  private val controller: Option[ControllerServer] = if (config.processRoles.contains(ControllerRole)) {
    Some(new ControllerServer())
  } else {
    None
  }

  override def startup(): Unit = {
    Mx4jLoader.maybeLoad()
    raftManager.startup()
    controller.foreach(_.startup())
    broker.foreach(_.startup())
    AppInfoParser.registerAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics, time.milliseconds())
  }

  override def shutdown(): Unit = {
    broker.foreach(_.shutdown())
    raftManager.shutdown()
    controller.foreach(_.shutdown())
    CoreUtils.swallow(AppInfoParser.unregisterAppInfo(Server.MetricsPrefix, config.brokerId.toString, metrics), this)

  }

  override def awaitShutdown(): Unit = {
    broker.foreach(_.awaitShutdown())
    controller.foreach(_.awaitShutdown())
  }

}

object KafkaRaftServer {
  val MetadataTopic = "@metadata"
  val MetadataPartition = new TopicPartition(MetadataTopic, 0)

  sealed trait ProcessRole
  case object BrokerRole extends ProcessRole
  case object ControllerRole extends ProcessRole
}
