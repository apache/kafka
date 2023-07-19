/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.server

import kafka.server.QuotaType._
import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.quota.ClientQuotaCallback
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.quota.ClientQuotaType

object QuotaType  {
  case object Fetch extends QuotaType
  case object Produce extends QuotaType
  case object Request extends QuotaType
  case object ControllerMutation extends QuotaType
  case object LeaderReplication extends QuotaType
  case object FollowerReplication extends QuotaType
  case object AlterLogDirsReplication extends QuotaType

  def toClientQuotaType(quotaType: QuotaType): ClientQuotaType = {
    quotaType match {
      case QuotaType.Fetch => ClientQuotaType.FETCH
      case QuotaType.Produce => ClientQuotaType.PRODUCE
      case QuotaType.Request => ClientQuotaType.REQUEST
      case QuotaType.ControllerMutation => ClientQuotaType.CONTROLLER_MUTATION
      case _ => throw new IllegalArgumentException(s"Not a client quota type: $quotaType")
    }
  }
}

sealed trait QuotaType

object QuotaFactory extends Logging {

  object UnboundedQuota extends ReplicaQuota {
    override def isThrottled(topicPartition: TopicPartition): Boolean = false
    override def isQuotaExceeded: Boolean = false
    def record(value: Long): Unit = ()
  }

  case class QuotaManagers(fetch: ClientQuotaManager,
                           produce: ClientQuotaManager,
                           request: ClientRequestQuotaManager,
                           controllerMutation: ControllerMutationQuotaManager,
                           leader: ReplicationQuotaManager,
                           follower: ReplicationQuotaManager,
                           alterLogDirs: ReplicationQuotaManager,
                           clientQuotaCallback: Option[ClientQuotaCallback]) {
    def shutdown(): Unit = {
      fetch.shutdown()
      produce.shutdown()
      request.shutdown()
      controllerMutation.shutdown()
      clientQuotaCallback.foreach(_.close())
    }
  }

  def instantiate(cfg: KafkaConfig, metrics: Metrics, time: Time, threadNamePrefix: String): QuotaManagers = {

    val clientQuotaCallback = Option(cfg.getConfiguredInstance(KafkaConfig.ClientQuotaCallbackClassProp,
      classOf[ClientQuotaCallback]))
    QuotaManagers(
      new ClientQuotaManager(clientConfig(cfg), metrics, Fetch, time, threadNamePrefix, clientQuotaCallback),
      new ClientQuotaManager(clientConfig(cfg), metrics, Produce, time, threadNamePrefix, clientQuotaCallback),
      new ClientRequestQuotaManager(clientConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallback),
      new ControllerMutationQuotaManager(clientControllerMutationConfig(cfg), metrics, time,
        threadNamePrefix, clientQuotaCallback),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, LeaderReplication, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, FollowerReplication, time),
      new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, AlterLogDirsReplication, time),
      clientQuotaCallback
    )
  }

  def clientConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    ClientQuotaManagerConfig(
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )
  }

  def clientControllerMutationConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    ClientQuotaManagerConfig(
      numQuotaSamples = cfg.numControllerQuotaSamples,
      quotaWindowSizeSeconds = cfg.controllerQuotaWindowSizeSeconds
    )
  }

  def replicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds
    )
  }

  def alterLogDirsReplicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numAlterLogDirsReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.alterLogDirsReplicationQuotaWindowSizeSeconds
    )
  }

}
