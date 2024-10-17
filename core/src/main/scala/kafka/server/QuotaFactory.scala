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

import kafka.utils.Logging
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.server.quota.{ClientQuotaCallback, QuotaType}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.config.{ClientQuotaManagerConfig, QuotaConfig, ReplicationQuotaManagerConfig}


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

    val clientQuotaCallback = Option(cfg.getConfiguredInstance(QuotaConfig.CLIENT_QUOTA_CALLBACK_CLASS_CONFIG,
      classOf[ClientQuotaCallback]))
    QuotaManagers(
      new ClientQuotaManager(clientConfig(cfg), metrics, QuotaType.FETCH, time, threadNamePrefix, clientQuotaCallback),
      new ClientQuotaManager(clientConfig(cfg), metrics, QuotaType.PRODUCE, time, threadNamePrefix, clientQuotaCallback),
      new ClientRequestQuotaManager(clientConfig(cfg), metrics, time, threadNamePrefix, clientQuotaCallback),
      new ControllerMutationQuotaManager(clientControllerMutationConfig(cfg), metrics, time,
        threadNamePrefix, clientQuotaCallback),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, QuotaType.LEADER_REPLICATION, time),
      new ReplicationQuotaManager(replicationConfig(cfg), metrics, QuotaType.FOLLOWER_REPLICATION, time),
      new ReplicationQuotaManager(alterLogDirsReplicationConfig(cfg), metrics, QuotaType.ALTER_LOG_DIRS_REPLICATION, time),
      clientQuotaCallback
    )
  }

  def clientConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    new ClientQuotaManagerConfig(
      cfg.quotaConfig.numQuotaSamples,
      cfg.quotaConfig.quotaWindowSizeSeconds
    )
  }

  private def clientControllerMutationConfig(cfg: KafkaConfig): ClientQuotaManagerConfig = {
    new ClientQuotaManagerConfig(
      cfg.quotaConfig.numControllerQuotaSamples,
      cfg.quotaConfig.controllerQuotaWindowSizeSeconds
    )
  }

  private def replicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    new ReplicationQuotaManagerConfig(
      cfg.quotaConfig.numReplicationQuotaSamples,
      cfg.quotaConfig.replicationQuotaWindowSizeSeconds
    )
  }

  private def alterLogDirsReplicationConfig(cfg: KafkaConfig): ReplicationQuotaManagerConfig = {
    new ReplicationQuotaManagerConfig(
      cfg.quotaConfig.numAlterLogDirsReplicationQuotaSamples,
      cfg.quotaConfig.alterLogDirsReplicationQuotaWindowSizeSeconds
    )
  }

}
