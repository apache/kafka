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

package kafka.log

import kafka.server.KafkaConfig

import org.apache.kafka.common.utils.Time
import org.apache.kafka.coordinator.transaction.{TransactionLogConfig, TransactionStateManagerConfig}
import org.apache.kafka.metadata.ConfigRepository
import org.apache.kafka.server.util.Scheduler
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogCleaner, LogConfig, LogDirFailureChannel, LogManager => JLogManager, ProducerStateManagerConfig}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats

import java.io.File
import java.util
import java.util.stream.Collectors


object LogManager {

  def apply(config: KafkaConfig,
            initialOfflineDirs: util.Set[String],
            configRepository: ConfigRepository,
            kafkaScheduler: Scheduler,
            time: Time,
            brokerTopicStats: BrokerTopicStats,
            logDirFailureChannel: LogDirFailureChannel): JLogManager = {
    val defaultProps = config.extractLogConfigMap

    LogConfig.validateBrokerLogConfigValues(defaultProps, config.remoteLogManagerConfig.isRemoteStorageSystemEnabled)
    val defaultLogConfig = new LogConfig(defaultProps)

    val cleanerConfig = new CleanerConfig(config)
    val transactionLogConfig = new TransactionLogConfig(config)

    new JLogManager(config.logDirs.stream.map(new File(_).getAbsoluteFile).collect(Collectors.toList()),
      initialOfflineDirs.stream.map(new File(_).getAbsoluteFile).collect(Collectors.toList()),
      configRepository,
      defaultLogConfig,
      cleanerConfig,
      config.numRecoveryThreadsPerDataDir,
      config.logFlushSchedulerIntervalMs,
      config.logFlushOffsetCheckpointIntervalMs,
      config.logFlushStartOffsetCheckpointIntervalMs,
      config.logCleanupIntervalMs,
      new TransactionStateManagerConfig(config).transactionMaxTimeoutMs,
      new ProducerStateManagerConfig(transactionLogConfig.producerIdExpirationMs, transactionLogConfig.transactionPartitionVerificationEnable),
      transactionLogConfig.producerIdExpirationCheckIntervalMs,
      kafkaScheduler,
      brokerTopicStats,
      logDirFailureChannel,
      time,
      config.remoteLogManagerConfig.isRemoteStorageSystemEnabled,
      config.logInitialTaskDelayMs,
      (cleanerConfig, files, map, logDirFailureChannel, time) => new LogCleaner(cleanerConfig, files, map, logDirFailureChannel, time))
  }
}
