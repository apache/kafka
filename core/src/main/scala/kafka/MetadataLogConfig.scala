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
package kafka.raft

import kafka.server.KafkaConfig
import org.apache.kafka.common.config.AbstractConfig
import org.apache.kafka.storage.internals.log.LogConfig

final case class MetadataLogConfig(
  logSegmentBytes: Int,
  logSegmentMinBytes: Int,
  logSegmentMillis: Long,
  retentionMaxBytes: Long,
  retentionMillis: Long,
  maxBatchSizeInBytes: Int,
  maxFetchSizeInBytes: Int,
  fileDeleteDelayMs: Long,
  nodeId: Int
)

object MetadataLogConfig {
  def apply(config: AbstractConfig, maxBatchSizeInBytes: Int, maxFetchSizeInBytes: Int): MetadataLogConfig = {
    new MetadataLogConfig(
      config.getInt(KafkaConfig.MetadataLogSegmentBytesProp),
      config.getInt(KafkaConfig.MetadataLogSegmentMinBytesProp),
      config.getLong(KafkaConfig.MetadataLogSegmentMillisProp),
      config.getLong(KafkaConfig.MetadataMaxRetentionBytesProp),
      config.getLong(KafkaConfig.MetadataMaxRetentionMillisProp),
      maxBatchSizeInBytes,
      maxFetchSizeInBytes,
      LogConfig.DEFAULT_FILE_DELETE_DELAY_MS,
      config.getInt(KafkaConfig.NodeIdProp)
    )
  }
}
