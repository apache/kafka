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

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}

import java.util
import java.util.concurrent.TimeUnit

/**
 * A NoOp handler instantiated by default when no other quota handler is being used.
 */
class NoOpQuotaV2Handler extends QuotaV2Handler {
  override def configure(configs: util.Map[String, _]): Unit = ()
  override def checkLimit(topicPartitions: TopicPartition, request: ProduceRequest): QuotaV2Decision.Value = QuotaV2Decision.APPROVE
  override def recordCharge(topicPartitions: TopicPartition, request: ProduceRequest, response: ProduceResponse.PartitionResponse): Unit = ()
  override def close(timeout: Long, unit: TimeUnit): Unit = ()
}
