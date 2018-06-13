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

package kafka.consumer

import org.apache.kafka.common.header.Headers
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.{RecordBatch, TimestampType}

@deprecated("This class has been deprecated and will be removed in a future release. " +
  "Please use org.apache.kafka.clients.consumer.ConsumerRecord instead.", "0.11.0.0")
case class BaseConsumerRecord(topic: String,
                              partition: Int,
                              offset: Long,
                              timestamp: Long = RecordBatch.NO_TIMESTAMP,
                              timestampType: TimestampType = TimestampType.NO_TIMESTAMP_TYPE,
                              key: Array[Byte],
                              value: Array[Byte],
                              headers: Headers = new RecordHeaders())
