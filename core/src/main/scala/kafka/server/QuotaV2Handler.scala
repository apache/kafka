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

import java.util.concurrent.TimeUnit
import kafka.utils.{CoreUtils, Logging}
import org.apache.kafka.common.requests.{ProduceRequest, ProduceResponse}
import org.apache.kafka.common.{Configurable, TopicPartition}

object QuotaV2Decision extends Enumeration {
  val APPROVE, DENY = Value
}

/**
  * A class implementing QuotaV2Handler is used to APPROVE or DENY produce requests. The intention is to
  * limit the amount of data handled and stored, to avoid excessive usage of the Kafka cluster
  * either for availability or capacity reasons.
  */
trait QuotaV2Handler extends Configurable {

  /**
    * Concrete classes implementing checkLimit should return a APPROVE or DENY decision, but should
    * not charge a request against the quota until recordCharge is called.
    */
  def checkLimit(topicPartition: TopicPartition, request: ProduceRequest): QuotaV2Decision.Value

  /**
   * recordCharge is used to update the quota backend with the quota consumed by the request.
   * recordCharge should be called after an APPROVE decision is made.
   */
  def recordCharge(topicPartition: TopicPartition, request: ProduceRequest, response: ProduceResponse.PartitionResponse): Unit

  /**
    * Close the QuotaV2Handler with the given timeout.
    *
    * @param timeout the maximum time to wait to close the handler.
    * @param unit    the time unit.
    */
  def close(timeout: Long, unit: TimeUnit): Unit
}

object QuotaV2Handler extends Logging {
  /**
   * Create a new QuotaV2Handler from the given Kafka config.
   * @param config the Kafka configuration defining the QuotaV2Handler properties.
   * @return A configured instance of QuotaV2Handler.
   */
  def apply(config: KafkaConfig): QuotaV2Handler = {
    val quotaV2Handler = try {
      CoreUtils.createObject[QuotaV2Handler](config.QuotaV2HandlerClassName)
    } catch {
      case e: Exception =>
        error(s"Creating QuotaV2Handler instance from the given class name ${config.QuotaV2HandlerClassName} failed.", e)
        new NoOpQuotaV2Handler
    }
    quotaV2Handler.configure(config.originals())
    quotaV2Handler
  }
}


