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

import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchResponse

import scala.jdk.CollectionConverters._

/**
 * the suites of helpers for KafkaApis class.
 * We don't use companion object since KafkaApis is too fat.
 */
object KafkaApisUtils {

  // Traffic from both in-sync and out of sync replicas are accounted for in replication quota to ensure total replication
  // traffic doesn't exceed quota.
  private[server] def sizeOfThrottledPartitions(versionId: Short,
                                                unconvertedResponse: FetchResponse[Records],
                                                quota: ReplicationQuotaManager): Int = {
    FetchResponse.sizeOf(versionId, unconvertedResponse.responseData.entrySet()
      .iterator().asScala.filter(element => quota.isThrottled(element.getKey)).asJava)
  }
}
