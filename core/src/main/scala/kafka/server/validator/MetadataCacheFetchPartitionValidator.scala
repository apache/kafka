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

package kafka.server.validator

import kafka.network.RequestChannel
import kafka.server.MetadataCache
import org.apache.kafka.common.protocol.Errors

final class MetadataCacheFetchPartitionValidator(metadataCache: MetadataCache) extends FetchPartitionValidator {
  override def validate(request: RequestChannel.Request,
                        partition: ValidElem): Option[ErrorElem] = {
    val (topic, data) = partition
    if (!metadataCache.contains(topic)) {
      Some(topic-> errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION))
    } else {
      None
    }
  }
}

final object MetadataCacheFetchPartitionValidator {
  def apply(metadata: MetadataCache): MetadataCacheFetchPartitionValidator = {
    new MetadataCacheFetchPartitionValidator(metadata)
  }
}
