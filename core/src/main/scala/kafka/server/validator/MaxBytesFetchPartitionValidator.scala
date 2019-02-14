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
import kafka.utils.Logging
import org.apache.kafka.common.protocol.Errors

final class MaxBytesFetchPartitionValidator extends FetchPartitionValidator with Logging {
  override def validate(request: RequestChannel.Request, partition: ValidElem): Option[ErrorElem] = {
    val (topic, data) = partition
    if (data.maxBytes < 0) {
      // Log why we are returning INVALID_REQUEST; documentation ask the user to read the broker logs
      warn(s"Invalid fetch from client `${request.header.clientId}` maximum bytes is negative for ${topic}: ${data.maxBytes}")
      Some(topic-> errorResponse(Errors.INVALID_REQUEST))
    } else {
      None
    }
  }
}

final object MaxBytesFetchPartitionValidator {
  def apply(): MaxBytesFetchPartitionValidator = {
    new MaxBytesFetchPartitionValidator()
  }
}
