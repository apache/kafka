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
import org.apache.kafka.common.requests.FetchRequest

final class MetadataCacheFetchRequestValidator(metadataCache: MetadataCache) extends Validator[FetchRequest, FetchRequestValidation] {
  import FetchRequestValidation._

  override def validate(request: RequestChannel.Request,
                        fetchRequest: FetchRequest,
                        validation: FetchRequestValidation): FetchRequestValidation = {

    val erroneous = Vector.newBuilder[ErrorElem]
    erroneous ++= validation.erroneous
    val interesting = Vector.newBuilder[ValidElem]

    validation.interesting.foreach { case (topicPartition, data) =>
      if (!metadataCache.contains(topicPartition)) {
        erroneous += (topicPartition -> errorResponse(Errors.UNKNOWN_TOPIC_OR_PARTITION))
      } else {
        interesting += (topicPartition -> data)
      }
    }

    FetchRequestValidation(erroneous.result(), interesting.result())
  }
}

final object MetadataCacheFetchRequestValidator {
  def apply(metadata: MetadataCache): MetadataCacheFetchRequestValidator = {
    new MetadataCacheFetchRequestValidator(metadata)
  }
}
