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
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.FetchRequest
import scala.collection.mutable

final class ChainFetchRequestValidator(requestValidators: List[FetchRequestValidator],
                                       partitionValidators: List[FetchPartitionValidator]) extends RequestValidator[(FetchRequest, mutable.Map[TopicPartition, FetchRequest.PartitionData]), FetchRequestValidation] with Logging {

  def validate(request: RequestChannel.Request,
               input: (FetchRequest, mutable.Map[TopicPartition, FetchRequest.PartitionData])): FetchRequestValidation = {
    var errors = Vector.empty[ErrorElem]
    requestValidators.foreach { requestValidator =>
      if (errors.isEmpty) {
        errors = requestValidator.validate(request, input)
      }
    }

    if (errors.isEmpty) {
      val erroneous = Vector.newBuilder[ErrorElem]
      val interesting = Vector.newBuilder[ValidElem]

      val (_, partitions) = input
      partitions.foreach { partition =>
        var someError = Option.empty[ErrorElem]
        partitionValidators.foreach { partitionValidator =>
          if (someError.isEmpty) {
            someError = partitionValidator.validate(request, partition)
          }
        }

        someError match {
          case Some(errorElem) => erroneous += errorElem
          case None => interesting += partition
        }
      }

      FetchRequestValidation(erroneous.result(), interesting.result())
    } else {
      FetchRequestValidation(errors, Vector.empty)
    }
  }
}

final object ChainFetchRequestValidator {
  def apply(requestValidators: List[FetchRequestValidator],
            partitionValidators: List[FetchPartitionValidator]): ChainFetchRequestValidator = {
    new ChainFetchRequestValidator(requestValidators, partitionValidators)
  }
}
