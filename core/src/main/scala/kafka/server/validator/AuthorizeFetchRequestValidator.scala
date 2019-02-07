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
import kafka.security.auth.Authorizer
import kafka.security.auth.ClusterAction
import kafka.security.auth.Operation
import kafka.security.auth.Read
import kafka.security.auth.Resource
import kafka.security.auth.Topic
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchRequest
import org.apache.kafka.common.resource.PatternType.LITERAL

final class AuthorizeFetchRequestValidator(authorizer: Option[Authorizer]) extends Validator[FetchRequest, FetchRequestValidation] {
  import FetchRequestValidation._

  private[this] def authorize(session: RequestChannel.Session,
                              operation: Operation,
                              resource: Resource): Boolean = {
    authorizer.forall(_.authorize(session, operation, resource))
  }

  override def validate(request: RequestChannel.Request,
                        fetchRequest: FetchRequest,
                        validation: FetchRequestValidation): FetchRequestValidation = {
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authorize(request.session, ClusterAction, Resource.ClusterResource)) {
        validation
      } else {
        val erroneous = validation.interesting.map { case (part, _) =>
          (part -> errorResponse[Records](Errors.TOPIC_AUTHORIZATION_FAILED))
        }

        FetchRequestValidation(erroneous ++ validation.erroneous, Vector.empty)
      }
    } else {
      // Regular Kafka consumers need READ permission on each partition they are fetching.
      val erroneous = Vector.newBuilder[ErrorElem]
      erroneous ++= validation.erroneous
      val interesting = Vector.newBuilder[ValidElem]

      validation.interesting.foreach { case (topicPartition, data) =>
        if (!authorize(request.session, Read, Resource(Topic, topicPartition.topic, LITERAL)))
          erroneous += (topicPartition -> errorResponse(Errors.TOPIC_AUTHORIZATION_FAILED))
        else
          interesting += (topicPartition -> data)
      }

      FetchRequestValidation(erroneous.result(), interesting.result())
    }
  }
}

final object AuthorizeFetchRequestValidator {
  def apply(authorizer: Option[Authorizer]): AuthorizeFetchRequestValidator = {
    new AuthorizeFetchRequestValidator(authorizer)
  }
}
