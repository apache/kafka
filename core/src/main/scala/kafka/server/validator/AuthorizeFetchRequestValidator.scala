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
import kafka.security.auth.Resource
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.Records
import org.apache.kafka.common.requests.FetchRequest
import scala.collection.breakOut
import scala.collection.mutable

final class AuthorizeFetchRequestValidator(authorizer: Option[Authorizer]) extends FetchRequestValidator {
  import AuthorizeFetchRequestValidator.authorize

  override def validate(request: RequestChannel.Request,
                        input: (FetchRequest, mutable.Map[TopicPartition, FetchRequest.PartitionData])): Vector[ErrorElem] = {
    val (fetchRequest, partitions) = input
    if (fetchRequest.isFromFollower) {
      // The follower must have ClusterAction on ClusterResource in order to fetch partition data.
      if (authorize(authorizer, request.session, ClusterAction, Resource.ClusterResource)) {
        Vector.empty
      } else {
        partitions.map { case (topic, _) =>
          topic -> errorResponse[Records](Errors.TOPIC_AUTHORIZATION_FAILED)
        }(breakOut)
      }
    } else {
      Vector.empty
    }
  }
}

final object AuthorizeFetchRequestValidator {
  def apply(authorizer: Option[Authorizer]): AuthorizeFetchRequestValidator = {
    new AuthorizeFetchRequestValidator(authorizer)
  }

  def authorize(authorizer: Option[Authorizer],
                session: RequestChannel.Session,
                operation: Operation,
                resource: Resource): Boolean = {
    authorizer.forall(_.authorize(session, operation, resource))
  }
}
