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
import kafka.server.MetadataCache

/** Abstract validation processing for requests.
 *
 *  The type T represents the request specific type while V represent the request specific
 *  validation or result.
 */
trait RequestValidator[I, O] {
  /** Returns the result of validating a request
   *
   *  @param request request object
   *  @param input parametrized input for the validator
   *  @param validation parametrized result for the validator
   */
  def validate(request: RequestChannel.Request,
               input: I): O
}

final object RequestValidator {
  def fetch(metadataCache: MetadataCache,
            authorizer: Option[Authorizer]): ChainFetchRequestValidator = {
    ChainFetchRequestValidator(
      List(
        MaxBytesFetchRequestValidator(),
        AuthorizeFetchRequestValidator(authorizer)
      ),
      List(
        MaxBytesFetchPartitionValidator(),
        AuthorizeFetchPartitionValidator(authorizer),
        MetadataCacheFetchPartitionValidator(metadataCache)
      )
    )
  }
}
