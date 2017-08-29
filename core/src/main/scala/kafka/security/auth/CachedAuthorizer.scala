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

package kafka.security.auth

import kafka.network.RequestChannel.Session

import scala.collection.concurrent.TrieMap


/**
  * Partial implementation of the Authorizer that implements a Cache
  * for performance improvement.
  * It is the responsibility of the classes that are extending CachedAuthorizer
  * to invalidate the cache with a call to `invalidateAuthorizerCache`
  */
abstract class CachedAuthorizer extends Authorizer {

  // we use a concurrent map to be thread safe, as the SimpleAclAuthorizer uses a lot of locks
  private val authorizerCache = TrieMap[(Session, Operation, Resource), Boolean]()

  /**
    * a call to authorize that will leverage the internal cache when possible
    * @param session The session being authenticated.
    * @param operation Type of operation client is trying to perform on resource.
    * @param resource Resource the client is trying to access.
    * @return true if the operation should be permitted, false otherwise
    */
  def authorize(session: Session, operation: Operation, resource: Resource): Boolean = {
    authorizerCache.get(session, operation, resource) match {
      case None =>
        val authorized = authorizeUncached(session, operation, resource)
        authorizerCache.update((session, operation, resource), authorized)
        authorized
      case Some(authorized) => authorized
    }
  }

  /**
    * The result of this call will be cached.
    * Therefore the implementation can be "expensive" on the first call.
    * @param session The session being authenticated.
    * @param operation Type of operation client is trying to perform on resource.
    * @param resource Resource the client is trying to access.
    * @return true if the operation should be permitted, false otherwise
    */
  def authorizeUncached(session: Session, operation: Operation, resource: Resource): Boolean

  /**
    * Invalidates the cache
    * @return unit
    */
  def invalidateAuthorizerCache(): Unit = {
    authorizerCache.clear()
  }

}
