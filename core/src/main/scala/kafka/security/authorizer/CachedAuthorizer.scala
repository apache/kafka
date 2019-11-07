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

package kafka.security.authorizer

import java.net.InetAddress

import kafka.network.RequestChannel.Session
import kafka.security.auth.{Acl, Operation, PermissionType, Resource, ResourceType}
import kafka.security.auth.{All, Allow, Alter, AlterConfigs, Delete, Deny, Describe, DescribeConfigs, Read, Write}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.security.auth.KafkaPrincipal

import scala.collection.concurrent.TrieMap
import org.apache.kafka.server.authorizer.AclDeleteResult.AclBindingDeleteResult
import org.apache.kafka.server.authorizer.{AclCreateResult, AclDeleteResult, Action, AuthorizableRequestContext, AuthorizationResult, Authorizer, AuthorizerServerInfo}


/**
  * Partial implementation of the Authorizer that implements a Cache
  * for performance improvement.
  * It is the responsibility of the classes that are extending CachedAuthorizer
  * to invalidate the cache with a call to `invalidateAuthorizerCache`
  */
abstract class CachedAuthorizer extends Authorizer {

  // we use a concurrent map to be thread safe, as the SimpleAclAuthorizer uses a lot of locks
  private val authorizerCache = TrieMap[Resource,TrieMap[(InetAddress, KafkaPrincipal, AclOperation), AuthorizationResult]]()

  /**
    * a call to authorize that will leverage the internal cache when possible
    * @param requestContext Request context interface that provides data from request header as well as connection and authentication information to plugins.
    * @param action Type of operation client is trying to perform on resource.
    * @return true if the operation should be permitted, false otherwise
    */
  def authorizeAction(requestContext: AuthorizableRequestContext, action: Action): AuthorizationResult = {
    val resource = AuthorizerUtils.convertToResource(action.resourcePattern)
    authorizerCache.get(resource) match {
      case None =>
        val authorized = authorizeActionUncached(requestContext, action)
        val authorizerCacheParam = TrieMap[(InetAddress, KafkaPrincipal, AclOperation), AuthorizationResult]()
        authorizerCacheParam.update( (requestContext.clientAddress,requestContext.principal, action.operation), authorized);
        authorizerCache.update(resource,authorizerCacheParam);
        authorized
      case Some(authorizedParam) => {
        authorizedParam.get(requestContext.clientAddress,requestContext.principal, action.operation) match {
          case None =>{
            val authorized = authorizeActionUncached(requestContext, action)
            authorizedParam.update((requestContext.clientAddress,requestContext.principal, action.operation), authorized);
            authorized
          }
          case Some(authorized)=>{
            authorized
          }
        }
      }
    }
  }

  /**
    * The result of this call will be cached.
    * Therefore the implementation can be "expensive" on the first call.
    * @param requestContext Request context interface that provides data from request header as well as connection and authentication information to plugins.
    * @param action Type of operation client is trying to perform on resource.
    * @return true if the operation should be permitted, false otherwise
    */
  def authorizeActionUncached(requestContext: AuthorizableRequestContext, action: Action): AuthorizationResult

  /**
    * remove the cache
    * @return unit
    */
  def removeResourceAuthorizerCache(resource: Resource): Unit = {
    authorizerCache.remove(resource)
  }
}