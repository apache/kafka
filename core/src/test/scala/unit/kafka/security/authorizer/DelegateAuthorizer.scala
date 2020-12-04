/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
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

import java.{lang, util}
import java.util.concurrent.CompletionStage

import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl.{AclBinding, AclBindingFilter}
import org.apache.kafka.server.authorizer.{AclCreateResult, AclDeleteResult, Action, AuthorizableRequestContext, AuthorizationResult, Authorizer, AuthorizerServerInfo}

/**
 * For testing the interface default
 */
class DelegateAuthorizer extends Authorizer {
    val authorizer = new AclAuthorizer

    override def start(serverInfo: AuthorizerServerInfo): util.Map[Endpoint, _ <: CompletionStage[Void]] = {
        authorizer.start(serverInfo)
    }

    override def authorize(requestContext: AuthorizableRequestContext, actions: util.List[Action]): util.List[AuthorizationResult] = {
        authorizer.authorize(requestContext, actions)
    }

    override def createAcls(requestContext: AuthorizableRequestContext, aclBindings: util.List[AclBinding]): util.List[_ <: CompletionStage[AclCreateResult]] = {
        authorizer.createAcls(requestContext, aclBindings)
    }

    override def deleteAcls(requestContext: AuthorizableRequestContext, aclBindingFilters: util.List[AclBindingFilter]): util.List[_ <: CompletionStage[AclDeleteResult]] = {
        authorizer.deleteAcls(requestContext, aclBindingFilters)
    }

    override def acls(filter: AclBindingFilter): lang.Iterable[AclBinding] = {
        authorizer.acls(filter)
    }

    override def configure(configs: util.Map[String, _]): Unit = {
        authorizer.configure(configs)
    }

    override def close(): Unit = {
        authorizer.close()
    }
}