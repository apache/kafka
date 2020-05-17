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
import kafka.security.auth.{Authorizer => LegacyAuthorizer}
import org.apache.kafka.common.acl._
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.resource.Resource
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.authorizer.{AuthorizableRequestContext, Authorizer}

import scala.annotation.nowarn


object AuthorizerUtils {

  @nowarn("cat=deprecation")
  def createAuthorizer(className: String): Authorizer = {
    Utils.newInstance(className, classOf[Object]) match {
      case auth: Authorizer => auth
      case auth: kafka.security.auth.Authorizer => new AuthorizerWrapper(auth)
      case _ => throw new ConfigException(s"Authorizer does not implement ${classOf[Authorizer].getName} or ${classOf[LegacyAuthorizer].getName}.")
    }
  }

  def validateAclBinding(aclBinding: AclBinding): Unit = {
    if (aclBinding.isUnknown)
      throw new IllegalArgumentException("ACL binding contains unknown elements")
  }

  def isClusterResource(name: String): Boolean = name.equals(Resource.CLUSTER_NAME)

  def sessionToRequestContext(session: Session): AuthorizableRequestContext = {
    new AuthorizableRequestContext {
      override def clientId(): String = ""
      override def requestType(): Int = -1
      override def listenerName(): String = ""
      override def clientAddress(): InetAddress = session.clientAddress
      override def principal(): KafkaPrincipal = session.principal
      override def securityProtocol(): SecurityProtocol = null
      override def correlationId(): Int = -1
      override def requestVersion(): Int = -1
    }
  }
}
