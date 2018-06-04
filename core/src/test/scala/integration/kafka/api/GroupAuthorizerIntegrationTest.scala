/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package kafka.api

import java.util.Properties

import kafka.api.GroupAuthorizerIntegrationTest._
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.auth.{AuthenticationContext, KafkaPrincipal, KafkaPrincipalBuilder}


object GroupAuthorizerIntegrationTest {
  val GroupPrincipalType = "Group"
  val TestGroupPrincipal = new KafkaPrincipal(GroupPrincipalType, "testGroup")
  class GroupPrincipalBuilder extends KafkaPrincipalBuilder {
    override def build(context: AuthenticationContext): KafkaPrincipal = {
      TestGroupPrincipal
    }
  }
}

class GroupAuthorizerIntegrationTest extends AuthorizerIntegrationTest {
  override val kafkaPrincipalType = GroupPrincipalType
  override def userPrincipal = TestGroupPrincipal

  override def propertyOverrides(properties: Properties): Unit = {
    properties.setProperty(BrokerSecurityConfigs.PRINCIPAL_BUILDER_CLASS_CONFIG,
      classOf[GroupPrincipalBuilder].getName)
    super.propertyOverrides(properties)
  }
}