/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.security.authorizer

import java.util.concurrent.CompletionStage
import java.{lang, util}
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.server.QuorumTestHarness
import kafka.zookeeper.ZooKeeperClient
import org.apache.kafka.common.Endpoint
import org.apache.kafka.common.acl._
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.authorizer._
import org.apache.zookeeper.client.ZKClientConfig
import org.junit.jupiter.api.{AfterEach, BeforeEach, TestInfo}

class AuthorizerInterfaceDefaultTest extends QuorumTestHarness with BaseAuthorizerTest {

  private val interfaceDefaultAuthorizer = new DelegateAuthorizer

  override def authorizer: Authorizer = interfaceDefaultAuthorizer

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    super.setUp(testInfo)

    // Increase maxUpdateRetries to avoid transient failures
    interfaceDefaultAuthorizer.authorizer.maxUpdateRetries = Int.MaxValue

    val props = TestUtils.createBrokerConfig(0, zkConnect)
    props.put(AclAuthorizer.SuperUsersProp, superUsers)

    config = KafkaConfig.fromProps(props)
    interfaceDefaultAuthorizer.authorizer.configure(config.originals)

    zooKeeperClient = new ZooKeeperClient(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkMaxInFlightRequests,
      Time.SYSTEM, "kafka.test", "AuthorizerInterfaceDefaultTest", new ZKClientConfig,
      "AuthorizerInterfaceDefaultTest")
  }

  @AfterEach
  override def tearDown(): Unit = {
    interfaceDefaultAuthorizer.close()
    zooKeeperClient.close()
    super.tearDown()
  }

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

}
