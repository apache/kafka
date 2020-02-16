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
package unit.kafka.admin

import java.net.InetAddress

import kafka.admin.AclCommand
import kafka.security.authorizer.AclAuthorizer
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.acl.AclOperation.READ
import org.apache.kafka.common.network.{ClientInformation, ListenerName}
import org.apache.kafka.common.protocol.ApiKeys
import org.apache.kafka.common.requests.{RequestContext, RequestHeader}
import org.apache.kafka.common.resource.PatternType.LITERAL
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.resource.ResourceType.TOPIC
import org.apache.kafka.common.security.auth.{KafkaPrincipal, SecurityProtocol}
import org.apache.kafka.server.authorizer.{Action, AuthorizationResult}
import org.junit.Assert.{assertFalse, assertTrue}
import org.junit.{After, Before, Test}
import org.apache.kafka.common.resource.PatternType
import scala.collection.JavaConverters._

class AclCommandLoadCacheTest extends ZooKeeperTestHarness {

  private val aclAuthorizer = new AclAuthorizer
  private var config: KafkaConfig = _

  private val testUsers2 = "User:test2"
  private val topic2 = "test.topic.00002"

  @Before
  override def setUp(): Unit = {
    super.setUp()
    val props = TestUtils.createBrokerConfig(0, zkConnect)
    config = KafkaConfig.fromProps(props)
    aclAuthorizer.configure(config.originals)
  }

  private def newRequestContext(principal: KafkaPrincipal, clientAddress: InetAddress, apiKey: ApiKeys = ApiKeys.PRODUCE): RequestContext = {
    val securityProtocol = SecurityProtocol.SASL_PLAINTEXT
    val header = new RequestHeader(apiKey, 2, "", 1) //ApiKeys apiKey, short version, String clientId, int correlation
    new RequestContext(header, "", clientAddress, principal, ListenerName.forSecurityProtocol(securityProtocol),
      securityProtocol, ClientInformation.EMPTY)
  }

  /** every pull request run all test case,it may run some diff erro,we can improve this,I think */
  @Test
  def loadCacheTest(): Unit = {
    val principal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, "test2")
    val requestContext = newRequestContext(principal, InetAddress.getByName("192.168.0.2"))

    AclCommand.main(Array[String]("--authorizer-properties", "zookeeper.connect=" + zkConnect, "--add", "--allow-principal", testUsers2, "--operation", "read", "--topic", topic2, "--resource-pattern-type", PatternType.LITERAL.toString))
    Thread.sleep(1500)
    assertTrue(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(TOPIC, topic2, LITERAL)))

    println("list the topic info...")
    AclCommand.main(Array[String]("--authorizer-properties", "zookeeper.connect=" + zkConnect, "--list", "--operation", "read", "--topic", topic2, "--resource-pattern-type", PatternType.LITERAL.toString))

    AclCommand.main(Array[String]("--authorizer-properties", "zookeeper.connect=" + zkConnect, "--force", "--remove", "--allow-principal", testUsers2, "--operation", "read", "--topic", topic2, "--resource-pattern-type", PatternType.LITERAL.toString))
    Thread.sleep(10000)
    assertFalse(authorize(aclAuthorizer, requestContext, READ, new ResourcePattern(TOPIC, topic2, LITERAL)))
  }

  private def authorize(authorizer: AclAuthorizer, requestContext: RequestContext, operation: AclOperation, resource: ResourcePattern): Boolean = {
    val action = new Action(operation, resource, 1, true, true)
    authorizer.authorize(requestContext, List(action).asJava).asScala.head == AuthorizationResult.ALLOWED
  }

  @After
  override def tearDown(): Unit = {
    aclAuthorizer.close()
    super.tearDown()
  }
}