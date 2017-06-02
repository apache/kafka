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

import java.util

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestUtils}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.server.policy.AlterConfigPolicy
import org.junit.{After, Before, Rule, Test}
import org.junit.rules.Timeout

import scala.collection.JavaConverters._

/**
  * Tests AdminClient calls when the broker is configured with policies like AlterConfigPolicy, CreateTopicPolicy, etc.
  */
class AdminClientWithPoliciesIntegrationTest extends KafkaServerTestHarness with Logging {

  import AdminClientWithPoliciesIntegrationTest._

  var client: AdminClient = null
  val brokerCount = 2

  @Rule
  def globalTimeout = Timeout.millis(120000)

  @Before
  override def setUp(): Unit = {
    super.setUp
    TestUtils.waitUntilBrokerMetadataIsPropagated(servers)
  }

  @After
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  def createConfig: util.Map[String, Object] =
    Map[String, Object](AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> brokerList).asJava

  override def generateConfigs = {
    val configs = TestUtils.createBrokerConfigs(brokerCount, zkConnect)
    configs.foreach(props => props.put(KafkaConfig.AlterConfigPolicyClassNameProp, classOf[Policy]))
    configs.map(KafkaConfig.fromProps)
  }

  @Test
  def testValidDescribeAlterConfigs(): Unit = {
    client = AdminClient.create(createConfig)
    KafkaAdminClientIntegrationTest.checkValidDescribeAlterConfigs(zkUtils, servers, client)
  }

  @Test
  def testInvalidAlterConfigs(): Unit = {
    client = AdminClient.create(createConfig)
    KafkaAdminClientIntegrationTest.checkInvalidAlterConfigs(zkUtils, servers, client)
  }

  @Test
  def testInvalidAlterConfigsDueToPolicy(): Unit = {
    client = AdminClient.create(createConfig)
  }


}

object AdminClientWithPoliciesIntegrationTest {

  class Policy extends AlterConfigPolicy {

    var configs: Map[String, _] = _
    var closed = false

    def configure(configs: util.Map[String, _]): Unit = {
      this.configs = configs.asScala.toMap
    }

    def validate(requestMetadata: AlterConfigPolicy.RequestMetadata): Unit = {
      require(!closed, "Policy should not be closed")
      require(!configs.isEmpty, "configure should have been called with non empty configs")
      require(!requestMetadata.configs.isEmpty, "request configs should not be empty")
      require(requestMetadata.resource.name.nonEmpty, "resource name should not be empty")
    }

    def close(): Unit = closed = true

  }
}
