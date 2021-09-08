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
package kafka.api

import java.util
import java.util.Properties
import java.util.concurrent.ExecutionException
import kafka.security.authorizer.AclEntry
import kafka.server.KafkaConfig
import kafka.utils.Logging
import kafka.utils.TestUtils._
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, DescribeTopicsOptions, TopicDescription}
import org.apache.kafka.common.acl.AclOperation
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.resource.ResourceType
import org.apache.kafka.common.utils.Utils
import org.junit.jupiter.api.{AfterEach, BeforeEach, Timeout}

import scala.jdk.CollectionConverters._
import scala.collection.Seq

/**
 * Base integration test cases for [[Admin]]. Each test case added here will be executed
 * in extending classes. Typically we prefer to write basic Admin functionality test cases in
 * [[kafka.api.PlaintextAdminIntegrationTest]] rather than here to avoid unnecessary execution
 * time to the build. However, if an admin API involves differing interactions with
 * authentication/authorization layers, we may add the test case here.
 */
@Timeout(120)
abstract class BaseAdminIntegrationTest2 extends IntegrationTestHarness with Logging {
  def brokerCount = 1
  override def logDirCount = 2

  var client: Admin = _

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    waitUntilBrokerMetadataIsPropagated(servers)
  }

  @AfterEach
  override def tearDown(): Unit = {
    if (client != null)
      Utils.closeQuietly(client, "AdminClient")
    super.tearDown()
  }

  def configuredClusterPermissions: Set[AclOperation] =
    AclEntry.supportedOperations(ResourceType.CLUSTER)

  override def modifyConfigs(configs: Seq[Properties]): Unit = {
    super.modifyConfigs(configs)
    configs.foreach { config =>
      config.setProperty(KafkaConfig.DeleteTopicEnableProp, "true")
      config.setProperty(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
      config.setProperty(KafkaConfig.AutoLeaderRebalanceEnableProp, "false")
      config.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false")
      // We set this in order to test that we don't expose sensitive data via describe configs. This will already be
      // set for subclasses with security enabled and we don't want to overwrite it.
      if (!config.containsKey(KafkaConfig.SslTruststorePasswordProp))
        config.setProperty(KafkaConfig.SslTruststorePasswordProp, "some.invalid.pass")
    }
  }

  def createConfig: util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    val securityProps: util.Map[Object, Object] =
      adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.forEach { (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }

  def waitForTopics(client: Admin, expectedPresent: Seq[String], expectedMissing: Seq[String]): Unit = {
    waitUntilTrue(() => {
      val topics = client.listTopics.names.get()
      expectedPresent.forall(topicName => topics.contains(topicName)) &&
        expectedMissing.forall(topicName => !topics.contains(topicName))
    }, "timed out waiting for topics")
  }

  def getTopicMetadata(client: Admin,
                       topic: String,
                       describeOptions: DescribeTopicsOptions = new DescribeTopicsOptions,
                       expectedNumPartitionsOpt: Option[Int] = None): TopicDescription = {
    var result: TopicDescription = null
    waitUntilTrue(() => {
      val topicResult = client.describeTopics(Set(topic).asJava, describeOptions).values.get(topic)
      try {
        result = topicResult.get
        expectedNumPartitionsOpt.map(_ == result.partitions.size).getOrElse(true)
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false  // metadata may not have propagated yet, so retry
      }
    }, s"Timed out waiting for metadata for $topic")
    result
  }

}
