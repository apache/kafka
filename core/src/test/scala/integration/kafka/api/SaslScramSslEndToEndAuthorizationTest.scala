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
package kafka.api

import java.util.Properties

import kafka.utils._
import kafka.tools.StorageTool
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.test.TestSslUtils

import scala.jdk.CollectionConverters._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.mutable.ArrayBuffer
import org.apache.kafka.server.common.ApiMessageAndVersion

class SaslScramSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  override protected def kafkaClientSaslMechanism = "SCRAM-SHA-256"
  override protected def kafkaServerSaslMechanisms = ScramMechanism.mechanismNames.asScala.toList
  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaScramUser)
  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaScramAdmin)
  private val kafkaPassword = JaasTestUtils.KafkaScramAdminPassword

  override def configureSecurityBeforeServersStart(testInfo: TestInfo): Unit = {
    super.configureSecurityBeforeServersStart(testInfo)

    if (!TestInfoUtils.isKRaft(testInfo)) {
      zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
      // Create broker credentials before starting brokers
      createScramCredentials(zkConnect, kafkaPrincipal.getName, kafkaPassword)
    }
    TestSslUtils.convertToPemWithoutFiles(producerConfig)
    TestSslUtils.convertToPemWithoutFiles(consumerConfig)
    TestSslUtils.convertToPemWithoutFiles(adminClientConfig)
  }

  // Create the admin credentials for KRaft as part of controller initialization
  override def optionalMetadataRecords: Option[ArrayBuffer[ApiMessageAndVersion]] = {
    val args = Seq("format", "-c", "config.props", "-t", "XcZZOzUqS4yHOjhMQB6JLQ", "-S",
                   s"SCRAM-SHA-256=[name=${JaasTestUtils.KafkaScramAdmin},password=${JaasTestUtils.KafkaScramAdminPassword}]")
    val namespace = StorageTool.parseArguments(args.toArray)
    val metadataRecords : ArrayBuffer[ApiMessageAndVersion] = ArrayBuffer()
    StorageTool.getUserScramCredentialRecords(namespace).foreach {
      userScramCredentialRecords => for (record <- userScramCredentialRecords) {
        metadataRecords.append(new ApiMessageAndVersion(record, 0.toShort))
      }
    }
    Some(metadataRecords)
  }

  override def configureListeners(props: collection.Seq[Properties]): Unit = {
    props.foreach(TestSslUtils.convertToPemWithoutFiles)
    super.configureListeners(props)
  }

  override def createPrivilegedAdminClient() = createScramAdminClient(kafkaClientSaslMechanism, kafkaPrincipal.getName, kafkaPassword)

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
      super.setUp(testInfo)
      // Create client credentials after starting brokers so that dynamic credential creation is also tested
      createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KafkaScramUser, JaasTestUtils.KafkaScramPassword)
      createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KafkaScramUser2, JaasTestUtils.KafkaScramPassword2)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft", "zk"))
  def testAuthentications(quorum: String): Unit = {
    val successfulAuths = TestUtils.totalMetricValue(brokers.head, "successful-authentication-total")
    assertTrue(successfulAuths > 0, "No successful authentications")
    val failedAuths = TestUtils.totalMetricValue(brokers.head, "failed-authentication-total")
    assertEquals(0, failedAuths)
  }
}
