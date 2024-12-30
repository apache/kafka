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

import kafka.security.JaasTestUtils

import java.util.Properties
import kafka.utils._
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.metadata.storage.Formatter
import org.apache.kafka.test.TestSslUtils

import scala.jdk.CollectionConverters._
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{BeforeEach, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

class SaslScramSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  override protected def kafkaClientSaslMechanism = "SCRAM-SHA-256"
  override protected def kafkaServerSaslMechanisms = ScramMechanism.mechanismNames.asScala.toList
  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KAFKA_SCRAM_USER)
  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KAFKA_SCRAM_ADMIN)
  private val kafkaPassword = JaasTestUtils.KAFKA_SCRAM_ADMIN_PASSWORD

  override def configureSecurityBeforeServersStart(testInfo: TestInfo): Unit = {
    super.configureSecurityBeforeServersStart(testInfo)

    TestSslUtils.convertToPemWithoutFiles(producerConfig)
    TestSslUtils.convertToPemWithoutFiles(consumerConfig)
    TestSslUtils.convertToPemWithoutFiles(adminClientConfig)
  }

  // Create the admin credentials for KRaft as part of controller initialization
  override def addFormatterSettings(formatter: Formatter): Unit = {
    formatter.setClusterId("XcZZOzUqS4yHOjhMQB6JLQ")
    formatter.setScramArguments(List(
      s"SCRAM-SHA-256=[name=${JaasTestUtils.KAFKA_SCRAM_ADMIN},password=${JaasTestUtils.KAFKA_SCRAM_ADMIN_PASSWORD}]").asJava)
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
      createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KAFKA_SCRAM_USER, JaasTestUtils.KAFKA_SCRAM_PASSWORD)
      createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KAFKA_SCRAM_USER_2, JaasTestUtils.KAFKA_SCRAM_PASSWORD_2)
  }

  @ParameterizedTest
  @ValueSource(strings = Array("kraft"))
  def testAuthentications(quorum: String): Unit = {
    val successfulAuths = TestUtils.totalMetricValue(brokers.head, "successful-authentication-total")
    assertTrue(successfulAuths > 0, "No successful authentications")
    val failedAuths = TestUtils.totalMetricValue(brokers.head, "failed-authentication-total")
    assertEquals(0, failedAuths)
  }
}
