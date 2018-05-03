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

import java.io.File
import java.util.Locale

import kafka.server.KafkaConfig
import kafka.utils.{CoreUtils, JaasTestUtils, TestUtils, ZkUtils}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.junit.{After, Before, Test}

class SaslPlainPlaintextConsumerTest extends BaseConsumerTest with SaslSetup {
  override protected def listenerName = new ListenerName("CLIENT")
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  private val kafkaServerJaasEntryName =
    s"${listenerName.value.toLowerCase(Locale.ROOT)}.${JaasTestUtils.KafkaServerContextName}"
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "false")
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, kafkaServerJaasEntryName))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  /**
   * Checks that everyone can access ZkUtils.SecureZkRootPaths and ZkUtils.SensitiveZkRootPaths
   * when zookeeper.set.acl=false, even if ZooKeeper is SASL-enabled.
   */
  @Test
  def testZkAclsDisabled(): Unit = {
    val zkUtils = ZkUtils(zkConnect, zkSessionTimeout, zkConnectionTimeout, zkAclsEnabled.getOrElse(JaasUtils.isZkSecurityEnabled))
    TestUtils.verifyUnsecureZkAcls(zkUtils)
    CoreUtils.swallow(zkUtils.close(), this)
  }
}
