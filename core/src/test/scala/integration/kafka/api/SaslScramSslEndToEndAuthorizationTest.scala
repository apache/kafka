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

import kafka.utils.JaasTestUtils
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.security.scram.internals.ScramMechanism
import org.apache.kafka.test.TestSslUtils

import scala.jdk.CollectionConverters._
import org.junit.jupiter.api.BeforeEach

class SaslScramSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  override protected def kafkaClientSaslMechanism = "SCRAM-SHA-256"
  override protected def kafkaServerSaslMechanisms = ScramMechanism.mechanismNames.asScala.toList
  override val clientPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaScramUser)
  override val kafkaPrincipal = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, JaasTestUtils.KafkaScramAdmin)
  private val kafkaPassword = JaasTestUtils.KafkaScramAdminPassword

  override def configureSecurityBeforeServersStart(): Unit = {
    super.configureSecurityBeforeServersStart()
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    // Create broker credentials before starting brokers
    createScramCredentials(zkConnect, kafkaPrincipal.getName, kafkaPassword)
    TestSslUtils.convertToPemWithoutFiles(producerConfig)
    TestSslUtils.convertToPemWithoutFiles(consumerConfig)
    TestSslUtils.convertToPemWithoutFiles(adminClientConfig)
  }

  override def configureListeners(props: collection.Seq[Properties]): Unit = {
    props.foreach(TestSslUtils.convertToPemWithoutFiles)
    super.configureListeners(props)
  }

  override def createPrivilegedAdminClient() = createScramAdminClient(kafkaClientSaslMechanism, kafkaPrincipal.getName, kafkaPassword)

  @BeforeEach
  override def setUp(): Unit = {
    super.setUp()
    // Create client credentials after starting brokers so that dynamic credential creation is also tested
    createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KafkaScramUser, JaasTestUtils.KafkaScramPassword)
    createScramCredentialsViaPrivilegedAdminClient(JaasTestUtils.KafkaScramUser2, JaasTestUtils.KafkaScramPassword2)
  }
}
