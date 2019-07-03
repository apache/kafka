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

import kafka.utils.JaasTestUtils
import kafka.zk.ConfigEntityChangeNotificationZNode
import org.apache.kafka.common.security.scram.internals.ScramMechanism

import scala.collection.JavaConverters._
import org.junit.Before

class SaslScramSslEndToEndAuthorizationTest extends SaslEndToEndAuthorizationTest {
  override protected def kafkaClientSaslMechanism = "SCRAM-SHA-256"
  override protected def kafkaServerSaslMechanisms = ScramMechanism.mechanismNames.asScala.toList
  override val clientPrincipal = JaasTestUtils.KafkaScramUser
  override val kafkaPrincipal = JaasTestUtils.KafkaScramAdmin
  private val kafkaPassword = JaasTestUtils.KafkaScramAdminPassword

  override def configureSecurityBeforeServersStart() {
    super.configureSecurityBeforeServersStart()
    zkClient.makeSurePersistentPathExists(ConfigEntityChangeNotificationZNode.path)
    // Create broker credentials before starting brokers
    createScramCredentials(zkConnect, kafkaPrincipal, kafkaPassword)
  }

  @Before
  override def setUp() {
    super.setUp()
    // Create client credentials after starting brokers so that dynamic credential creation is also tested
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramUser, JaasTestUtils.KafkaScramPassword)
    createScramCredentials(zkConnect, JaasTestUtils.KafkaScramUser2, JaasTestUtils.KafkaScramPassword2)
  }
}
