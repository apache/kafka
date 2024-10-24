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

import kafka.security.JaasTestUtils
import kafka.utils.TestUtils
import kafka.zk.{KafkaZkClient, ZkData}
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.config.ZkConfigs
import org.apache.zookeeper.ZooDefs.Perms
import org.apache.zookeeper.data.ACL
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api._

import java.util.Locale
import scala.collection.Seq

@Timeout(600)
class SaslPlainPlaintextConsumerTest extends BaseConsumerTest with SaslSetup {
  override protected def listenerName = new ListenerName("CLIENT")
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  private val kafkaServerJaasEntryName =
    s"${listenerName.value.toLowerCase(Locale.ROOT)}.${JaasTestUtils.KAFKA_SERVER_CONTEXT_NAME}"
  this.serverConfig.setProperty(ZkConfigs.ZK_ENABLE_SECURE_ACLS_CONFIG, "false")
  // disable secure acls of zkClient in QuorumTestHarness
  override protected def zkAclsEnabled = Some(false)
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected lazy val trustStoreFile = Some(TestUtils.tempFile("truststore", ".jks"))
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @BeforeEach
  override def setUp(testInfo: TestInfo): Unit = {
    startSasl(jaasSections(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), Both, kafkaServerJaasEntryName))
    super.setUp(testInfo)
  }

  @AfterEach
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  /**
   * Checks that everyone can access ZkData.SecureZkRootPaths and ZkData.SensitiveZkRootPaths
   * when zookeeper.set.acl=false, even if ZooKeeper is SASL-enabled.
   */
  @Test
  def testZkAclsDisabled(): Unit = {
    secureZkPaths(zkClient).foreach(path => {
      if (zkClient.pathExists(path)) {
        val acls = zkClient.getAcl(path)
        assertEquals(1, acls.size, s"Invalid ACLs for $path $acls")
        acls.foreach(isAclUnsecure)
      }
    })
  }

  def secureZkPaths(zkClient: KafkaZkClient): Seq[String] = {
    def subPaths(path: String): Seq[String] = {
      if (zkClient.pathExists(path))
        path +: zkClient.getChildren(path).map(c => path + "/" + c).flatMap(subPaths)
      else
        Seq.empty
    }
    val topLevelPaths = ZkData.SecureRootPaths ++ ZkData.SensitiveRootPaths
    topLevelPaths.flatMap(subPaths)
  }

  /**
   * Verifies that the ACL corresponds to the unsecure one that
   * provides ALL access to everyone (world).
   */
  def isAclUnsecure(acl: ACL): Boolean = {
    debug(s"ACL $acl")
    acl.getPerms match {
      case Perms.ALL => acl.getId.getScheme == "world"
      case _ => false
    }
  }
}
