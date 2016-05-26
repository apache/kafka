/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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

import java.io.File
import java.util.Properties
import javax.security.auth.login.Configuration
import kafka.security.minikdc.MiniKdc
import kafka.server.KafkaConfig
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.config.SaslConfigs

/*
 * Implements an enumeration for the modes enabled here:
 * zk only, kafka only, both.
 */
sealed trait SaslSetupMode
case object ZkSasl extends SaslSetupMode
case object KafkaSasl extends SaslSetupMode
case object Both extends SaslSetupMode

/*
 * Trait used in SaslTestHarness and EndToEndAuthorizationTest to setup keytab and jaas files.
 */
trait SaslSetup {
  private val workDir = TestUtils.tempDir()
  private val kdcConf = MiniKdc.createConfig
  private var kdc: MiniKdc = null

  def startSasl(mode: SaslSetupMode = Both, kafkaServerSaslMechanisms: List[String], kafkaClientSaslMechanisms: List[String]) {
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    val hasKerberos = mode != ZkSasl && (kafkaClientSaslMechanisms.contains("GSSAPI") || kafkaServerSaslMechanisms.contains("GSSAPI"))
    if (hasKerberos) {
      val serverKeytabFile = TestUtils.tempFile()
      val clientKeytabFile = TestUtils.tempFile()
      setJaasConfiguration(mode, kafkaServerSaslMechanisms, kafkaClientSaslMechanisms, Some(serverKeytabFile), Some(clientKeytabFile))
      kdc = new MiniKdc(kdcConf, workDir)
      kdc.start()
      kdc.createPrincipal(serverKeytabFile, "kafka/localhost")
      kdc.createPrincipal(clientKeytabFile, "client")
    } else {
      setJaasConfiguration(mode, kafkaServerSaslMechanisms, kafkaClientSaslMechanisms)
    }
    if (mode == Both || mode == ZkSasl)
      System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
  }

  protected def setJaasConfiguration(mode: SaslSetupMode, kafkaServerSaslMechanisms: List[String], kafkaClientSaslMechanisms: List[String], 
      serverKeytabFile: Option[File] = None, clientKeytabFile: Option[File] = None) {
    val jaasFile = mode match {
      case ZkSasl => JaasTestUtils.writeZkFile()
      case KafkaSasl => JaasTestUtils.writeKafkaFile(kafkaServerSaslMechanisms, kafkaClientSaslMechanisms, serverKeytabFile, clientKeytabFile)
      case Both => JaasTestUtils.writeZkAndKafkaFiles(kafkaServerSaslMechanisms, kafkaClientSaslMechanisms, serverKeytabFile, clientKeytabFile)
    }
    // This will cause a reload of the Configuration singleton when `getConfiguration` is called
    Configuration.setConfiguration(null)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, jaasFile)
  }

  def closeSasl() {
    if (kdc != null)
      kdc.stop()
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    System.clearProperty("zookeeper.authProvider.1")
    Configuration.setConfiguration(null)
  }

  def kafkaSaslProperties(clientSaslMechanism: String, serverSaslMechanisms: Option[Seq[String]] = None) = {
    val props = new Properties
    props.put(SaslConfigs.SASL_MECHANISM, clientSaslMechanism)
    serverSaslMechanisms.foreach { serverMechanisms =>
        props.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp, clientSaslMechanism)
        props.put(SaslConfigs.SASL_ENABLED_MECHANISMS, serverMechanisms.mkString(","))
    }
    props
  }
}
