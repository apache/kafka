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

import kafka.admin.ConfigCommand
import kafka.security.minikdc.MiniKdc
import kafka.server.KafkaConfig
import kafka.utils.JaasTestUtils.{JaasSection, Krb5LoginModule, ZkDigestModule}
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.security.scram.internals.ScramMechanism

/*
 * Implements an enumeration for the modes enabled here:
 * zk only, kafka only, both, custom KafkaServer.
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
  private var serverKeytabFile: Option[File] = None
  private var clientKeytabFile: Option[File] = None

  def startSasl(jaasSections: Seq[JaasSection]) {
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()
    val hasKerberos = jaasSections.exists(_.modules.exists {
      case _: Krb5LoginModule => true
      case _ => false
    })
    if (hasKerberos) {
      initializeKerberos()
    }
    writeJaasConfigurationToFile(jaasSections)
    val hasZk = jaasSections.exists(_.modules.exists {
      case _: ZkDigestModule => true
      case _ => false
    })
    if (hasZk)
      System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
  }

  protected def initializeKerberos(): Unit = {
    val (serverKeytabFile, clientKeytabFile) = maybeCreateEmptyKeytabFiles()
    kdc = new MiniKdc(kdcConf, workDir)
    kdc.start()
    kdc.createPrincipal(serverKeytabFile, JaasTestUtils.KafkaServerPrincipalUnqualifiedName + "/localhost")
    kdc.createPrincipal(clientKeytabFile,
      JaasTestUtils.KafkaClientPrincipalUnqualifiedName, JaasTestUtils.KafkaClientPrincipalUnqualifiedName2)
  }

  /** Return a tuple with the path to the server keytab file and client keytab file */
  protected def maybeCreateEmptyKeytabFiles(): (File, File) = {
    if (serverKeytabFile.isEmpty)
      serverKeytabFile = Some(TestUtils.tempFile())
    if (clientKeytabFile.isEmpty)
      clientKeytabFile = Some(TestUtils.tempFile())
    (serverKeytabFile.get, clientKeytabFile.get)
  }

  protected def jaasSections(kafkaServerSaslMechanisms: Seq[String],
                             kafkaClientSaslMechanism: Option[String],
                             mode: SaslSetupMode = Both,
                             kafkaServerEntryName: String = JaasTestUtils.KafkaServerContextName): Seq[JaasSection] = {
    val hasKerberos = mode != ZkSasl &&
      (kafkaServerSaslMechanisms.contains("GSSAPI") || kafkaClientSaslMechanism.contains("GSSAPI"))
    if (hasKerberos)
      maybeCreateEmptyKeytabFiles()
    mode match {
      case ZkSasl => JaasTestUtils.zkSections
      case KafkaSasl =>
        Seq(JaasTestUtils.kafkaServerSection(kafkaServerEntryName, kafkaServerSaslMechanisms, serverKeytabFile),
          JaasTestUtils.kafkaClientSection(kafkaClientSaslMechanism, clientKeytabFile))
      case Both => Seq(JaasTestUtils.kafkaServerSection(kafkaServerEntryName, kafkaServerSaslMechanisms, serverKeytabFile),
        JaasTestUtils.kafkaClientSection(kafkaClientSaslMechanism, clientKeytabFile)) ++ JaasTestUtils.zkSections
    }
  }

  private def writeJaasConfigurationToFile(jaasSections: Seq[JaasSection]) {
    val file = JaasTestUtils.writeJaasContextsToFile(jaasSections)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, file.getAbsolutePath)
    // This will cause a reload of the Configuration singleton when `getConfiguration` is called
    Configuration.setConfiguration(null)
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

  def kafkaServerSaslProperties(serverSaslMechanisms: Seq[String], interBrokerSaslMechanism: String): Properties = {
    val props = new Properties
    props.put(KafkaConfig.SaslMechanismInterBrokerProtocolProp, interBrokerSaslMechanism)
    props.put(BrokerSecurityConfigs.SASL_ENABLED_MECHANISMS_CONFIG, serverSaslMechanisms.mkString(","))
    props
  }

  def kafkaClientSaslProperties(clientSaslMechanism: String, dynamicJaasConfig: Boolean = false): Properties = {
    val props = new Properties
    props.put(SaslConfigs.SASL_MECHANISM, clientSaslMechanism)
    if (dynamicJaasConfig)
      props.put(SaslConfigs.SASL_JAAS_CONFIG, jaasClientLoginModule(clientSaslMechanism))
    props
  }

  def jaasClientLoginModule(clientSaslMechanism: String, serviceName: Option[String] = None): String = {
    if (serviceName.isDefined)
      JaasTestUtils.clientLoginModule(clientSaslMechanism, clientKeytabFile, serviceName.get)
    else
      JaasTestUtils.clientLoginModule(clientSaslMechanism, clientKeytabFile)
  }

  def createScramCredentials(zkConnect: String, userName: String, password: String): Unit = {
    val credentials = ScramMechanism.values.map(m => s"${m.mechanismName}=[iterations=4096,password=$password]")
    val args = Array("--zookeeper", zkConnect,
      "--alter", "--add-config", credentials.mkString(","),
      "--entity-type", "users",
      "--entity-name", userName)
    ConfigCommand.main(args)
  }

}
