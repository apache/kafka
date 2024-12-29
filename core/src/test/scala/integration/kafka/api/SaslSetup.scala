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

import kafka.security.JaasTestUtils
import kafka.security.JaasTestUtils.JaasSection
import kafka.security.minikdc.MiniKdc
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import kafka.zk.{AdminZkClient, KafkaZkClient}
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, ScramCredentialInfo, UserScramCredentialUpsertion, ScramMechanism => PublicScramMechanism}
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.security.authenticator.LoginManager
import org.apache.kafka.common.security.scram.internals.{ScramCredentialUtils, ScramFormatter, ScramMechanism}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.config.ConfigType
import org.apache.zookeeper.client.ZKClientConfig

import java.io.File
import java.util
import java.util.Properties
import javax.security.auth.login.Configuration
import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.jdk.javaapi.OptionConverters
import scala.util.Using

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
  private var kdc: MiniKdc = _
  private var serverKeytabFile: Option[File] = None
  private var clientKeytabFile: Option[File] = None

  def startSasl(jaasSections: Seq[JaasSection]): Unit = {
    // Important if tests leak consumers, producers or brokers
    LoginManager.closeAll()

    val hasKerberos = jaasSections.exists(_.getModules.asScala.exists(_.name().endsWith("Krb5LoginModule")))

    if (hasKerberos) {
      initializeKerberos()
    }

    writeJaasConfigurationToFile(jaasSections)

    val hasZk = jaasSections.exists(_.getModules.asScala.exists(_.name() == "org.apache.zookeeper.server.auth.DigestLoginModule"))

    if (hasZk)
      System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
  }

  protected def initializeKerberos(): Unit = {
    val (serverKeytabFile, clientKeytabFile) = maybeCreateEmptyKeytabFiles()
    kdc = new MiniKdc(kdcConf, workDir)
    kdc.start()
    kdc.createPrincipal(serverKeytabFile, List(JaasTestUtils.KAFKA_SERVER_PRINCIPAL_UNQUALIFIED_NAME + "/localhost").asJava)
    kdc.createPrincipal(clientKeytabFile,
      List(JaasTestUtils.KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME, JaasTestUtils.KAFKA_CLIENT_PRINCIPAL_UNQUALIFIED_NAME_2).asJava)
  }

  /** Return a tuple with the path to the server keytab file and client keytab file */
  protected def maybeCreateEmptyKeytabFiles(): (File, File) = {
    if (serverKeytabFile.isEmpty)
      serverKeytabFile = Some(TestUtils.tempFile())
    if (clientKeytabFile.isEmpty)
      clientKeytabFile = Some(TestUtils.tempFile())
    (serverKeytabFile.get, clientKeytabFile.get)
  }

  def jaasSections(kafkaServerSaslMechanisms: Seq[String],
                             kafkaClientSaslMechanism: Option[String],
                             mode: SaslSetupMode = Both,
                             kafkaServerEntryName: String = JaasTestUtils.KAFKA_SERVER_CONTEXT_NAME): Seq[JaasSection] = {
    val hasKerberos = mode != ZkSasl &&
      (kafkaServerSaslMechanisms.contains("GSSAPI") || kafkaClientSaslMechanism.contains("GSSAPI"))
    if (hasKerberos)
      maybeCreateEmptyKeytabFiles()
    mode match {
      case ZkSasl => JaasTestUtils.zkSections.asScala
      case KafkaSasl =>
        Seq(JaasTestUtils.kafkaServerSection(kafkaServerEntryName, kafkaServerSaslMechanisms.asJava, serverKeytabFile.toJava),
          JaasTestUtils.kafkaClientSection(kafkaClientSaslMechanism.toJava, clientKeytabFile.toJava))
      case Both => Seq(JaasTestUtils.kafkaServerSection(kafkaServerEntryName, kafkaServerSaslMechanisms.asJava, serverKeytabFile.toJava),
        JaasTestUtils.kafkaClientSection(kafkaClientSaslMechanism.toJava, clientKeytabFile.toJava)) ++ JaasTestUtils.zkSections.asScala
    }
  }

  private def writeJaasConfigurationToFile(jaasSections: Seq[JaasSection]): Unit = {
    val file = JaasTestUtils.writeJaasContextsToFile(jaasSections.asJava)
    System.setProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM, file.getAbsolutePath)
    // This will cause a reload of the Configuration singleton when `getConfiguration` is called
    Configuration.setConfiguration(null)
  }

  def closeSasl(): Unit = {
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
    props.put(BrokerSecurityConfigs.SASL_MECHANISM_INTER_BROKER_PROTOCOL_CONFIG, interBrokerSaslMechanism)
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
      JaasTestUtils.clientLoginModule(clientSaslMechanism, clientKeytabFile.toJava, serviceName.get)
    else
      JaasTestUtils.clientLoginModule(clientSaslMechanism, clientKeytabFile.toJava)
  }

  def jaasAdminLoginModule(clientSaslMechanism: String, serviceName: Option[String] = None): String = {
    if (serviceName.isDefined)
      JaasTestUtils.adminLoginModule(clientSaslMechanism, serverKeytabFile.toJava, serviceName.get)
    else
      JaasTestUtils.adminLoginModule(clientSaslMechanism, serverKeytabFile.toJava)
  }

  def jaasScramClientLoginModule(clientSaslScramMechanism: String, scramUser: String, scramPassword: String): String = {
    JaasTestUtils.scramClientLoginModule(clientSaslScramMechanism, scramUser, scramPassword)
  }

  def createPrivilegedAdminClient(): Admin = {
    // create an admin client instance that is authorized to create credentials
    throw new UnsupportedOperationException("Must implement this if a test needs to use it")
  }

  def createAdminClient(brokerList: String, securityProtocol: SecurityProtocol, trustStoreFile: Option[File],
                        clientSaslProperties: Option[Properties], scramMechanism: String, user: String, password: String) : Admin = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val securityProps: util.Map[Object, Object] =
      JaasTestUtils.adminClientSecurityConfigs(securityProtocol, OptionConverters.toJava(trustStoreFile), OptionConverters.toJava(clientSaslProperties))
    securityProps.forEach { (key, value) => config.put(key.asInstanceOf[String], value) }
    config.put(SaslConfigs.SASL_JAAS_CONFIG, jaasScramClientLoginModule(scramMechanism, user, password))
    Admin.create(config)
  }

  def createScramCredentialsViaPrivilegedAdminClient(userName: String, password: String): Unit = {
    val privilegedAdminClient = createPrivilegedAdminClient() // must explicitly implement this method
    try {
      // create the SCRAM credential for the given user
      createScramCredentials(privilegedAdminClient, userName, password)
    } finally {
      privilegedAdminClient.close()
    }
  }

  def createScramCredentials(adminClient: Admin, userName: String, password: String): Unit = {
    PublicScramMechanism.values().filter(_ != PublicScramMechanism.UNKNOWN).map(mechanism => {

      val results = adminClient.alterUserScramCredentials(util.Arrays.asList(
        new UserScramCredentialUpsertion(userName, new ScramCredentialInfo(mechanism, 4096), password)))
      results.all.get
    })
  }

  def createScramCredentials(zkConnect: String, userName: String, password: String): Unit = {
    val zkClientConfig = new ZKClientConfig()
    Using.resource(KafkaZkClient(
      zkConnect, JaasUtils.isZkSaslEnabled || KafkaConfig.zkTlsClientAuthEnabled(zkClientConfig), 30000, 30000,
      Int.MaxValue, Time.SYSTEM, name = "SaslSetup", zkClientConfig = zkClientConfig, enableEntityConfigControllerCheck = false)) { zkClient =>
      val adminZkClient = new AdminZkClient(zkClient)

      val entityType = ConfigType.USER
      val entityName = userName
      val configs = adminZkClient.fetchEntityConfig(entityType, entityName)

      ScramMechanism.values().foreach(mechanism => {
        val credential = new ScramFormatter(mechanism).generateCredential(password, 4096)
        val credentialString = ScramCredentialUtils.credentialToString(credential)
        configs.setProperty(mechanism.mechanismName, credentialString)
      })

      adminZkClient.changeConfigs(entityType, entityName, configs)
    }
  }

}
