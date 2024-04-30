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
package kafka

import java.nio.file.Files
import java.util
import java.util.Properties
import kafka.server.KafkaConfig
import kafka.utils.{Exit, TestUtils}
import kafka.utils.TestUtils.assertBadConfigContainingMessage
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig

import org.apache.kafka.server.config.{KRaftConfigs, KafkaSecurityConfigs, ZkConfigs}
import org.apache.kafka.server.config.ReplicationConfigs
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._

class KafkaConfigTest {

  @BeforeEach
  def setUp(): Unit = Exit.setExitProcedure((status, _) => throw new FatalExitError(status))

  @AfterEach
  def tearDown(): Unit = Exit.resetExitProcedure()

  @Test
  def testGetKafkaConfigFromArgs(): Unit = {
    val propertiesFile = prepareDefaultConfig()

    // We should load configuration file without any arguments
    val config1 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertEquals(1, config1.brokerId)

    // We should be able to override given property on command line
    val config2 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "broker.id=2")))
    assertEquals(2, config2.brokerId)

    // We should be also able to set completely new property
    val config3 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "log.cleanup.policy=compact")))
    assertEquals(1, config3.brokerId)
    assertEquals(util.Arrays.asList("compact"), config3.logCleanupPolicy)

    // We should be also able to set several properties
    val config4 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "log.cleanup.policy=compact,delete", "--override", "broker.id=2")))
    assertEquals(2, config4.brokerId)
    assertEquals(util.Arrays.asList("compact","delete"), config4.logCleanupPolicy)
  }

  @Test
  def testGetKafkaConfigFromArgsNonArgsAtTheEnd(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    assertThrows(classOf[FatalExitError], () => KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "broker.id=1", "broker.id=2"))))
  }

  @Test
  def testGetKafkaConfigFromArgsNonArgsOnly(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    assertThrows(classOf[FatalExitError], () => KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "broker.id=1", "broker.id=2"))))
  }

  @Test
  def testGetKafkaConfigFromArgsNonArgsAtTheBegging(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    assertThrows(classOf[FatalExitError], () => KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "broker.id=1", "--override", "broker.id=2"))))
  }

  @Test
  def testBrokerRoleNodeIdValidation(): Unit = {
    // Ensure that validation is happening at startup to check that brokers do not use their node.id as a voter in controller.quorum.voters 
    val propertiesFile = new Properties
    propertiesFile.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
    propertiesFile.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    propertiesFile.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9092")
    setListenerProps(propertiesFile)
    assertBadConfigContainingMessage(propertiesFile,
      "If process.roles contains just the 'broker' role, the node id 1 must not be included in the set of voters")

    // Ensure that with a valid config no exception is thrown
    propertiesFile.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    KafkaConfig.fromProps(propertiesFile)
  }

  @Test
  def testControllerRoleNodeIdValidation(): Unit = {
    // Ensure that validation is happening at startup to check that controllers use their node.id as a voter in controller.quorum.voters 
    val propertiesFile = new Properties
    propertiesFile.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    propertiesFile.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    propertiesFile.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092")
    setListenerProps(propertiesFile)
    assertBadConfigContainingMessage(propertiesFile,
      "If process.roles contains the 'controller' role, the node id 1 must be included in the set of voters")

    // Ensure that with a valid config no exception is thrown
    propertiesFile.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    KafkaConfig.fromProps(propertiesFile)
  }

  @Test
  def testCombinedRoleNodeIdValidation(): Unit = {
    // Ensure that validation is happening at startup to check that combined processes use their node.id as a voter in controller.quorum.voters
    val propertiesFile = new Properties
    propertiesFile.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller,broker")
    propertiesFile.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    propertiesFile.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "2@localhost:9092")
    setListenerProps(propertiesFile)
    assertBadConfigContainingMessage(propertiesFile,
      "If process.roles contains the 'controller' role, the node id 1 must be included in the set of voters")

    // Ensure that with a valid config no exception is thrown
    propertiesFile.setProperty(KRaftConfigs.NODE_ID_CONFIG, "2")
    KafkaConfig.fromProps(propertiesFile)
  }

  @Test
  def testIsKRaftCombinedMode(): Unit = {
    val propertiesFile = new Properties
    propertiesFile.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller,broker")
    propertiesFile.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    propertiesFile.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "1@localhost:9092")
    setListenerProps(propertiesFile)
    val config = KafkaConfig.fromProps(propertiesFile)
    assertTrue(config.isKRaftCombinedMode)
  }

  @Test
  def testMustContainQuorumVotersIfUsingProcessRoles(): Unit = {
    // Ensure that validation is happening at startup to check that if process.roles is set controller.quorum.voters is not empty
    val propertiesFile = new Properties
    propertiesFile.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller,broker")
    propertiesFile.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1")
    propertiesFile.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, "")
    setListenerProps(propertiesFile)
    assertBadConfigContainingMessage(propertiesFile,
      "If using process.roles, controller.quorum.voters must contain a parseable set of voters.")

    // Ensure that if neither process.roles nor controller.quorum.voters is populated, then an exception is thrown if zookeeper.connect is not defined
    propertiesFile.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "")
    assertBadConfigContainingMessage(propertiesFile,
      "Missing required configuration `zookeeper.connect` which has no default value.")

    // Ensure that no exception is thrown once zookeeper.connect is defined (and we clear controller.listener.names)
    propertiesFile.setProperty(ZkConfigs.ZK_CONNECT_CONFIG, "localhost:2181")
    propertiesFile.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "")
    KafkaConfig.fromProps(propertiesFile)
  }

  private def setListenerProps(props: Properties): Unit = {
    val hasBrokerRole = props.getProperty(KRaftConfigs.PROCESS_ROLES_CONFIG).contains("broker")
    val hasControllerRole = props.getProperty(KRaftConfigs.PROCESS_ROLES_CONFIG).contains("controller")
    val controllerListener = "SASL_PLAINTEXT://localhost:9092"
    val brokerListener = "PLAINTEXT://localhost:9093"

    if (hasBrokerRole || hasControllerRole) { // KRaft
      props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SASL_PLAINTEXT")
      if (hasBrokerRole && hasControllerRole) {
        props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, s"$brokerListener,$controllerListener")
      } else if (hasControllerRole) {
        props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, controllerListener)
      } else if (hasBrokerRole) {
        props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, brokerListener)
      }
    } else { // ZK-based
       props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, brokerListener)
    }
    if (!(hasControllerRole & !hasBrokerRole)) { // not controller-only
      props.setProperty(ReplicationConfigs.INTER_BROKER_LISTENER_NAME_CONFIG, "PLAINTEXT")
      props.setProperty(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, "PLAINTEXT://localhost:9092")
    }
  }

  @Test
  def testKafkaSslPasswords(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "ssl.keystore.password=keystore_password",
                                                                                    "--override", "ssl.key.password=key_password",
                                                                                    "--override", "ssl.truststore.password=truststore_password",
                                                                                    "--override", "ssl.keystore.certificate.chain=certificate_chain",
                                                                                    "--override", "ssl.keystore.key=private_key",
                                                                                    "--override", "ssl.truststore.certificates=truststore_certificates")))
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_KEY_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_KEYSTORE_KEY_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString)

    assertEquals("key_password", config.getPassword(KafkaSecurityConfigs.SSL_KEY_PASSWORD_CONFIG).value)
    assertEquals("keystore_password", config.getPassword(KafkaSecurityConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value)
    assertEquals("truststore_password", config.getPassword(KafkaSecurityConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).value)
    assertEquals("private_key", config.getPassword(KafkaSecurityConfigs.SSL_KEYSTORE_KEY_CONFIG).value)
    assertEquals("certificate_chain", config.getPassword(KafkaSecurityConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).value)
    assertEquals("truststore_certificates", config.getPassword(KafkaSecurityConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).value)
  }

  @Test
  def testKafkaSslPasswordsWithSymbols(): Unit = {
    val password = "=!#-+!?*/\"\'^%$=\\.,@:;="
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile,
      "--override", "ssl.keystore.password=" + password,
      "--override", "ssl.key.password=" + password,
      "--override", "ssl.truststore.password=" + password)))
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_KEY_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaSecurityConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString)

    assertEquals(password, config.getPassword(KafkaSecurityConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value)
    assertEquals(password, config.getPassword(KafkaSecurityConfigs.SSL_KEY_PASSWORD_CONFIG).value)
    assertEquals(password, config.getPassword(KafkaSecurityConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).value)
  }

  private val booleanPropValueToSet = true
  private val stringPropValueToSet = "foo"
  private val passwordPropValueToSet = "ThePa$$word!"
  private val listPropValueToSet = List("A", "B")

  @Test
  def testZkSslClientEnable(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_CLIENT_ENABLE_CONFIG, "zookeeper.ssl.client.enable",
      "zookeeper.client.secure", booleanPropValueToSet, config => Some(config.zkSslClientEnable), booleanPropValueToSet, Some(false))
  }

  @Test
  def testZkSslKeyStoreLocation(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_KEY_STORE_LOCATION_CONFIG, "zookeeper.ssl.keystore.location",
      "zookeeper.ssl.keyStore.location", stringPropValueToSet, config => config.zkSslKeyStoreLocation, stringPropValueToSet)
  }

  @Test
  def testZkSslTrustStoreLocation(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_TRUST_STORE_LOCATION_CONFIG, "zookeeper.ssl.truststore.location",
      "zookeeper.ssl.trustStore.location", stringPropValueToSet, config => config.zkSslTrustStoreLocation, stringPropValueToSet)
  }

  @Test
  def testZookeeperKeyStorePassword(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_KEY_STORE_PASSWORD_CONFIG, "zookeeper.ssl.keystore.password",
      "zookeeper.ssl.keyStore.password", passwordPropValueToSet, config => config.zkSslKeyStorePassword, new Password(passwordPropValueToSet))
  }

  @Test
  def testZookeeperTrustStorePassword(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_TRUST_STORE_PASSWORD_CONFIG, "zookeeper.ssl.truststore.password",
      "zookeeper.ssl.trustStore.password", passwordPropValueToSet, config => config.zkSslTrustStorePassword, new Password(passwordPropValueToSet))
  }

  @Test
  def testZkSslKeyStoreType(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_KEY_STORE_TYPE_CONFIG, "zookeeper.ssl.keystore.type",
      "zookeeper.ssl.keyStore.type", stringPropValueToSet, config => config.zkSslKeyStoreType, stringPropValueToSet)
  }

  @Test
  def testZkSslTrustStoreType(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_TRUST_STORE_TYPE_CONFIG, "zookeeper.ssl.truststore.type",
      "zookeeper.ssl.trustStore.type", stringPropValueToSet, config => config.zkSslTrustStoreType, stringPropValueToSet)
  }

  @Test
  def testZkSslProtocol(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_PROTOCOL_CONFIG, "zookeeper.ssl.protocol",
      "zookeeper.ssl.protocol", stringPropValueToSet, config => Some(config.ZkSslProtocol), stringPropValueToSet, Some("TLSv1.2"))
  }

  @Test
  def testZkSslEnabledProtocols(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_ENABLED_PROTOCOLS_CONFIG, "zookeeper.ssl.enabled.protocols",
      "zookeeper.ssl.enabledProtocols", listPropValueToSet.mkString(","), config => config.ZkSslEnabledProtocols, listPropValueToSet.asJava)
  }

  @Test
  def testZkSslCipherSuites(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_CIPHER_SUITES_CONFIG, "zookeeper.ssl.cipher.suites",
      "zookeeper.ssl.ciphersuites", listPropValueToSet.mkString(","), config => config.ZkSslCipherSuites, listPropValueToSet.asJava)
  }

  @Test
  def testZkSslEndpointIdentificationAlgorithm(): Unit = {
    // this property is different than the others
    // because the system property values and the Kafka property values don't match
    val kafkaPropName = ZkConfigs.ZK_SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG
    assertEquals("zookeeper.ssl.endpoint.identification.algorithm", kafkaPropName)
    val sysProp = "zookeeper.ssl.hostnameVerification"
    val expectedDefaultValue = "HTTPS"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct default value
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.originals.get(kafkaPropName)) // doesn't appear in the originals
    assertEquals(expectedDefaultValue, emptyConfig.values.get(kafkaPropName)) // but default value appears in the values
    assertEquals(expectedDefaultValue, emptyConfig.ZkSslEndpointIdentificationAlgorithm) // and has the correct default value
    // next set system property alone
    Map("true" -> "HTTPS", "false" -> "").foreach { case (sysPropValue, expected) =>
      try {
        System.setProperty(sysProp, sysPropValue)
        val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
        assertNull(config.originals.get(kafkaPropName)) // doesn't appear in the originals
        assertEquals(expectedDefaultValue, config.values.get(kafkaPropName)) // default value appears in the values
        assertEquals(expected, config.ZkSslEndpointIdentificationAlgorithm) // system property impacts the ultimate value of the property
      } finally {
        System.clearProperty(sysProp)
      }
    }
    // finally set Kafka config alone
    List("https", "").foreach(expected => {
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaPropName=$expected")))
      assertEquals(expected, config.originals.get(kafkaPropName)) // appears in the originals
      assertEquals(expected, config.values.get(kafkaPropName)) // appears in the values
      assertEquals(expected, config.ZkSslEndpointIdentificationAlgorithm) // is the ultimate value
    })
  }

  @Test
  def testZkSslCrlEnable(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_CRL_ENABLE_CONFIG, "zookeeper.ssl.crl.enable",
      "zookeeper.ssl.crl", booleanPropValueToSet, config => Some(config.ZkSslCrlEnable), booleanPropValueToSet, Some(false))
  }

  @Test
  def testZkSslOcspEnable(): Unit = {
    testZkConfig(ZkConfigs.ZK_SSL_OCSP_ENABLE_CONFIG, "zookeeper.ssl.ocsp.enable",
      "zookeeper.ssl.ocsp", booleanPropValueToSet, config => Some(config.ZkSslOcspEnable), booleanPropValueToSet, Some(false))
  }

  @Test
  def testConnectionsMaxReauthMsDefault(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertEquals(0L, config.valuesWithPrefixOverride("sasl_ssl.oauthbearer.").get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS).asInstanceOf[Long])
  }

  @Test
  def testConnectionsMaxReauthMsExplicit(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val expected = 3600000
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"sasl_ssl.oauthbearer.connections.max.reauth.ms=$expected")))
    assertEquals(expected, config.valuesWithPrefixOverride("sasl_ssl.oauthbearer.").get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS).asInstanceOf[Long])
  }

  private def testZkConfig[T, U](kafkaPropName: String,
                                 expectedKafkaPropName: String,
                                 sysPropName: String,
                                 propValueToSet: T,
                                 getPropValueFrom: KafkaConfig => Option[T],
                                 expectedPropertyValue: U,
                                 expectedDefaultValue: Option[T] = None): Unit = {
    assertEquals(expectedKafkaPropName, kafkaPropName)
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct default value (if any)
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.originals.get(kafkaPropName)) // doesn't appear in the originals
    if (expectedDefaultValue.isDefined) {
      // confirm default value behavior
      assertEquals(expectedDefaultValue.get, emptyConfig.values.get(kafkaPropName)) // default value appears in the values
      assertEquals(expectedDefaultValue.get, getPropValueFrom(emptyConfig).get) // default value appears in the property
    } else {
      // confirm no default value behavior
      assertNull(emptyConfig.values.get(kafkaPropName)) // doesn't appear in the values
      assertEquals(None, getPropValueFrom(emptyConfig)) // has no default value
    }
    // next set system property alone
    try {
      System.setProperty(sysPropName, s"$propValueToSet")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.originals.get(kafkaPropName)) // doesn't appear in the originals
      // confirm default value (if any) overridden by system property
      if (expectedDefaultValue.isDefined)
        assertEquals(expectedDefaultValue.get, config.values.get(kafkaPropName)) // default value (different from system property) appears in the values
      else
        assertNull(config.values.get(kafkaPropName)) // doesn't appear in the values
      // confirm system property appears in the property
      assertEquals(Some(expectedPropertyValue), getPropValueFrom(config))
    } finally {
      System.clearProperty(sysPropName)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaPropName=$propValueToSet")))
    assertEquals(expectedPropertyValue, config.values.get(kafkaPropName)) // appears in the values
    assertEquals(Some(expectedPropertyValue), getPropValueFrom(config)) // appears in the property
  }

  def prepareDefaultConfig(): String = {
    prepareConfig(Array("broker.id=1", "zookeeper.connect=somewhere"))
  }

  def prepareConfig(lines : Array[String]): String = {
    val file = TestUtils.tempFile("kafkatest", ".properties")

    val writer = Files.newOutputStream(file.toPath)
    try {
      lines.foreach { l =>
        writer.write(l.getBytes)
        writer.write("\n".getBytes)
      }
      file.getAbsolutePath
    } finally writer.close()
  }
}
