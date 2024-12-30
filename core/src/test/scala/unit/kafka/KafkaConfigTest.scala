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
import kafka.utils.TestUtils
import kafka.utils.TestUtils.assertBadConfigContainingMessage
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.Exit
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.config.{KRaftConfigs, ReplicationConfigs, ZkConfigs}
import org.junit.jupiter.api.{AfterEach, BeforeEach, Test}
import org.junit.jupiter.api.Assertions._

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
    assertBadConfigContainingMessage(
      propertiesFile,
      """If using process.roles, either controller.quorum.bootstrap.servers
      |must contain the set of bootstrap controllers or controller.quorum.voters must contain a
      |parseable set of controllers.""".stripMargin.replace("\n", " ")
    )

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

    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "SASL_PLAINTEXT")
    if (hasBrokerRole && hasControllerRole) {
      props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, s"$brokerListener,$controllerListener")
    } else if (hasControllerRole) {
      props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, controllerListener)
    } else if (hasBrokerRole) {
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
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_KEYSTORE_KEY_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).toString)

    assertEquals("key_password", config.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG).value)
    assertEquals("keystore_password", config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value)
    assertEquals("truststore_password", config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).value)
    assertEquals("private_key", config.getPassword(SslConfigs.SSL_KEYSTORE_KEY_CONFIG).value)
    assertEquals("certificate_chain", config.getPassword(SslConfigs.SSL_KEYSTORE_CERTIFICATE_CHAIN_CONFIG).value)
    assertEquals("truststore_certificates", config.getPassword(SslConfigs.SSL_TRUSTSTORE_CERTIFICATES_CONFIG).value)
  }

  @Test
  def testKafkaSslPasswordsWithSymbols(): Unit = {
    val password = "=!#-+!?*/\"\'^%$=\\.,@:;="
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile,
      "--override", "ssl.keystore.password=" + password,
      "--override", "ssl.key.password=" + password,
      "--override", "ssl.truststore.password=" + password)))
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).toString)
    assertEquals(Password.HIDDEN, config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).toString)

    assertEquals(password, config.getPassword(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG).value)
    assertEquals(password, config.getPassword(SslConfigs.SSL_KEY_PASSWORD_CONFIG).value)
    assertEquals(password, config.getPassword(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG).value)
  }

  @Test
  def testConnectionsMaxReauthMsDefault(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertEquals(0L, config.valuesWithPrefixOverride("sasl_ssl.oauthbearer.").get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG).asInstanceOf[Long])
  }

  @Test
  def testConnectionsMaxReauthMsExplicit(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val expected = 3600000
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"sasl_ssl.oauthbearer.connections.max.reauth.ms=$expected")))
    assertEquals(expected, config.valuesWithPrefixOverride("sasl_ssl.oauthbearer.").get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS_CONFIG).asInstanceOf[Long])
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
