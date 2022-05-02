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
package kafka.admin

import kafka.admin.ConfigCommand.ConfigCommandOptions
import kafka.cluster.{Broker, EndPoint}
import kafka.server.{ConfigEntityName, KafkaConfig, QuorumTestHarness}
import kafka.utils.{Exit, Logging, TestInfoUtils}
import kafka.zk.{AdminZkClient, BrokerInfo}
import org.apache.kafka.common.config.ConfigException
import org.apache.kafka.common.network.ListenerName
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.server.common.MetadataVersion
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ConfigCommandIntegrationTest extends QuorumTestHarness with Logging {

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def shouldExitWithNonZeroStatusOnUpdatingUnallowedConfigViaZk(quorum: String): Unit = {
    assertNonZeroStatusExit(Array(
      "--zookeeper", zkConnect,
      "--entity-name", "1",
      "--entity-type", "brokers",
      "--alter",
      "--add-config", "security.inter.broker.protocol=PLAINTEXT"))
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def shouldExitWithNonZeroStatusOnZkCommandAlterUserQuota(quorum: String): Unit = {
    assertNonZeroStatusExit(Array(
      "--zookeeper", zkConnect,
      "--entity-type", "users",
      "--entity-name", "admin",
      "--alter", "--add-config", "consumer_byte_rate=20000"))
  }

  private def assertNonZeroStatusExit(args: Array[String]): Unit = {
    var exitStatus: Option[Int] = None
    Exit.setExitProcedure { (status, _) =>
      exitStatus = Some(status)
      throw new RuntimeException
    }

    try {
      ConfigCommand.main(args)
    } catch {
      case _: RuntimeException =>
    } finally {
      Exit.resetExitProcedure()
    }

    assertEquals(Some(1), exitStatus)
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk"))
  def testDynamicBrokerConfigUpdateUsingZooKeeper(quorum: String): Unit = {
    val brokerId = "1"
    val adminZkClient = new AdminZkClient(zkClient)
    val alterOpts = Array("--zookeeper", zkConnect, "--entity-type", "brokers", "--alter")

    def entityOpt(brokerId: Option[String]): Array[String] = {
      brokerId.map(id => Array("--entity-name", id)).getOrElse(Array("--entity-default"))
    }

    def alterConfigWithZk(configs: Map[String, String], brokerId: Option[String],
                          encoderConfigs: Map[String, String] = Map.empty): Unit = {
      val configStr = (configs ++ encoderConfigs).map { case (k, v) => s"$k=$v" }.mkString(",")
      val addOpts = new ConfigCommandOptions(alterOpts ++ entityOpt(brokerId) ++ Array("--add-config", configStr))
      ConfigCommand.alterConfigWithZk(zkClient, addOpts, adminZkClient)
    }

    def verifyConfig(configs: Map[String, String], brokerId: Option[String]): Unit = {
      val entityConfigs = zkClient.getEntityConfigs("brokers", brokerId.getOrElse(ConfigEntityName.Default))
      assertEquals(configs, entityConfigs.asScala)
    }

    def alterAndVerifyConfig(configs: Map[String, String], brokerId: Option[String]): Unit = {
      alterConfigWithZk(configs, brokerId)
      verifyConfig(configs, brokerId)
    }

    def deleteAndVerifyConfig(configNames: Set[String], brokerId: Option[String]): Unit = {
      val deleteOpts = new ConfigCommandOptions(alterOpts ++ entityOpt(brokerId) ++
        Array("--delete-config", configNames.mkString(",")))
      ConfigCommand.alterConfigWithZk(zkClient, deleteOpts, adminZkClient)
      verifyConfig(Map.empty, brokerId)
    }

    // Add config
    alterAndVerifyConfig(Map("message.max.size" -> "110000"), Some(brokerId))
    alterAndVerifyConfig(Map("message.max.size" -> "120000"), None)

    // Change config
    alterAndVerifyConfig(Map("message.max.size" -> "130000"), Some(brokerId))
    alterAndVerifyConfig(Map("message.max.size" -> "140000"), None)

    // Delete config
    deleteAndVerifyConfig(Set("message.max.size"), Some(brokerId))
    deleteAndVerifyConfig(Set("message.max.size"), None)

    // Listener configs: should work only with listener name
    alterAndVerifyConfig(Map("listener.name.external.ssl.keystore.location" -> "/tmp/test.jks"), Some(brokerId))
    assertThrows(classOf[ConfigException], () => alterConfigWithZk(Map("ssl.keystore.location" -> "/tmp/test.jks"), Some(brokerId)))

    // Per-broker config configured at default cluster-level should fail
    assertThrows(classOf[ConfigException], () => alterConfigWithZk(Map("listener.name.external.ssl.keystore.location" -> "/tmp/test.jks"), None))
    deleteAndVerifyConfig(Set("listener.name.external.ssl.keystore.location"), Some(brokerId))

    // Password config update without encoder secret should fail
    assertThrows(classOf[IllegalArgumentException], () => alterConfigWithZk(Map("listener.name.external.ssl.keystore.password" -> "secret"), Some(brokerId)))

    // Password config update with encoder secret should succeed and encoded password must be stored in ZK
    val configs = Map("listener.name.external.ssl.keystore.password" -> "secret", "log.cleaner.threads" -> "2")
    val encoderConfigs = Map(KafkaConfig.PasswordEncoderSecretProp -> "encoder-secret")
    alterConfigWithZk(configs, Some(brokerId), encoderConfigs)
    val brokerConfigs = zkClient.getEntityConfigs("brokers", brokerId)
    assertFalse(brokerConfigs.contains(KafkaConfig.PasswordEncoderSecretProp), "Encoder secret stored in ZooKeeper")
    assertEquals("2", brokerConfigs.getProperty("log.cleaner.threads")) // not encoded
    val encodedPassword = brokerConfigs.getProperty("listener.name.external.ssl.keystore.password")
    val passwordEncoder = ConfigCommand.createPasswordEncoder(encoderConfigs)
    assertEquals("secret", passwordEncoder.decode(encodedPassword).value)
    assertEquals(configs.size, brokerConfigs.size)

    // Password config update with overrides for encoder parameters
    val configs2 = Map("listener.name.internal.ssl.keystore.password" -> "secret2")
    val encoderConfigs2 = Map(KafkaConfig.PasswordEncoderSecretProp -> "encoder-secret",
      KafkaConfig.PasswordEncoderCipherAlgorithmProp -> "DES/CBC/PKCS5Padding",
      KafkaConfig.PasswordEncoderIterationsProp -> "1024",
      KafkaConfig.PasswordEncoderKeyFactoryAlgorithmProp -> "PBKDF2WithHmacSHA1",
      KafkaConfig.PasswordEncoderKeyLengthProp -> "64")
    alterConfigWithZk(configs2, Some(brokerId), encoderConfigs2)
    val brokerConfigs2 = zkClient.getEntityConfigs("brokers", brokerId)
    val encodedPassword2 = brokerConfigs2.getProperty("listener.name.internal.ssl.keystore.password")
    assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs).decode(encodedPassword2).value)
    assertEquals("secret2", ConfigCommand.createPasswordEncoder(encoderConfigs2).decode(encodedPassword2).value)


    // Password config update at default cluster-level should fail
    assertThrows(classOf[ConfigException], () => alterConfigWithZk(configs, None, encoderConfigs))

    // Dynamic config updates using ZK should fail if broker is running.
    registerBrokerInZk(brokerId.toInt)
    assertThrows(classOf[IllegalArgumentException], () => alterConfigWithZk(Map("message.max.size" -> "210000"), Some(brokerId)))
    assertThrows(classOf[IllegalArgumentException], () => alterConfigWithZk(Map("message.max.size" -> "220000"), None))

    // Dynamic config updates using ZK should for a different broker that is not running should succeed
    alterAndVerifyConfig(Map("message.max.size" -> "230000"), Some("2"))
  }

  private def registerBrokerInZk(id: Int): Unit = {
    zkClient.createTopLevelPaths()
    val securityProtocol = SecurityProtocol.PLAINTEXT
    val endpoint = new EndPoint("localhost", 9092, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)
    val brokerInfo = BrokerInfo(Broker(id, Seq(endpoint), rack = None), MetadataVersion.latest, jmxPort = 9192)
    zkClient.registerBroker(brokerInfo)
  }
}
