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

import java.io.File
import java.nio.file.Files
import java.util

import kafka.server.KafkaConfig
import kafka.utils.Exit
import org.apache.kafka.common.config.types.Password
import org.apache.kafka.common.internals.FatalExitError
import org.junit.{After, Before, Test}
import org.junit.Assert._
import org.apache.kafka.common.config.internals.BrokerSecurityConfigs

import scala.collection.JavaConverters._

class KafkaTest {

  @Before
  def setUp(): Unit = Exit.setExitProcedure((status, _) => throw new FatalExitError(status))

  @After
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

  @Test(expected = classOf[FatalExitError])
  def testGetKafkaConfigFromArgsNonArgsAtTheEnd(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "broker.id=1", "broker.id=2")))
  }

  @Test(expected = classOf[FatalExitError])
  def testGetKafkaConfigFromArgsNonArgsOnly(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "broker.id=1", "broker.id=2")))
  }

  @Test(expected = classOf[FatalExitError])
  def testGetKafkaConfigFromArgsNonArgsAtTheBegging(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "broker.id=1", "--override", "broker.id=2")))
  }

  @Test
  def testKafkaSslPasswords(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "ssl.keystore.password=keystore_password",
                                                                                    "--override", "ssl.key.password=key_password",
                                                                                    "--override", "ssl.truststore.password=truststore_password")))
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslKeyPasswordProp).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslKeystorePasswordProp).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslTruststorePasswordProp).toString)

    assertEquals("key_password", config.getPassword(KafkaConfig.SslKeyPasswordProp).value)
    assertEquals("keystore_password", config.getPassword(KafkaConfig.SslKeystorePasswordProp).value)
    assertEquals("truststore_password", config.getPassword(KafkaConfig.SslTruststorePasswordProp).value)
  }

  @Test
  def testKafkaSslPasswordsWithSymbols(): Unit = {
    val password = "=!#-+!?*/\"\'^%$=\\.,@:;="
    val propertiesFile = prepareDefaultConfig()
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile,
      "--override", "ssl.keystore.password=" + password,
      "--override", "ssl.key.password=" + password,
      "--override", "ssl.truststore.password=" + password)))
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslKeyPasswordProp).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslKeystorePasswordProp).toString)
    assertEquals(Password.HIDDEN, config.getPassword(KafkaConfig.SslTruststorePasswordProp).toString)

    assertEquals(password, config.getPassword(KafkaConfig.SslKeystorePasswordProp).value)
    assertEquals(password, config.getPassword(KafkaConfig.SslKeyPasswordProp).value)
    assertEquals(password, config.getPassword(KafkaConfig.SslTruststorePasswordProp).value)
  }

  @Test
  def testZkSslClientEnable(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslClientEnableProp
    assertEquals("zookeeper.ssl.client.enable", kafkaProp)
    val sysProp = "zookeeper.client.secure"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct "default" value
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertFalse(emptyConfig.zkSslClientEnable) // but still has the correct "default" value
    // next set system property alone
    val expected = true
    try {
      System.setProperty(sysProp, s"$expected")
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(expected, config.zkSslClientEnable) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
    assertEquals(expected, config.zkSslClientEnable)
  }

  @Test
  def testZkSslKeyStoreLocation(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslKeyStoreLocationProp
    assertEquals("zookeeper.ssl.keystore.location", kafkaProp)
    val sysProp = "zookeeper.ssl.keyStore.location"
    val expected = "/the/location"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals(None, emptyConfig.zkSslKeyStoreLocation) // has no default value
    // next set system property alone
    try {
      System.setProperty(sysProp, s"$expected")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(Some(expected), config.zkSslKeyStoreLocation) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
    assertEquals(Some(expected), config.zkSslKeyStoreLocation)
  }

  @Test
  def testZkSslTrustStoreLocation(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslTrustStoreLocationProp
    assertEquals("zookeeper.ssl.truststore.location", kafkaProp)
    val sysProp = "zookeeper.ssl.trustStore.location"
    val expected = "/the/location"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals(None, emptyConfig.zkSslTrustStoreLocation) // has no default value
    // next set system property alone
    try {
      System.setProperty(sysProp, s"$expected")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(Some(expected), config.zkSslTrustStoreLocation) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
    assertEquals(Some(expected), config.zkSslTrustStoreLocation)
  }

  @Test
  def testZookeeperKeyStorePassword(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslKeyStorePasswordProp
    assertEquals("zookeeper.ssl.keystore.password", kafkaProp)
    val sysProp = "zookeeper.ssl.keyStore.password"
    val expected = "ThePa$$word!"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals(None, emptyConfig.zkSslKeyStorePassword) // has no default value
    // next set system property alone
    try {
      System.setProperty(sysProp, s"$expected")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(Some(new Password(expected)), config.zkSslKeyStorePassword) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(new Password(expected), config.values.get(kafkaProp)) // appears in the values
    assertEquals(Some(new Password(expected)), config.zkSslKeyStorePassword)
  }

  @Test
  def testZookeeperTrustStorePassword(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslTrustStorePasswordProp
    assertEquals("zookeeper.ssl.truststore.password", kafkaProp)
    val sysProp = "zookeeper.ssl.trustStore.password"
    val expected = "ThePa$$word!"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals(None, emptyConfig.zkSslTrustStorePassword) // has no default value
    // next set system property alone
    try {
      System.setProperty(sysProp, s"$expected")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(Some(new Password(expected)), config.zkSslTrustStorePassword) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(new Password(expected), config.values.get(kafkaProp)) // appears in the values
    assertEquals(Some(new Password(expected)), config.zkSslTrustStorePassword)
  }

  @Test
  def testZkSslKeyStoreType(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslKeyStoreTypeProp
    assertEquals("zookeeper.ssl.keystore.type", kafkaProp)
    val sysProp = "zookeeper.ssl.keyStore.type"
    val expected = "PEM"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals(None, emptyConfig.zkSslKeyStoreType) // has no default value
    // next set system property alone
    try {
      System.setProperty(sysProp, s"$expected")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(Some(expected), config.zkSslKeyStoreType) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
    assertEquals(Some(expected), config.zkSslKeyStoreType)
  }

  @Test
  def testZkSslTrustStoreType(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslTrustStoreTypeProp
    assertEquals("zookeeper.ssl.truststore.type", kafkaProp)
    val sysProp = "zookeeper.ssl.trustStore.type"
    val expected = "PEM"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals(None, emptyConfig.zkSslTrustStoreType) // has no default value
    // next set system property alone
    try {
      System.setProperty(sysProp, s"$expected")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(Some(expected), config.zkSslTrustStoreType) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
    assertEquals(Some(expected), config.zkSslTrustStoreType)
  }

  @Test
  def testZkSslProtocol(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslProtocolProp
    assertEquals("zookeeper.ssl.protocol", kafkaProp)
    val sysProp = "zookeeper.ssl.protocol"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct "default" value
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals("TLSv1.2", emptyConfig.ZkSslProtocol) // but still has the correct "default" value
    // next set system property alone
    val expected = "TheProtocol"
    try {
      System.setProperty(sysProp, s"$expected")
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(expected, config.ZkSslProtocol) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
    assertEquals(expected, config.ZkSslProtocol)
  }

  @Test
  def testZkSslEnabledProtocols(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslEnabledProtocolsProp
    assertEquals("zookeeper.ssl.enabled.protocols", kafkaProp)
    val sysProp = "zookeeper.ssl.enabledProtocols"
    val expected = List("PROTOCOL_A", "PROTOCOL_B")
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals(None, emptyConfig.ZkSslEnabledProtocols) // has no default value
    // next set system property alone
    try {
      System.setProperty(sysProp, s"${expected.mkString(",")}")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(Some(expected.asJava), config.ZkSslEnabledProtocols) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected.mkString(",")}")))
    assertEquals(expected.asJava, config.values.get(kafkaProp)) // appears in the values
    assertEquals(Some(expected.asJava), config.ZkSslEnabledProtocols)
  }

  @Test
  def testZkSslCipherSuites(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslCipherSuitesProp
    assertEquals("zookeeper.ssl.cipher.suites", kafkaProp)
    val sysProp = "zookeeper.ssl.ciphersuites"
    val expected = List("CIPHER_A", "CIPHER_B")
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals(None, emptyConfig.ZkSslCipherSuites) // has no default value
    // next set system property alone
    try {
      System.setProperty(sysProp, s"${expected.mkString(",")}")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(Some(expected.asJava), config.ZkSslCipherSuites) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected.mkString(",")}")))
    assertEquals(expected.asJava, config.values.get(kafkaProp)) // appears in the values
    assertEquals(Some(expected.asJava), config.ZkSslCipherSuites)
  }

  @Test
  def testZkSslEndpointIdentificationAlgorithm(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp
    assertEquals("zookeeper.ssl.endpoint.identification.algorithm", kafkaProp)
    val sysProp = "zookeeper.ssl.hostnameVerification"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct "default" value
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertEquals("HTTPS", emptyConfig.ZkSslEndpointIdentificationAlgorithm) // but still has the correct "default" value
    // next set system property alone
    Map("true" -> "HTTPS", "false" -> "").foreach { case (sysPropValue, expected) => {
      try {
        System.setProperty(sysProp, sysPropValue)
        val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
        assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
        assertEquals(expected, config.ZkSslEndpointIdentificationAlgorithm) // but does impact the ultimate value of the property
      } finally {
        System.clearProperty(sysProp)
      }
    }}
    // finally set Kafka config alone
    List("https", "").foreach(expected => {
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
      assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
      assertEquals(expected, config.ZkSslEndpointIdentificationAlgorithm)
    })
  }

  @Test
  def testZkSslCrlEnable(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslCrlEnableProp
    assertEquals("zookeeper.ssl.crl.enable", kafkaProp)
    val sysProp = "zookeeper.ssl.crl"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct "default" value
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertFalse(emptyConfig.ZkSslCrlEnable) // but still has the correct "default" value
    // next set system property alone
    val expected = true
    try {
      System.setProperty(sysProp, s"$expected")
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(expected, config.ZkSslCrlEnable) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
    assertEquals(expected, config.ZkSslCrlEnable)
  }

  @Test
  def testZkSslOcspEnable(): Unit = {
    val kafkaProp = KafkaConfig.ZkSslOcspEnableProp
    assertEquals("zookeeper.ssl.ocsp.enable", kafkaProp)
    val sysProp = "zookeeper.ssl.ocsp"
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct "default" value
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.values.get(kafkaProp)) // doesn't appear in the values
    assertFalse(emptyConfig.ZkSslOcspEnable) // but still has the correct "default" value
    // next set system property alone
    val expected = true
    try {
      System.setProperty(sysProp, s"$expected")
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.values.get(kafkaProp)) // doesn't appear in the values
      assertEquals(expected, config.ZkSslOcspEnable) // but does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysProp)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaProp=${expected}")))
    assertEquals(expected, config.values.get(kafkaProp)) // appears in the values
    assertEquals(expected, config.ZkSslOcspEnable)
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
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"sasl_ssl.oauthbearer.connections.max.reauth.ms=${expected}")))
    assertEquals(expected, config.valuesWithPrefixOverride("sasl_ssl.oauthbearer.").get(BrokerSecurityConfigs.CONNECTIONS_MAX_REAUTH_MS).asInstanceOf[Long])
  }

  def prepareDefaultConfig(): String = {
    prepareConfig(Array("broker.id=1", "zookeeper.connect=somewhere"))
  }

  def prepareConfig(lines : Array[String]): String = {
    val file = File.createTempFile("kafkatest", ".properties")
    file.deleteOnExit()

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
