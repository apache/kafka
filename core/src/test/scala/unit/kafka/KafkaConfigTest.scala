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
    testZkBooleanConfigWithDefaultValue(KafkaConfig.ZkSslClientEnableProp, "zookeeper.ssl.client.enable",
      "zookeeper.client.secure", config => config.zkSslClientEnable, false)
  }
    @Test
  def testZkSslKeyStoreLocation(): Unit = {
    testZkStringConfigWithNoDefault(KafkaConfig.ZkSslKeyStoreLocationProp, "zookeeper.ssl.keystore.location",
      "zookeeper.ssl.keyStore.location", config => config.zkSslKeyStoreLocation)
  }

  @Test
  def testZkSslTrustStoreLocation(): Unit = {
    testZkStringConfigWithNoDefault(KafkaConfig.ZkSslTrustStoreLocationProp, "zookeeper.ssl.truststore.location",
      "zookeeper.ssl.trustStore.location", config => config.zkSslTrustStoreLocation)
  }

  @Test
  def testZookeeperKeyStorePassword(): Unit = {
    testZkPasswordConfig(KafkaConfig.ZkSslKeyStorePasswordProp, "zookeeper.ssl.keystore.password",
      "zookeeper.ssl.keyStore.password", config => config.zkSslKeyStorePassword)
  }

  @Test
  def testZookeeperTrustStorePassword(): Unit = {
    testZkPasswordConfig(KafkaConfig.ZkSslTrustStorePasswordProp, "zookeeper.ssl.truststore.password",
      "zookeeper.ssl.trustStore.password", config => config.zkSslTrustStorePassword)
  }

  @Test
  def testZkSslKeyStoreType(): Unit = {
    testZkStringConfigWithNoDefault(KafkaConfig.ZkSslKeyStoreTypeProp, "zookeeper.ssl.keystore.type",
      "zookeeper.ssl.keyStore.type", config => config.zkSslKeyStoreType)
  }

  @Test
  def testZkSslTrustStoreType(): Unit = {
    testZkStringConfigWithNoDefault(KafkaConfig.ZkSslTrustStoreTypeProp, "zookeeper.ssl.truststore.type",
      "zookeeper.ssl.trustStore.type", config => config.zkSslTrustStoreType)
  }

  @Test
  def testZkSslProtocol(): Unit = {
    testZkStringConfigWithDefaultValue(KafkaConfig.ZkSslProtocolProp, "zookeeper.ssl.protocol",
      "zookeeper.ssl.protocol", config => config.ZkSslProtocol, "TLSv1.2")
  }

  @Test
  def testZkSslEnabledProtocols(): Unit = {
    testZkListConfig(KafkaConfig.ZkSslEnabledProtocolsProp, "zookeeper.ssl.enabled.protocols",
      "zookeeper.ssl.enabledProtocols", config => config.ZkSslEnabledProtocols)
  }

  @Test
  def testZkSslCipherSuites(): Unit = {
    testZkListConfig(KafkaConfig.ZkSslCipherSuitesProp, "zookeeper.ssl.cipher.suites",
      "zookeeper.ssl.ciphersuites", config => config.ZkSslCipherSuites)
  }

  @Test
  def testZkSslEndpointIdentificationAlgorithm(): Unit = {
    // this property is different than the others
    // because the system property values and the Kafka property values don't match
    val kafkaPropName = KafkaConfig.ZkSslEndpointIdentificationAlgorithmProp
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
    Map("true" -> "HTTPS", "false" -> "").foreach { case (sysPropValue, expected) => {
      try {
        System.setProperty(sysProp, sysPropValue)
        val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
        assertNull(config.originals.get(kafkaPropName)) // doesn't appear in the originals
        assertEquals(expectedDefaultValue, config.values.get(kafkaPropName)) // default value appears in the values
        assertEquals(expected, config.ZkSslEndpointIdentificationAlgorithm) // system property impacts the ultimate value of the property
      } finally {
        System.clearProperty(sysProp)
      }
    }}
    // finally set Kafka config alone
    List("https", "").foreach(expected => {
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaPropName=${expected}")))
      assertEquals(expected, config.originals.get(kafkaPropName)) // appears in the originals
      assertEquals(expected, config.values.get(kafkaPropName)) // appears in the values
      assertEquals(expected, config.ZkSslEndpointIdentificationAlgorithm) // is the ultimate value
    })
  }

  @Test
  def testZkSslCrlEnable(): Unit = {
    testZkBooleanConfigWithDefaultValue(KafkaConfig.ZkSslCrlEnableProp, "zookeeper.ssl.crl.enable",
      "zookeeper.ssl.crl", config => config.ZkSslCrlEnable, false)
  }

  @Test
  def testZkSslOcspEnable(): Unit = {
    testZkBooleanConfigWithDefaultValue(KafkaConfig.ZkSslOcspEnableProp, "zookeeper.ssl.ocsp.enable",
      "zookeeper.ssl.ocsp", config => config.ZkSslOcspEnable, false)
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

  private def testZkBooleanConfigWithDefaultValue(kafkaPropName: String, expectedKafkaPropName: String, sysPropName: String,
                                                  getPropValueFrom: (KafkaConfig) => Boolean, expectedDefaultValue: Boolean): Unit = {
    assertEquals(expectedKafkaPropName, kafkaPropName)
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct default value
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.originals.get(kafkaPropName)) // doesn't appear in the originals
    assertEquals(expectedDefaultValue, emptyConfig.values.get(kafkaPropName)) // but default value appears in the values
    assertEquals(expectedDefaultValue, getPropValueFrom(emptyConfig)) // and has the correct default value
    // next set system property alone
    val expectedPropValue = !expectedDefaultValue
    try {
      System.setProperty(sysPropName, s"$expectedPropValue")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.originals.get(kafkaPropName)) // doesn't appear in the originals
      assertEquals(expectedDefaultValue, config.values.get(kafkaPropName)) // default value (different from system property) appears in the values
      assertEquals(expectedPropValue, getPropValueFrom(config)) // but system property does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysPropName)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaPropName=${expectedPropValue}")))
    assertEquals(expectedPropValue, config.values.get(kafkaPropName)) // appears in the values
    assertEquals(expectedPropValue, getPropValueFrom(config))
  }

  private def testZkStringConfigWithNoDefault(kafkaPropName: String, expectedKafkaPropName: String, sysPropName: String,
                                              getPropValueFrom: (KafkaConfig) => Option[String]): Unit = {
    assertEquals(expectedKafkaPropName, kafkaPropName)
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.originals.get(kafkaPropName)) // doesn't appear in the originals
    assertNull(emptyConfig.values.get(kafkaPropName)) // doesn't appear in the values
    assertEquals(None, getPropValueFrom(emptyConfig)) // has no default value
    // next set system property alone
    val expectedPropValue = "foo"
    try {
      System.setProperty(sysPropName, s"$expectedPropValue")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.originals.get(kafkaPropName)) // doesn't appear in the originals
      assertNull(config.values.get(kafkaPropName)) // doesn't appear in the values
      assertEquals(Some(expectedPropValue), getPropValueFrom(config)) // but system property does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysPropName)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaPropName=${expectedPropValue}")))
    assertEquals(expectedPropValue, config.values.get(kafkaPropName)) // appears in the values
    assertEquals(Some(expectedPropValue), getPropValueFrom(config))
  }

  private def testZkStringConfigWithDefaultValue(kafkaPropName: String, expectedKafkaPropName: String, sysPropName: String,
                                                 getPropValueFrom: (KafkaConfig) => String, expectedDefaultValue: String): Unit = {
    assertEquals(expectedKafkaPropName, kafkaPropName)
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is the correct default value
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.originals.get(kafkaPropName)) // doesn't appear in the originals
    assertEquals(expectedDefaultValue, emptyConfig.values.get(kafkaPropName)) // but default value appears in the values
    assertEquals(expectedDefaultValue, getPropValueFrom(emptyConfig)) // and has the correct default value
    // next set system property alone
    val expectedPropValue = "foo"
    try {
      System.setProperty(sysPropName, s"$expectedPropValue")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.originals.get(kafkaPropName)) // doesn't appear in the originals
      assertEquals(expectedDefaultValue, config.values.get(kafkaPropName)) // default value (different from system property) appears in the values
      assertEquals(expectedPropValue, getPropValueFrom(config)) // but system property does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysPropName)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaPropName=${expectedPropValue}")))
    assertEquals(expectedPropValue, config.values.get(kafkaPropName)) // appears in the values
    assertEquals(expectedPropValue, getPropValueFrom(config))
  }

  private def testZkPasswordConfig(kafkaPropName: String, expectedKafkaPropName: String, sysPropName: String,
                                   getPropValueFrom: (KafkaConfig) => Option[Password]): Unit = {
    assertEquals(expectedKafkaPropName, kafkaPropName)
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.originals.get(kafkaPropName)) // doesn't appear in the originals
    assertNull(emptyConfig.values.get(kafkaPropName)) // doesn't appear in the values
    assertEquals(None, getPropValueFrom(emptyConfig)) // has no default value
    // next set system property alone
    val expectedPropValue = "ThePa$$word!"
    try {
      System.setProperty(sysPropName, s"$expectedPropValue")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.originals.get(kafkaPropName)) // doesn't appear in the originals
      assertNull(config.values.get(kafkaPropName)) // doesn't appear in the values
      assertEquals(Some(new Password(expectedPropValue)), getPropValueFrom(config)) // but system property does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysPropName)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaPropName=${expectedPropValue}")))
    assertEquals(new Password(expectedPropValue), config.values.get(kafkaPropName)) // appears in the values
    assertEquals(Some(new Password(expectedPropValue)), getPropValueFrom(config))
  }

  private def testZkListConfig(kafkaPropName: String, expectedKafkaPropName: String, sysPropName: String,
                               getPropValueFrom: (KafkaConfig) => Option[util.List[String]]): Unit = {
    assertEquals(expectedKafkaPropName, kafkaPropName)
    val propertiesFile = prepareDefaultConfig()
    // first make sure there is no default
    val emptyConfig = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
    assertNull(emptyConfig.originals.get(kafkaPropName)) // doesn't appear in the originals
    assertNull(emptyConfig.values.get(kafkaPropName)) // doesn't appear in the values
    assertEquals(None, getPropValueFrom(emptyConfig)) // has no default value
    // next set system property alone
    val expectedPropValue = List("A", "B")
    try {
      System.setProperty(sysPropName, s"${expectedPropValue.mkString(",")}")
      // need to create a new Kafka config for the system property to be recognized
      val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile)))
      assertNull(config.originals.get(kafkaPropName)) // doesn't appear in the originals
      assertNull(config.values.get(kafkaPropName)) // doesn't appear in the values
      assertEquals(Some(expectedPropValue.asJava), getPropValueFrom(config)) // but system property does impact the ultimate value of the property
    } finally {
      System.clearProperty(sysPropName)
    }
    // finally set Kafka config alone
    val config = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", s"$kafkaPropName=${expectedPropValue.mkString(",")}")))
    assertEquals(expectedPropValue.asJava, config.values.get(kafkaPropName)) // appears in the values
    assertEquals(Some(expectedPropValue.asJava), getPropValueFrom(config))
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
