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

import java.io.{FileOutputStream, File}
import java.security.Permission

import kafka.server.KafkaConfig
import org.apache.kafka.common.config.SslConfigs
import org.apache.kafka.common.config.types.Password
import org.junit.{After, Before, Test}
import org.junit.Assert._

class KafkaTest {

  val originalSecurityManager: SecurityManager = System.getSecurityManager

  class ExitCalled extends SecurityException {
  }

  private class NoExitSecurityManager extends SecurityManager {
    override def checkExit(status: Int): Unit = {
      throw new ExitCalled
    }

    override def checkPermission(perm : Permission): Unit = {
    }

    override def checkPermission(perm : Permission, context: Object): Unit = {
    }
  }

  @Before
  def setSecurityManager() : Unit = {
    System.setSecurityManager(new NoExitSecurityManager)
  }

  @After
  def setOriginalSecurityManager() : Unit = {
    System.setSecurityManager(originalSecurityManager)
  }

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
    assertEquals("compact", config3.logCleanupPolicy)

    // We should be also able to set several properties
    val config4 = KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "log.cleanup.policy=compact", "--override", "broker.id=2")))
    assertEquals(2, config4.brokerId)
    assertEquals("compact", config4.logCleanupPolicy)
  }

  @Test(expected = classOf[ExitCalled])
  def testGetKafkaConfigFromArgsWrongSetValue(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "a=b=c")))
  }

  @Test(expected = classOf[ExitCalled])
  def testGetKafkaConfigFromArgsNonArgsAtTheEnd(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "--override", "broker.id=1", "broker.id=2")))
  }

  @Test(expected = classOf[ExitCalled])
  def testGetKafkaConfigFromArgsNonArgsOnly(): Unit = {
    val propertiesFile = prepareDefaultConfig()
    KafkaConfig.fromProps(Kafka.getPropsFromArgs(Array(propertiesFile, "broker.id=1", "broker.id=2")))
  }

  @Test(expected = classOf[ExitCalled])
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
    assertEquals(Password.HIDDEN, config.sslKeyPassword.toString)
    assertEquals(Password.HIDDEN, config.sslKeystorePassword.toString)
    assertEquals(Password.HIDDEN, config.sslTruststorePassword.toString)

    assertEquals("key_password", config.sslKeyPassword.value)
    assertEquals("keystore_password", config.sslKeystorePassword.value)
    assertEquals("truststore_password", config.sslTruststorePassword.value)
  }

  def prepareDefaultConfig(): String = {
    prepareConfig(Array("broker.id=1", "zookeeper.connect=somewhere"))
  }

  def prepareConfig(lines : Array[String]): String = {
    val file = File.createTempFile("kafkatest", ".properties")
    file.deleteOnExit()

    val writer = new FileOutputStream(file)
    lines.foreach { l =>
      writer.write(l.getBytes)
      writer.write("\n".getBytes)
    }

    writer.close

    file.getAbsolutePath
  }
}
