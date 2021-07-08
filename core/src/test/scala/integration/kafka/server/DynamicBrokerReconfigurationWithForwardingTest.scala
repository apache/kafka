/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import java.io.File
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.controller.{ControllerBrokerStateInfo, ControllerChannelManager}
import kafka.server.AbstractDynamicBrokerReconfigurationTest.{SecureExternal, SecureInternal}
import kafka.utils.Implicits._
import kafka.utils.TestUtils
import org.apache.kafka.common.config.SslConfigs.{SSL_KEYSTORE_LOCATION_CONFIG, SSL_TRUSTSTORE_LOCATION_CONFIG}
import org.apache.kafka.common.network.CertStores.{KEYSTORE_PROPS, TRUSTSTORE_PROPS}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.junit.jupiter.api.Test

import scala.collection.mutable

class DynamicBrokerReconfigurationWithForwardingTest extends AbstractDynamicBrokerReconfigurationTest {

  override def enableForwarding: Boolean = true

  @Test
  def testKeyStoreAlter(): Unit = {
    val topic2 = "testtopic2"
    TestUtils.createTopic(zkClient, topic2, numPartitions, replicationFactor = numServers, servers)

    // Start a producer and consumer that work with the current broker keystore.
    // This should continue working while changes are made
    val (producerThread, consumerThread) = startProduceConsume(retries = 0)
    TestUtils.waitUntilTrue(() => consumerThread.received >= 10, "Messages not received")

    // Producer with new truststore should fail to connect before keystore update
    val producer1 = ProducerBuilder().trustStoreProps(sslProperties2).maxRetries(0).build()
    verifyAuthenticationFailure(producer1)

    // Update broker keystore for external listener
    alterSslKeystoreUsingConfigCommand(sslProperties2, SecureExternal)

    // New producer with old truststore should fail to connect
    val producer2 = ProducerBuilder().trustStoreProps(sslProperties1).maxRetries(0).build()
    verifyAuthenticationFailure(producer2)

    // Produce/consume should work with new truststore with new producer/consumer
    val producer = ProducerBuilder().trustStoreProps(sslProperties2).maxRetries(0).build()
    val consumer = ConsumerBuilder("group1").trustStoreProps(sslProperties2).topic(topic2).build()
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with incompatible keystore should fail without update
    val adminClient = adminClients.head
    alterSslKeystore(adminClient, sslProperties2, SecureInternal, expectFailure = true)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Broker keystore update for internal listener with compatible keystore should succeed
    val sslPropertiesCopy = sslProperties1.clone().asInstanceOf[Properties]
    val oldFile = new File(sslProperties1.getProperty(SSL_KEYSTORE_LOCATION_CONFIG))
    val newFile = File.createTempFile("keystore", ".jks")
    Files.copy(oldFile.toPath, newFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    sslPropertiesCopy.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, newFile.getPath)
    alterSslKeystore(adminClient, sslPropertiesCopy, SecureInternal)
    verifyProduceConsume(producer, consumer, 10, topic2)

    // Verify that keystores can be updated using same file name.
    val reusableProps = sslProperties2.clone().asInstanceOf[Properties]
    val reusableFile = File.createTempFile("keystore", ".jks")
    reusableProps.setProperty(SSL_KEYSTORE_LOCATION_CONFIG, reusableFile.getPath)
    Files.copy(new File(sslProperties1.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)).toPath,
      reusableFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    alterSslKeystore(adminClient, reusableProps, SecureExternal)
    val producer3 = ProducerBuilder().trustStoreProps(sslProperties2).maxRetries(0).build()
    verifyAuthenticationFailure(producer3)
    // Now alter using same file name. We can't check if the update has completed by comparing config on
    // the broker, so we wait for producer operation to succeed to verify that the update has been performed.
    Files.copy(new File(sslProperties2.getProperty(SSL_KEYSTORE_LOCATION_CONFIG)).toPath,
      reusableFile.toPath, StandardCopyOption.REPLACE_EXISTING)
    reusableFile.setLastModified(System.currentTimeMillis() + 1000)
    alterSslKeystore(adminClient, reusableProps, SecureExternal)
    TestUtils.waitUntilTrue(() => {
      try {
        producer3.partitionsFor(topic).size() == numPartitions
      } catch {
        case _: Exception  => false
      }
    }, "Keystore not updated")

    // Verify that all messages sent with retries=0 while keystores were being altered were consumed
    stopAndVerifyProduceConsume(producerThread, consumerThread)
  }

  @Test
  def testTrustStoreAlter(): Unit = {
    val producerBuilder = ProducerBuilder().listenerName(SecureInternal).securityProtocol(SecurityProtocol.SSL)

    // Producer with new keystore should fail to connect before truststore update
    verifyAuthenticationFailure(producerBuilder.keyStoreProps(sslProperties2).build())

    // Update broker truststore for SSL listener with both certificates
    val combinedStoreProps = mergeTrustStores(sslProperties1, sslProperties2)
    val prefix = listenerPrefix(SecureInternal)
    val existingDynamicProps = new Properties
    servers.head.config.dynamicConfig.currentDynamicBrokerConfigs.foreach { case (k, v) =>
      existingDynamicProps.put(k, v)
    }
    val newProps = new Properties
    newProps ++= existingDynamicProps
    newProps ++= securityProps(combinedStoreProps, TRUSTSTORE_PROPS, prefix)
    reconfigureServers(newProps, perBrokerConfig = true,
      (s"$prefix$SSL_TRUSTSTORE_LOCATION_CONFIG", combinedStoreProps.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)))

    def verifySslProduceConsume(keyStoreProps: Properties, group: String, waitTime: Int = 10): Unit = {
      val producer = producerBuilder.keyStoreProps(keyStoreProps).build()
      val consumer = ConsumerBuilder(group)
        .listenerName(SecureInternal)
        .securityProtocol(SecurityProtocol.SSL)
        .keyStoreProps(keyStoreProps)
        .autoOffsetReset("latest")
        .build()
      verifyProduceConsume(producer, consumer, 10, topic, waitTime)
    }

    // Produce/consume should work with old as well as new client keystore
    verifySslProduceConsume(sslProperties1, "alter-truststore-1")
    verifySslProduceConsume(sslProperties2, "alter-truststore-2")

    // Revert to old truststore with only one certificate and update. Clients should connect only with old keystore.
    val oldTruststoreProps = new Properties
    oldTruststoreProps ++= existingDynamicProps
    oldTruststoreProps ++= securityProps(sslProperties1, TRUSTSTORE_PROPS, prefix)
    reconfigureServers(oldTruststoreProps, perBrokerConfig = true,
      (s"$prefix$SSL_TRUSTSTORE_LOCATION_CONFIG", sslProperties1.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)))
    verifyAuthenticationFailure(producerBuilder.keyStoreProps(sslProperties2).build())
    verifySslProduceConsume(sslProperties1, "alter-truststore-3")

    // Update same truststore file to contain both certificates without changing any configs.
    // Clients should connect successfully with either keystore after admin client AlterConfigsRequest completes.
    Files.copy(Paths.get(combinedStoreProps.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)),
      Paths.get(sslProperties1.getProperty(SSL_TRUSTSTORE_LOCATION_CONFIG)),
      StandardCopyOption.REPLACE_EXISTING)

    TestUtils.incrementalAlterConfigs(servers, adminClients.head, oldTruststoreProps, perBrokerConfig = true).all.get()
    verifySslProduceConsume(sslProperties1, "alter-truststore-4")

    // Sleep to wait for config changes propagation.
    Thread.sleep(10000)

    verifySslProduceConsume(sslProperties2, "alter-truststore-5")

    // Update internal keystore/truststore and validate new client connections from broker (e.g. controller).
    // Alter internal keystore from `sslProperties1` to `sslProperties2`, force disconnect of a controller connection
    // and verify that metadata is propagated for new topic.
    val props2 = securityProps(sslProperties2, KEYSTORE_PROPS, prefix)
    props2 ++= securityProps(combinedStoreProps, TRUSTSTORE_PROPS, prefix)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props2, perBrokerConfig = true).all.get(15, TimeUnit.SECONDS)
    verifySslProduceConsume(sslProperties2, "alter-truststore-6")
    props2 ++= securityProps(sslProperties2, TRUSTSTORE_PROPS, prefix)
    TestUtils.incrementalAlterConfigs(servers, adminClients.head, props2, perBrokerConfig = true).all.get(15, TimeUnit.SECONDS)
    verifySslProduceConsume(sslProperties2, "alter-truststore-7")
    waitForAuthenticationFailure(producerBuilder.keyStoreProps(sslProperties1))

    val controller = servers.find(_.config.brokerId == TestUtils.waitUntilControllerElected(zkClient)).get
    val controllerChannelManager = controller.kafkaController.controllerChannelManager
    val brokerStateInfo: mutable.HashMap[Int, ControllerBrokerStateInfo] =
      JTestUtils.fieldValue(controllerChannelManager, classOf[ControllerChannelManager], "brokerStateInfo")
    brokerStateInfo(0).networkClient.disconnect("0")
    TestUtils.createTopic(zkClient, "testtopic2", numPartitions, replicationFactor = numServers, servers)
  }
}
