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

package kafka.integration

import java.io.File
import java.util.ArrayList
import java.util.Arrays
import java.util.concurrent.ExecutionException
import java.util.Properties

import kafka.api.SaslTestHarness
import kafka.admin.AclCommand
import kafka.cluster.EndPoint
import kafka.common.{ErrorMapping, TopicAndPartition, KafkaException}
import kafka.coordinator.GroupCoordinator
import kafka.security.auth._
import kafka.server._
import kafka.utils._
import kafka.zk.ZooKeeperTestHarness
import org.apache.kafka.clients.consumer.{OffsetAndMetadata, Consumer, ConsumerRecord, ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.junit.Assert._
import org.junit.{Test, After, Before}

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.collection.mutable.Buffer

trait SecurityTestHarness extends ZooKeeperTestHarness with SaslTestHarness{
  var instanceConfigs: Seq[KafkaConfig] = null
  var servers: Buffer[KafkaServer] = null
  var brokerList: String = null
  var alive: Array[Boolean] = null
  
  val producerCount: Int
  val consumerCount: Int
  val serverCount: Int
  val numRecords: Int
  val group: String
  val brokerId: Integer = 0
  val correlationId = 0
  
  val kafkaPrincipalType = KafkaPrincipal.USER_TYPE
  val clientPrincipal = s"$kafkaPrincipalType:client"
  val kafkaPrincipal = s"$kafkaPrincipalType:kafka"
  
  override protected val zkSaslEnabled = true
  protected def securityProtocol = SecurityProtocol.SASL_SSL
  protected lazy val trustStoreFile = Some(File.createTempFile("truststore", ".jks"))
  lazy val producerConfig = new Properties
  lazy val consumerConfig = new Properties
  lazy val serverConfig = new Properties

  val consumers = Buffer[KafkaConsumer[Array[Byte], Array[Byte]]]()
  val producers = Buffer[KafkaProducer[Array[Byte], Array[Byte]]]()

  def clusterAclArgs: Array[String] = Array("--authorizer-properties", 
                                            s"zookeeper.connect=$zkConnect",
                                            s"--add",
                                            s"--cluster",
                                            s"--allow-principal=$kafkaPrincipal")
  def generateConfigs() = {
    val cfgs = TestUtils.createBrokerConfigs(serverCount, zkConnect, interBrokerSecurityProtocol = Some(securityProtocol),
      trustStoreFile = trustStoreFile)
    cfgs.foreach(_.putAll(serverConfig))
    cfgs.map(KafkaConfig.fromProps)
  }
  
  def configs: Seq[KafkaConfig] = {
    if (instanceConfigs == null)
      instanceConfigs = generateConfigs()
    instanceConfigs
  }

  def serverForId(id: Int) = servers.find(s => s.config.brokerId == id)
  
  System.setProperty("zookeeper.authProvider.1", "org.apache.zookeeper.server.auth.SASLAuthenticationProvider")
  this.serverConfig.setProperty(KafkaConfig.HostNameProp, "127.0.0.1")
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "true")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicPartitionsProp, "1")
  this.serverConfig.setProperty(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
  this.serverConfig.setProperty(KafkaConfig.AuthorizerClassNameProp, classOf[SimpleAclAuthorizer].getName)

  @Before
  override def setUp() {
    // First bootstrap ZooKeeper
    super.setUp()
    // Set the cluster acl so that we can start brokers
    AclCommand.main(clusterAclArgs)
    // Starte brokers
    if(configs.size <= 0)
      throw new KafkaException("Must supply at least one server config.")
    servers = configs.map(TestUtils.createServer(_)).toBuffer
    brokerList = TestUtils.getBrokerListStrFromServers(servers, securityProtocol)
    alive = new Array[Boolean](servers.length)
    Arrays.fill(alive, true)
    // Configure producers and consumers 
    producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.putAll(TestUtils.producerSecurityConfigs(securityProtocol, trustStoreFile))
    consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig.putAll(TestUtils.consumerSecurityConfigs(securityProtocol, trustStoreFile))
    
    for (i <- 0 until producerCount)
      producers += TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers, securityProtocol), 
                                               acks = 1,
                                               securityProtocol = this.securityProtocol,
                                               trustStoreFile = this.trustStoreFile,
                                               props = Some(producerConfig))
    for (i <- 0 until consumerCount)
      consumers += TestUtils.createNewConsumer(TestUtils.getBrokerListStrFromServers(servers, securityProtocol), 
                                               groupId = group,
                                               securityProtocol = this.securityProtocol,
                                               trustStoreFile = this.trustStoreFile)

    TestUtils.createTopic(zkUtils, GroupCoordinator.GroupMetadataTopicName,
      serverConfig.getProperty(KafkaConfig.OffsetsTopicPartitionsProp).toInt,
      serverConfig.getProperty(KafkaConfig.OffsetsTopicReplicationFactorProp).toInt,
      servers,
      servers.head.consumerCoordinator.offsetsTopicConfigs)
  }
  
  @After
  override def tearDown() {
    producers.foreach(_.close())
    consumers.foreach(_.close())
    servers.foreach(_.shutdown())
    servers.foreach(_.config.logDirs.foreach(CoreUtils.rm(_)))
    super.tearDown
  }
  
}