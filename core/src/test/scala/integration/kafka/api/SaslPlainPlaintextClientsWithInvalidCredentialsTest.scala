/**
  * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
  * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
  * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
  * License. You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
  * specific language governing permissions and limitations under the License.
  */
package kafka.api

import java.io.FileOutputStream
import java.util.Locale
import java.util.concurrent.{ExecutionException, Future}
import scala.collection.JavaConverters.seqAsJavaListConverter

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.AuthenticationFailedException
import org.apache.kafka.common.protocol.SecurityProtocol
import org.junit.{After, Before, Ignore, Test}

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, KafkaConsumerGroupService}
import kafka.server.KafkaConfig
import kafka.utils.{JaasTestUtils, TestUtils}

class SaslPlainPlaintextClientsWithInvalidCredentialsTest extends BaseConsumerTest with SaslSetup {
  private val kafkaClientSaslMechanism = "PLAIN"
  private val kafkaServerSaslMechanisms = List(kafkaClientSaslMechanism)
  private val kafkaServerJaasEntryName =
    s"${listenerName.value.toLowerCase(Locale.ROOT)}.${JaasTestUtils.KafkaServerContextName}"
  this.serverConfig.setProperty(KafkaConfig.ZkEnableSecureAclsProp, "false")
  override protected def securityProtocol = SecurityProtocol.SASL_PLAINTEXT
  override protected val serverSaslProperties = Some(kafkaServerSaslProperties(kafkaServerSaslMechanisms, kafkaClientSaslMechanism))
  override protected val clientSaslProperties = Some(kafkaClientSaslProperties(kafkaClientSaslMechanism))

  @Before
  override def setUp(): Unit = {
    startSasl(jaasSectionsInvalidClientCredentials(kafkaServerSaslMechanisms, Some(kafkaClientSaslMechanism), KafkaSasl, kafkaServerJaasEntryName))
    super.setUp()
  }

  @After
  override def tearDown(): Unit = {
    super.tearDown()
    closeSasl()
  }

  protected def send(numRecords: Int): Seq[Future[RecordMetadata]] =
    send(numRecords, tp)

  protected def send(numRecords: Int, tp: TopicPartition): Seq[Future[RecordMetadata]] =
    send(this.producers.head, numRecords, tp)

  protected def send(producer: KafkaProducer[Array[Byte], Array[Byte]], numRecords: Int,
                            tp: TopicPartition): Seq[Future[RecordMetadata]] = {
    val records = (0 until numRecords).map { i =>
      val record = new ProducerRecord(tp.topic(), tp.partition(), i.toLong, s"key $i".getBytes, s"value $i".getBytes)
      producer.send(record)
    }
    producer.flush()
    records
  }

  @Test(expected = classOf[AuthenticationFailedException])
  override def testCoordinatorFailover() {
    super.testCoordinatorFailover()
  }

  @Test
  def testSimpleProduction() {
    try {
      val futures = send(1)
      val head = futures.head
      TestUtils.waitUntilTrue(() => {
        head.isDone()
      }, "Producing records did not complete as expected.")
      head.get
      fail("Expected an authentication exception")
    } catch {
      case e: ExecutionException =>
        assert(e.getCause.isInstanceOf[AuthenticationFailedException])
      case e: Exception =>
        fail("Unexpected exception was thrown")
    }
  }

  @Test(expected = classOf[AuthenticationFailedException])
  override def testSimpleConsumption() {
    val consumer = this.consumers.head
    consumer.assign(List(tp).asJava)
    consumer.poll(200)
  }

  @Test(expected = classOf[AuthenticationFailedException])
  def testConsumerGroupDescription() {
    val propsFile = TestUtils.tempFile()
    val propsStream = new FileOutputStream(propsFile)
    propsStream.write("security.protocol=SASL_PLAINTEXT\n".getBytes())
    propsStream.write("sasl.mechanism=PLAIN".getBytes())
    propsStream.close()

    val cgcArgs = Array("--bootstrap-server", brokerList,
                        "--describe",
                        "--group", "test.group",
                        "--command-config", propsFile.getAbsolutePath)
    val opts = new ConsumerGroupCommandOptions(cgcArgs)
    new KafkaConsumerGroupService(opts)
  }

  /**
   * Checks that everyone can access ZkUtils.SecureZkRootPaths and ZkUtils.SensitiveZkRootPaths
   * when zookeeper.set.acl=false, even if Zookeeper is SASL-enabled.
   */
  @Test
  def testZkAclsDisabled() {
    TestUtils.verifyUnsecureZkAcls(zkUtils)
  }
}
