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

package integration.kafka.api

import org.junit.runner.RunWith
import org.junit.runners.Parameterized
import org.junit.runners.Parameterized.Parameters
import java.lang.{Boolean => JBoolean}
import java.time.Duration
import java.util

import scala.collection.JavaConverters._
import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.{After, Test}

/**
 * Tests behavior of specifying auto topic creation configuration for the consumer and broker
 */
@RunWith(value = classOf[Parameterized])
class ConsumerTopicCreationTest(brokerAutoTopicCreationEnable: JBoolean, consumerAllowAutoCreateTopics: JBoolean) extends IntegrationTestHarness {
  override protected def brokerCount: Int = 1

  val topic = "topic"
  val part = 0
  val tp = new TopicPartition(topic, part)
  val producerClientId = "ConsumerTestProducer"
  val consumerClientId = "ConsumerTestConsumer"
  var adminClient: AdminClient = null

  // configure server properties
  this.serverConfig.setProperty(KafkaConfig.ControlledShutdownEnableProp, "false") // speed up shutdown
  this.serverConfig.setProperty(KafkaConfig.AutoCreateTopicsEnableProp, brokerAutoTopicCreationEnable.toString)

  // configure client properties
  this.producerConfig.setProperty(ProducerConfig.CLIENT_ID_CONFIG, producerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, consumerClientId)
  this.consumerConfig.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "my-test")
  this.consumerConfig.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
  this.consumerConfig.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
  this.consumerConfig.setProperty(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "100")
  this.consumerConfig.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, consumerAllowAutoCreateTopics.toString)

  @After
  override def tearDown(): Unit = {
    if (adminClient != null)
      Utils.closeQuietly(adminClient, "AdminClient")
    super.tearDown()
  }

  @Test
  def testAutoTopicCreation(): Unit = {
    val consumer = createConsumer()
    adminClient = AdminClient.create(createConfig())

    consumer.subscribe(util.Arrays.asList(topic))
    consumer.poll(Duration.ofMillis(100))

    val topicCreated = adminClient.listTopics.names.get.contains(topic)
    if (brokerAutoTopicCreationEnable && consumerAllowAutoCreateTopics)
      assert(topicCreated == true)
    else
      assert(topicCreated == false)
  }

  def createConfig(): util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    config.put(AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG, "20000")
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    config
  }
}

object ConsumerTopicCreationTest {
  @Parameters(name = "brokerTopicCreation={0}, consumerTopicCreation={1}")
  def parameters: java.util.Collection[Array[Object]] = {
    val data = new java.util.ArrayList[Array[Object]]()
    for (brokerAutoTopicCreationEnable <- Array(JBoolean.TRUE, JBoolean.FALSE))
      for (consumerAutoCreateTopicsPolicy <- Array(JBoolean.TRUE, JBoolean.FALSE))
        data.add(Array(brokerAutoTopicCreationEnable, consumerAutoCreateTopicsPolicy))
    data
  }
}
