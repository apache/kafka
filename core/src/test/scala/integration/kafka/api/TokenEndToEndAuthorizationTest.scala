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
package kafka.api

import java.util
import java.util.Properties

import kafka.admin.AdminClient
import kafka.server.KafkaConfig
import kafka.utils.Implicits._
import kafka.utils.{JaasTestUtils, TestUtils}
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.config.SaslConfigs
import org.apache.kafka.common.security.token.DelegationToken
import org.junit.Before

import scala.collection.JavaConverters._

class TokenEndToEndAuthorizationTest extends SaslScramSslEndToEndAuthorizationTest {

  this.serverConfig.setProperty(KafkaConfig.DelegationTokenMasterKeyProp, "testKey")
  var adminClient: AdminClient = null

  @Before
  override def setUp() {
    super.setUp()

    //create a token with client1 scram credentials
    adminClient = AdminClient.create(createAdminConfig.asScala.toMap)
    val tokenResult = adminClient.createToken(List())
    //wait for token to reach all the brokers
     Thread.sleep(100)
    // init clients with token auth
    initTokenClients(tokenResult._2)
  }

  //override default initialization clients
  override def initClients(producerSecurityProps: Properties, consumerSecurityProps: Properties) = {
  }

  def initTokenClients(token: DelegationToken) = {
    val clientLoginContext = JaasTestUtils.tokenClientLoginModule(token.tokenInfo().tokenId(), token.hmacAsBase64String())
    producerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    consumerConfig.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)

    val producerSecurityProps = TestUtils.producerSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    val consumerSecurityProps = TestUtils.consumerSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)

    producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArraySerializer])
    producerConfig ++= producerSecurityProps
    consumerConfig.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[org.apache.kafka.common.serialization.ByteArrayDeserializer])
    consumerConfig ++= consumerSecurityProps
    for (_ <- 0 until producerCount)
      producers += createNewProducer
    for (_ <- 0 until consumerCount) {
      consumers += createNewConsumer
    }
  }

  def createAdminConfig(): util.Map[String, Object] = {
    val config = new util.HashMap[String, Object]
    config.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    val securityProps: util.Map[Object, Object] =
      TestUtils.adminClientSecurityConfigs(securityProtocol, trustStoreFile, clientSaslProperties)
    securityProps.asScala.foreach { case (key, value) => config.put(key.asInstanceOf[String], value) }
    val clientLoginContext = jaasClientLoginModule(kafkaClientSaslMechanism)
    config.put(SaslConfigs.SASL_JAAS_CONFIG, clientLoginContext)
    config
  }
}
