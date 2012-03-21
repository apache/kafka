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

package kafka.log4j

import java.util.Properties
import java.io.File
import junit.framework.Assert._
import kafka.api.FetchRequestBuilder
import kafka.consumer.SimpleConsumer
import kafka.message.Message
import kafka.producer.async.MissingConfigException
import kafka.serializer.Encoder
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.zk.ZooKeeperTestHarness
import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{PropertyConfigurator, Logger}
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnit3Suite
import kafka.utils._

class KafkaLog4jAppenderTest extends JUnit3Suite with ZooKeeperTestHarness with Logging {

  var logDirZk: File = null
  var serverZk: KafkaServer = null

  var simpleConsumerZk: SimpleConsumer = null

  val tLogger = Logger.getLogger(getClass())

  private val brokerZk = 0

  private val ports = TestUtils.choosePorts(2)
  private val portZk = ports(0)

  @Before
  override def setUp() {
    super.setUp()

    val propsZk = TestUtils.createBrokerConfig(brokerZk, portZk)
    val logDirZkPath = propsZk.getProperty("log.dir")
    logDirZk = new File(logDirZkPath)
    serverZk = TestUtils.createServer(new KafkaConfig(propsZk));

    Thread.sleep(100)

    simpleConsumerZk = new SimpleConsumer("localhost", portZk, 1000000, 64*1024)
  }

  @After
  override def tearDown() {
    simpleConsumerZk.close

    serverZk.shutdown
    Utils.rm(logDirZk)

    Thread.sleep(500)
    super.tearDown()
  }

  @Test
  def testKafkaLog4jConfigs() {
    var props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout","org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern","%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.appender.KAFKA.SerializerClass", "kafka.log4j.AppenderStringEncoder")
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")

    // port missing
    try {
      PropertyConfigurator.configure(props)
      fail("Missing properties exception was expected !")
    }catch {
      case e: MissingConfigException =>
    }

    props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout","org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern","%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.appender.KAFKA.SerializerClass", "kafka.log4j.AppenderStringEncoder")
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")

    // host missing
    try {
      PropertyConfigurator.configure(props)
      fail("Missing properties exception was expected !")
    }catch {
      case e: MissingConfigException =>
    }

    props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout","org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern","%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.SerializerClass", "kafka.log4j.AppenderStringEncoder")
    props.put("log4j.appender.KAFKA.ZkConnect", TestZKUtils.zookeeperConnect)
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")

    // topic missing
    try {
      PropertyConfigurator.configure(props)
      fail("Missing properties exception was expected !")
    }catch {
      case e: MissingConfigException =>
    }

    props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout","org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern","%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.ZkConnect", TestZKUtils.zookeeperConnect)
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")

    // serializer missing
    try {
      PropertyConfigurator.configure(props)
    }catch {
      case e: MissingConfigException => fail("should default to kafka.serializer.StringEncoder")
    }
  }

  @Test
  def testZkConnectLog4jAppends() {
    PropertyConfigurator.configure(getLog4jConfigWithZkConnect)

    for(i <- 1 to 5)
      info("test")

    Thread.sleep(500)

    val response = simpleConsumerZk.fetch(new FetchRequestBuilder().addFetch("test-topic", 0, 0L, 1024*1024).build())
    val fetchMessage = response.messageSet("test-topic", 0)

    var count = 0
    for(message <- fetchMessage) {
      count = count + 1
    }

    assertEquals(5, count)
  }

  private def getLog4jConfigWithZkConnect: Properties = {
    var props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout","org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern","%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.ZkConnect", TestZKUtils.zookeeperConnect)
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.logger.kafka.log4j", "INFO,KAFKA")
    props
  }
}

class AppenderStringEncoder extends Encoder[LoggingEvent] {
  def toMessage(event: LoggingEvent):Message = {
    val logMessage = event.getMessage
    new Message(logMessage.asInstanceOf[String].getBytes)
  }
}

