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

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{PropertyConfigurator, Logger}
import java.util.Properties
import java.io.File
import kafka.consumer.SimpleConsumer
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, TestZKUtils,Utils, Logging}
import kafka.zk.EmbeddedZookeeper
import junit.framework.Assert._
import kafka.api.FetchRequest
import kafka.serializer.Encoder
import kafka.message.Message
import kafka.producer.async.MissingConfigException
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}

class KafkaLog4jAppenderTest extends JUnitSuite with Logging {

  var logDirZk: File = null
  var logDirBl: File = null
  //  var topicLogDir: File = null
  var serverBl: KafkaServer = null
  var serverZk: KafkaServer = null

  var simpleConsumerZk: SimpleConsumer = null
  var simpleConsumerBl: SimpleConsumer = null

  val tLogger = Logger.getLogger(getClass())

  private val brokerZk = 0
  private val brokerBl = 1

  private val ports = TestUtils.choosePorts(2)
  private val (portZk, portBl) = (ports(0), ports(1))

  private var zkServer:EmbeddedZookeeper = null

  @Before
  def setUp() {
    zkServer = new EmbeddedZookeeper(TestZKUtils.zookeeperConnect)

    val propsZk = TestUtils.createBrokerConfig(brokerZk, portZk)
    val logDirZkPath = propsZk.getProperty("log.dir")
    logDirZk = new File(logDirZkPath)
    serverZk = TestUtils.createServer(new KafkaConfig(propsZk));

    val propsBl: Properties = createBrokerConfig(brokerBl, portBl)
    val logDirBlPath = propsBl.getProperty("log.dir")
    logDirBl = new File(logDirBlPath)
    serverBl = TestUtils.createServer(new KafkaConfig(propsBl))

    Thread.sleep(100)

    simpleConsumerZk = new SimpleConsumer("localhost", portZk, 1000000, 64*1024)
    simpleConsumerBl = new SimpleConsumer("localhost", portBl, 1000000, 64*1024)
  }

  @After
  def tearDown() {
    simpleConsumerZk.close
    simpleConsumerBl.close

    serverZk.shutdown
    serverBl.shutdown
    Utils.rm(logDirZk)
    Utils.rm(logDirBl)

    Thread.sleep(500)
    zkServer.shutdown
    Thread.sleep(500)
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
    props.put("log4j.appender.KAFKA.BrokerList", "0:localhost:"+portBl.toString)
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
    props.put("log4j.appender.KAFKA.BrokerList", "0:localhost:"+portBl.toString)
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
  def testBrokerListLog4jAppends() {
    PropertyConfigurator.configure(getLog4jConfigWithBrokerList)

    for(i <- 1 to 5)
      info("test")

    Thread.sleep(500)

    var offset = 0L
    val messages = simpleConsumerBl.fetch(new FetchRequest("test-topic", 0, offset, 1024*1024))

    var count = 0
    for(message <- messages) {
      count = count + 1
      offset += message.offset
    }

    assertEquals(5, count)
  }

  @Test
  def testZkConnectLog4jAppends() {
    PropertyConfigurator.configure(getLog4jConfigWithZkConnect)

    for(i <- 1 to 5)
      info("test")

    Thread.sleep(500)

    var offset = 0L
    val messages = simpleConsumerZk.fetch(new FetchRequest("test-topic", 0, offset, 1024*1024))

    var count = 0
    for(message <- messages) {
      count = count + 1
      offset += message.offset
    }

    assertEquals(5, count)
  }

  private def getLog4jConfigWithBrokerList: Properties = {
    var props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout","org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern","%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.BrokerList", "0:localhost:"+portBl.toString)
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.logger.kafka.log4j", "INFO,KAFKA")
    props
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

  private def createBrokerConfig(nodeId: Int, port: Int): Properties = {
    val props = new Properties
    props.put("brokerid", nodeId.toString)
    props.put("port", port.toString)
    props.put("log.dir", getLogDir.getAbsolutePath)
    props.put("log.flush.interval", "1")
    props.put("enable.zookeeper", "false")
    props.put("num.partitions", "1")
    props.put("log.retention.hours", "10")
    props.put("log.cleanup.interval.mins", "5")
    props.put("log.file.size", "1000")
    props
  }

  private def getLogDir(): File = {
    val dir = TestUtils.tempDir()
    dir
  }
}

class AppenderStringEncoder extends Encoder[LoggingEvent] {
  def toMessage(event: LoggingEvent):Message = {
    val logMessage = event.getMessage
    new Message(logMessage.asInstanceOf[String].getBytes)
  }
}

