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
import kafka.utils.TestUtils
import kafka.utils.{Utils, Logging}
import junit.framework.Assert._
import kafka.api.FetchRequest
import kafka.serializer.Encoder
import kafka.message.{MessageSet, Message}
import kafka.producer.async.MissingConfigException
import org.scalatest.junit.JUnitSuite
import org.junit.{After, Before, Test}

class KafkaLog4jAppenderTest extends JUnitSuite with Logging {

  var logDir: File = null
  //  var topicLogDir: File = null
  var server: KafkaServer = null
  val brokerPort: Int = 9092
  var simpleConsumer: SimpleConsumer = null
  val tLogger = Logger.getLogger(getClass())

  @Before
  def setUp() {
    val config: Properties = createBrokerConfig(1, brokerPort)
    val logDirPath = config.getProperty("log.dir")
    logDir = new File(logDirPath)

    server = TestUtils.createServer(new KafkaConfig(config))
    Thread.sleep(100)
    simpleConsumer = new SimpleConsumer("localhost", brokerPort, 1000000, 64*1024)
  }

  @After
  def tearDown() {
    simpleConsumer.close
    server.shutdown
    Thread.sleep(100)
    Utils.rm(logDir)
  }

  @Test
  def testKafkaLog4jConfigs() {
    var props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.Host", "localhost")
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.appender.KAFKA.encoder", "kafka.log4j.AppenderStringEncoder")
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
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.appender.KAFKA.Encoder", "kafka.log4j.AppenderStringEncoder")
    props.put("log4j.appender.KAFKA.Port", "9092")
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
    props.put("log4j.appender.KAFKA.Host", "localhost")
    props.put("log4j.appender.KAFKA.Port", "9092")
    props.put("log4j.appender.KAFKA.Encoder", "kafka.log4j.AppenderStringEncoder")
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
    props.put("log4j.appender.KAFKA.Host", "localhost")
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.appender.KAFKA.Port", "9092")
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")

    // serializer missing
    try {
      PropertyConfigurator.configure(props)
    }catch {
      case e: MissingConfigException => fail("should default to kafka.producer.DefaultStringEncoder")
    }
  }

  @Test
  def testLog4jAppends() {
    PropertyConfigurator.configure(getLog4jConfig)

    for(i <- 1 to 5)
      info("test")

    Thread.sleep(500)

    var offset = 0L
    val messages = simpleConsumer.fetch(new FetchRequest("test-topic", 0, offset, 1024*1024))

    var count = 0
    for(message <- messages) {
      count = count + 1
      offset += message.offset
    }

    assertEquals(5, count)
  }


  private def getLog4jConfig: Properties = {
    var props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.Port", "9092")
    props.put("log4j.appender.KAFKA.Host", "localhost")
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")
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

