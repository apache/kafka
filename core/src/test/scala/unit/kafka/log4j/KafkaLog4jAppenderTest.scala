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

package kafka.log4j

import kafka.consumer.SimpleConsumer
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.utils.{TestUtils, Utils, Logging}
import kafka.api.FetchRequestBuilder
import kafka.producer.async.MissingConfigException
import kafka.serializer.Encoder
import kafka.zk.ZooKeeperTestHarness

import java.util.Properties
import java.io.File

import org.apache.log4j.spi.LoggingEvent
import org.apache.log4j.{PropertyConfigurator, Logger}
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnit3Suite

import junit.framework.Assert._

class KafkaLog4jAppenderTest extends JUnit3Suite with ZooKeeperTestHarness with Logging {

  var logDirZk: File = null
  var config: KafkaConfig = null
  var server: KafkaServer = null

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
    config = new KafkaConfig(propsZk)
    server = TestUtils.createServer(config)
    simpleConsumerZk = new SimpleConsumer("localhost", portZk, 1000000, 64 * 1024, "")
  }

  @After
  override def tearDown() {
    simpleConsumerZk.close
    server.shutdown
    Utils.rm(logDirZk)
    super.tearDown()
  }

  @Test
  def testKafkaLog4jConfigs() {
    // host missing
    var props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")

    try {
      PropertyConfigurator.configure(props)
      fail("Missing properties exception was expected !")
    } catch {
      case e: MissingConfigException =>
    }

    // topic missing
    props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.brokerList", TestUtils.getBrokerListStrFromConfigs(Seq(config)))
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")

    try {
      PropertyConfigurator.configure(props)
      fail("Missing properties exception was expected !")
    } catch {
      case e: MissingConfigException =>
    }
  }

  @Test
  def testLog4jAppends() {
    PropertyConfigurator.configure(getLog4jConfig)

    for(i <- 1 to 5)
      info("test")

    val response = simpleConsumerZk.fetch(new FetchRequestBuilder().addFetch("test-topic", 0, 0L, 1024*1024).build())
    val fetchMessage = response.messageSet("test-topic", 0)

    var count = 0
    for(message <- fetchMessage) {
      count = count + 1
    }

    assertEquals(5, count)
  }

  private def getLog4jConfig: Properties = {
    val props = new Properties()
    props.put("log4j.rootLogger", "INFO")
    props.put("log4j.appender.KAFKA", "kafka.producer.KafkaLog4jAppender")
    props.put("log4j.appender.KAFKA.layout", "org.apache.log4j.PatternLayout")
    props.put("log4j.appender.KAFKA.layout.ConversionPattern", "%-5p: %c - %m%n")
    props.put("log4j.appender.KAFKA.BrokerList", TestUtils.getBrokerListStrFromConfigs(Seq(config)))
    props.put("log4j.appender.KAFKA.Topic", "test-topic")
    props.put("log4j.appender.KAFKA.RequiredNumAcks", "1")
    props.put("log4j.appender.KAFKA.SyncSend", "true")
    props.put("log4j.logger.kafka.log4j", "INFO, KAFKA")
    props
  }
}

class AppenderStringEncoder(encoding: String = "UTF-8") extends Encoder[LoggingEvent] {
  def toBytes(event: LoggingEvent): Array[Byte] = {
    event.getMessage.toString.getBytes(encoding)
  }
}

