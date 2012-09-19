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

import kafka.server.{KafkaServer, KafkaConfig}
import org.scalatest.junit.JUnit3Suite
import org.apache.log4j.Logger
import java.util.Properties
import kafka.consumer.SimpleConsumer
import kafka.utils.TestUtils
import kafka.api.{OffsetRequest, FetchRequest}
import junit.framework.Assert._

class BackwardsCompatibilityTest extends JUnit3Suite {

  val topic = "MagicByte0"
  val group = "default_group"
  val testConsumer = "consumer"
  val kafkaProps = new Properties
  val host = "localhost"
  val port = TestUtils.choosePort
  val loader = getClass.getClassLoader
  val kafkaLogDir = loader.getResource("test-kafka-logs")
  kafkaProps.put("brokerid", "12")
  kafkaProps.put("port", port.toString)
  kafkaProps.put("log.dir", kafkaLogDir.getPath)
  val kafkaConfig =
    new KafkaConfig(kafkaProps) {
      override val enableZookeeper = false
    }
  var kafkaServer : KafkaServer = null
  var simpleConsumer: SimpleConsumer = null

  private val logger = Logger.getLogger(getClass())

  override def setUp() {
    super.setUp()
    kafkaServer = TestUtils.createServer(kafkaConfig)
    simpleConsumer = new SimpleConsumer(host, port, 1000000, 64*1024)
  }

  override def tearDown() {
    simpleConsumer.close
    kafkaServer.shutdown
    super.tearDown
  }

  // test for reading data with magic byte 0
  def testProtocolVersion0() {
    val lastOffset = simpleConsumer.getOffsetsBefore(topic, 0, OffsetRequest.LatestTime, 1)
    var fetchOffset: Long = 0L
    var messageCount: Int = 0

    while(fetchOffset < lastOffset(0)) {
      val fetched = simpleConsumer.fetch(new FetchRequest(topic, 0, fetchOffset, 10000))
      fetched.foreach(m => fetchOffset = m.offset)
      messageCount += fetched.size
    }
    assertEquals(100, messageCount)
  }
}
