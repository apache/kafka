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
package kafka.server

import kafka.utils.TestUtils
import java.io.File
import kafka.utils.Utils
import kafka.api.FetchRequest
import kafka.producer.{SyncProducer, SyncProducerConfig}
import kafka.consumer.SimpleConsumer
import java.util.Properties
import org.scalatest.junit.JUnitSuite
import org.junit.Test
import junit.framework.Assert._
import kafka.message.{NoCompressionCodec, Message, ByteBufferMessageSet}

class ServerShutdownTest extends JUnitSuite {
  val port = TestUtils.choosePort

  @Test
  def testCleanShutdown() {
    val props = TestUtils.createBrokerConfig(0, port)
    val config = new KafkaConfig(props) {
      override val enableZookeeper = false
    }

    val host = "localhost"
    val topic = "test"
    val sent1 = new ByteBufferMessageSet(NoCompressionCodec, new Message("hello".getBytes()), new Message("there".getBytes()))
    val sent2 = new ByteBufferMessageSet(NoCompressionCodec, new Message("more".getBytes()), new Message("messages".getBytes()))

    {
      val producer = new SyncProducer(getProducerConfig(host,
                                                        port,
                                                        64*1024,
                                                        100000,
                                                        10000))
      val consumer = new SimpleConsumer(host,
                                        port,
                                        1000000,
                                        64*1024)

      val server = new KafkaServer(config)
      server.startup()

      // send some messages
      producer.send(topic, sent1)
      sent1.getBuffer.rewind

      Thread.sleep(200)
      // do a clean shutdown
      server.shutdown()
      val cleanShutDownFile = new File(new File(config.logDir), server.CLEAN_SHUTDOWN_FILE)
      assertTrue(cleanShutDownFile.exists)
      producer.close()
    }


    {
      val producer = new SyncProducer(getProducerConfig(host,
                                                        port,
                                                        64*1024,
                                                        100000,
                                                        10000))
      val consumer = new SimpleConsumer(host,
                                        port,
                                        1000000,
                                        64*1024)

      val server = new KafkaServer(config)
      server.startup()

      // bring the server back again and read the messages
      var fetched: ByteBufferMessageSet = null
      while(fetched == null || fetched.validBytes == 0)
        fetched = consumer.fetch(new FetchRequest(topic, 0, 0, 10000))
      TestUtils.checkEquals(sent1.iterator, fetched.iterator)
      val newOffset = fetched.validBytes

      // send some more messages
      producer.send(topic, sent2)
      sent2.getBuffer.rewind

      Thread.sleep(200)

      fetched = null
      while(fetched == null || fetched.validBytes == 0)
        fetched = consumer.fetch(new FetchRequest(topic, 0, newOffset, 10000))
      TestUtils.checkEquals(sent2.map(m => m.message).iterator, fetched.map(m => m.message).iterator)

      server.shutdown()
      Utils.rm(server.config.logDir)
      producer.close()
    }

  }

  private def getProducerConfig(host: String, port: Int, bufferSize: Int, connectTimeout: Int,
                                reconnectInterval: Int): SyncProducerConfig = {
    val props = new Properties()
    props.put("host", host)
    props.put("port", port.toString)
    props.put("buffer.size", bufferSize.toString)
    props.put("connect.timeout.ms", connectTimeout.toString)
    props.put("reconnect.interval", reconnectInterval.toString)
    new SyncProducerConfig(props)
  }
}
