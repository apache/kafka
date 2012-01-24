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

package kafka.javaapi.producer

import junit.framework.Assert
import kafka.server.KafkaConfig
import org.apache.log4j.Logger
import java.util.Properties
import kafka.producer.SyncProducerConfig
import kafka.javaapi.message.ByteBufferMessageSet
import kafka.javaapi.ProducerRequest
import kafka.message.{NoCompressionCodec, Message}
import kafka.integration.KafkaServerTestHarness
import org.scalatest.junit.JUnit3Suite
import kafka.utils.{SystemTime, TestUtils}

class SyncProducerTest extends JUnit3Suite with KafkaServerTestHarness {
  private var messageBytes =  new Array[Byte](2);
  val simpleProducerLogger = Logger.getLogger(classOf[kafka.producer.SyncProducer])
  val configs = List(new KafkaConfig(TestUtils.createBrokerConfigs(1).head))
  val zookeeperConnect = zkConnect

  def testReachableServer() {
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", servers.head.socketServer.port.toString)
    props.put("buffer.size", "102400")
    props.put("connect.timeout.ms", "500")
    props.put("reconnect.interval", "1000")
    val producer = new SyncProducer(new SyncProducerConfig(props))
    var failed = false
    val firstStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                        messages = getMessageList(new Message(messageBytes))))
    }catch {
      case e: Exception => failed=true
    }
    Assert.assertFalse(failed)
    failed = false
    val firstEnd = SystemTime.milliseconds
    Assert.assertTrue((firstEnd-firstStart) < 500)
    val secondStart = SystemTime.milliseconds
    try {
      producer.send("test", 0, new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                        messages = getMessageList(new Message(messageBytes))))
    }catch {
      case e: Exception => failed = true
    }
    Assert.assertFalse(failed)
    val secondEnd = SystemTime.milliseconds
    Assert.assertTrue((secondEnd-secondEnd) < 500)

    try {
      producer.multiSend(Array(new ProducerRequest("test", 0,
        new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                 messages = getMessageList(new Message(messageBytes))))))
    }catch {
      case e: Exception => failed=true
    }
    Assert.assertFalse(failed)
  }

  private def getMessageList(message: Message): java.util.List[Message] = {
    val messageList = new java.util.ArrayList[Message]()
    messageList.add(message)
    messageList
  }
}
