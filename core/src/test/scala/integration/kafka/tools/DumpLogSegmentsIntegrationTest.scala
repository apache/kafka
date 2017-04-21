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
package integration.kafka.tools

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.Properties

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.tools.DumpLogSegments
import kafka.utils.TestUtils
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.junit.Assert.assertTrue
import org.junit.Test

class DumpLogSegmentsIntegrationTest extends KafkaServerTestHarness {

  override def generateConfigs: Seq[KafkaConfig] = TestUtils.createBrokerConfigs(1, zkConnect)
    .map(KafkaConfig.fromProps(_, new Properties()))

  @Test
  def testSaneOutputForPopulatedLog(): Unit = {
    val topic = "new-topic"
    val msg = "a test message"
    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, TestUtils.getBrokerListStrFromServers(servers))
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)
    producer.send(new ProducerRecord(topic, msg.getBytes()))
    producer.close()
    val log = servers.head.logManager.logsByTopicPartition(new TopicPartition(topic, 0))
    val baos = new ByteArrayOutputStream()
    val out = new PrintStream(baos)
    val old = Console.out
    val err = Console.err
    try {
      old.flush()
      err.flush()
      Console.setOut(out)
      Console.setErr(out)
      DumpLogSegments.main(
        Seq("--files", log.dir.listFiles.filter(_.getName.endsWith(".log")).head.getCanonicalPath).toArray
      )
      out.flush()
    } finally {
      Console.setOut(old)
      Console.setErr(err)
    }
    assertTrue(baos.toString.contains("Starting offset: 0"))
  }

}
