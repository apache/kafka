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

package kafka.tools

import java.nio.file.Files
import java.io.File

import kafka.admin.LeaderElectionCommandTest
import kafka.common.MessageReader
import kafka.integration.KafkaServerTestHarness
import kafka.server.{KafkaConfig, KafkaServer}
import kafka.tools.ConsoleProducer.{getReaderProps, producerProps}
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.{Admin, AdminClientConfig, OffsetSpec}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.network.ListenerName
import org.junit.jupiter.api.Assertions.{assertEquals, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.Test

import scala.collection.Seq
import scala.jdk.CollectionConverters._

class ConsoleProducerIntegrationTest extends KafkaServerTestHarness {

  import LeaderElectionCommandTest._

  def generateConfigs: Seq[KafkaConfig] = {
    TestUtils.createBrokerConfigs(1, zkConnect)
      .map(KafkaConfig.fromProps)
  }

  def send(producer: KafkaProducer[Array[Byte], Array[Byte]],
           record: ProducerRecord[Array[Byte], Array[Byte]], sync: Boolean): Unit = {
    if (sync)
      producer.send(record).get()
    else
      producer.send(record, new ErrorLoggingCallback(record.topic, record.key, record.value, false))
  }

  @Test
  def checkFilesPathExistentOrNonExistent(): Unit = {
    val testTopic = "test-01"

    val existentFilePath = TestUtils.tempFile().getAbsolutePath
    val nonExistentFilePath1 = new File("nonExistentFilePath1").getAbsolutePath
    val nonExistentFilePath2 = new File("nonExistentFilePath2").getAbsolutePath

    var producerConfig: ConsoleProducer.ProducerConfig = null
    var reader: MessageReader = null
    var e: IllegalArgumentException = null

    //check only one existent file path and only one nonExistent file path
    producerConfig = new ConsoleProducer.ProducerConfig(
      Array("--bootstrap-server", brokerList,
        "--topic", testTopic, "--files",
        Array(existentFilePath, nonExistentFilePath1).mkString(",")
      ))
    reader = Class.forName(producerConfig.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[MessageReader]

    e = assertThrows(classOf[IllegalArgumentException], () => reader.init(System.in, getReaderProps(producerConfig)))
    assertNotNull(e)
    assertEquals(s"NonExistent files ($nonExistentFilePath1) Duplicate files (). Please check the file path!", e.getMessage)
    reader.close()

    //check two existent files path but they are the same file path
    producerConfig = new ConsoleProducer.ProducerConfig(
      Array("--bootstrap-server", brokerList,
        "--topic", testTopic, "--files",
        Array(existentFilePath, existentFilePath).mkString(",")
      ))
    reader = Class.forName(producerConfig.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[MessageReader]

    e = assertThrows(classOf[IllegalArgumentException], () => reader.init(System.in, getReaderProps(producerConfig)))
    assertNotNull(e)
    assertEquals(s"NonExistent files () Duplicate files ($existentFilePath). Please check the file path!", e.getMessage)
    reader.close()

    //check two nonExistent files path
    producerConfig = new ConsoleProducer.ProducerConfig(
      Array("--bootstrap-server", brokerList,
        "--topic", testTopic, "--files",
        Array(nonExistentFilePath1, nonExistentFilePath2).mkString(",")
      ))
    reader = Class.forName(producerConfig.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[MessageReader]

    e = assertThrows(classOf[IllegalArgumentException], () => reader.init(System.in, getReaderProps(producerConfig)))
    assertNotNull(e)
    assertEquals(s"NonExistent files ($nonExistentFilePath1,$nonExistentFilePath2) Duplicate files (). Please check the file path!", e.getMessage)
    reader.close()

    //check two nonExistent files path but they are the same file path, we only expect it to appear in the list of nonExistent files
    producerConfig = new ConsoleProducer.ProducerConfig(
      Array("--bootstrap-server", brokerList,
        "--topic", testTopic, "--files",
        Array(nonExistentFilePath1, nonExistentFilePath1).mkString(",")
      ))
    reader = Class.forName(producerConfig.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[MessageReader]

    e = assertThrows(classOf[IllegalArgumentException], () => reader.init(System.in, getReaderProps(producerConfig)))
    assertNotNull(e)
    assertEquals(s"NonExistent files ($nonExistentFilePath1) Duplicate files (). Please check the file path!", e.getMessage)
    reader.close()

  }

  @Test
  def checkInputOneFilePath(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val testTopic = "test-01"
      TestUtils.createTopic(zkClient, testTopic, 1, 1, servers)
      val topicPartition = new TopicPartition(testTopic, 0)
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(0))

      val propsFile = TestUtils.tempFile()
      val propsStream = Files.newOutputStream(propsFile.toPath)
      propsStream.write("aa\r\nbb\r\ncc".getBytes())
      propsStream.close()

      val producerConfig = new ConsoleProducer.ProducerConfig(
        Array("--bootstrap-server", brokerList, "--topic", testTopic, "--files", propsFile.getAbsolutePath))
      val reader = Class.forName(producerConfig.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[MessageReader]
      reader.init(System.in, getReaderProps(producerConfig))

      val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps(producerConfig))
      var sendMessagesNum = 0
      var record: ProducerRecord[Array[Byte], Array[Byte]] = null
      do {
        record = reader.readMessage()
        if (record != null) {
          sendMessagesNum += 1
          send(producer, record, producerConfig.sync)
        }
      } while (record != null)

      producer.flush()

      val partitionLatestOffset = client.listOffsets(Map(topicPartition -> OffsetSpec.latest()).asJava)
        .all().get().get(topicPartition).offset()

      assertTrue(partitionLatestOffset == sendMessagesNum)
      reader.close()
      producer.close()
    }
  }

  @Test
  def checkInputMultifilePaths(): Unit = {
    TestUtils.resource(Admin.create(createConfig(servers).asJava)) { client =>
      val testTopic = "test-01"
      TestUtils.createTopic(zkClient, testTopic, 1, 1, servers)
      val topicPartition = new TopicPartition(testTopic, 0)
      TestUtils.waitForBrokersInIsr(client, topicPartition, Set(0))

      val propsFile1 = TestUtils.tempFile()
      val propsStream1 = Files.newOutputStream(propsFile1.toPath)
      propsStream1.write("aa\r\nbb\r\ncc".getBytes())
      propsStream1.close()

      val propsFile2 = TestUtils.tempFile()
      val propsStream2 = Files.newOutputStream(propsFile2.toPath)
      propsStream2.write("aa\r\nbb\r\ncc".getBytes())
      propsStream2.close()

      val propsFile3 = TestUtils.tempFile()
      val propsStream3 = Files.newOutputStream(propsFile3.toPath)
      propsStream3.write("aa\r\nbb\r\ncc".getBytes())
      propsStream3.close()

      val producerConfig = new ConsoleProducer.ProducerConfig(
        Array("--bootstrap-server", brokerList,
          "--topic", testTopic, "--files",
          Array(propsFile1.getAbsolutePath, propsFile2.getAbsolutePath, propsFile3.getAbsolutePath).mkString(",")
        ))
      val reader = Class.forName(producerConfig.readerClass).getDeclaredConstructor().newInstance().asInstanceOf[MessageReader]
      reader.init(System.in, getReaderProps(producerConfig))

      val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps(producerConfig))
      var sendMessagesNum = 0
      var record: ProducerRecord[Array[Byte], Array[Byte]] = null
      do {
        record = reader.readMessage()
        if (record != null) {
          sendMessagesNum += 1
          send(producer, record, producerConfig.sync)
        }
      } while (record != null)

      producer.flush()

      val partitionLatestOffset = client.listOffsets(Map(topicPartition -> OffsetSpec.latest()).asJava)
        .all().get().get(topicPartition).offset()

      assertTrue(partitionLatestOffset == sendMessagesNum)
      reader.close()
      producer.close()
    }
  }

}

object ConsoleProducerIntegrationTest {
  def createConfig(servers: Seq[KafkaServer]): Map[String, Object] = {
    Map(
      AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers(servers),
      AdminClientConfig.DEFAULT_API_TIMEOUT_MS_CONFIG -> "20000",
      AdminClientConfig.REQUEST_TIMEOUT_MS_CONFIG -> "10000"
    )
  }

  def bootstrapServers(servers: Seq[KafkaServer]): String = {
    TestUtils.bootstrapServers(servers, new ListenerName("PLAINTEXT"))
  }
}
