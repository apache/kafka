/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.streams.scala

import java.util.Properties
import java.util.regex.Pattern

import org.junit.Assert._
import org.scalatest.junit.JUnitSuite
import org.junit._
import org.junit.rules.TemporaryFolder

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams._
import org.apache.kafka.streams.scala.kstream._

import org.apache.kafka.streams.integration.utils.{EmbeddedKafkaCluster, IntegrationTestUtils}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils

import ImplicitConversions._
import com.typesafe.scalalogging.LazyLogging

class WordCountTest extends JUnitSuite with WordCountTestData with LazyLogging {

  private val privateCluster: EmbeddedKafkaCluster = new EmbeddedKafkaCluster(1)

  @Rule def cluster: EmbeddedKafkaCluster = privateCluster

  final val alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000
  val mockTime: MockTime = cluster.time
  mockTime.setCurrentTimeMs(alignedTime)

  val streamsConfiguration: Properties = new Properties()

  val tFolder: TemporaryFolder = new TemporaryFolder(TestUtils.tempDirectory())
  @Rule def testFolder: TemporaryFolder = tFolder
    

  @Before
  def startKafkaCluster(): Unit = {
    cluster.createTopic(inputTopic)
    cluster.createTopic(outputTopic)
  }

  @Test def testShouldCountWords(): Unit = {

    import DefaultSerdes._

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-test")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath())

    val streamBuilder = new StreamsBuilder
    val textLines = streamBuilder.stream[String, String](inputTopic)

    val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

    val wordCounts: KTable[String, Long] =
      textLines.flatMapValues(v => pattern.split(v.toLowerCase))
        .groupBy((k, v) => v)
        .count()

    wordCounts.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(streamBuilder.build(), streamsConfiguration)
    streams.start()

    // produce stream of lines
    val linesProducerConfig: Properties = {
      val p = new Properties()
      p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ProducerConfig.ACKS_CONFIG, "all")
      p.put(ProducerConfig.RETRIES_CONFIG, "0")
      p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
      p
    }

    import collection.JavaConverters._
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues.asJava, linesProducerConfig, mockTime)

    // consume and verify
    val consumerConfig = {
      val p = new Properties()
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-scala-integration-test-standard-consumer")
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
      p
    }

    val actualWordCounts: java.util.List[KeyValue[String, Long]] =
      IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, expectedWordCounts.size)

    streams.close()

    assertEquals(actualWordCounts.asScala.take(expectedWordCounts.size).sortBy(_.key), expectedWordCounts.sortBy(_.key))
  }
}

trait WordCountTestData {
  val inputTopic = s"inputTopic"
  val outputTopic = s"outputTopic"
  val localStateDir = "local_state_data_wc"

  val inputValues = List(
    "Hello Kafka Streams",
    "All streams lead to Kafka",
    "Join Kafka Summit",
    "И теперь пошли русские слова"
  )

  val expectedWordCounts: List[KeyValue[String, Long]] = List(
    new KeyValue("hello", 1L),
    new KeyValue("all", 1L),
    new KeyValue("streams", 2L),
    new KeyValue("lead", 1L),
    new KeyValue("to", 1L),
    new KeyValue("join", 1L),
    new KeyValue("kafka", 3L),
    new KeyValue("summit", 1L),
    new KeyValue("и", 1L),
    new KeyValue("теперь", 1L),
    new KeyValue("пошли", 1L),
    new KeyValue("русские", 1L),
    new KeyValue("слова", 1L)
  )
}

