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

import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import org.junit._
import org.junit.rules.TemporaryFolder

import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams._
import org.apache.kafka.streams.scala.kstream._

import org.apache.kafka.streams.integration.utils.{
  EmbeddedKafkaCluster,
  IntegrationTestUtils
}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils

import ImplicitConversions._

/**
  * Test suite that does not use the default serializer for an table aggregation, but implicitly provided ones.
  */
class AggregateImplicitSerializerTest extends JUnitSuite with AggregateTestData {

  private val privateCluster: EmbeddedKafkaCluster = new EmbeddedKafkaCluster(1)

  @Rule def cluster: EmbeddedKafkaCluster = privateCluster

  final val alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000
  val mockTime: MockTime = cluster.time
  mockTime.setCurrentTimeMs(alignedTime)

  val tFolder: TemporaryFolder = new TemporaryFolder(TestUtils.tempDirectory())

  @Rule def testFolder: TemporaryFolder = tFolder

  @Before
  def startKafkaCluster(): Unit = {
    cluster.createTopic(inputTopic)
  }

  @Test
  def shouldUseCorrectSerializer(): Unit = {
    import Serdes._

    val streamsConfiguration = getStreamsConfiguration()

    val streamBuilder = new StreamsBuilder
    val textLines = streamBuilder.stream[String, String](inputTopic)

    val pattern = Pattern.compile("\\W+", Pattern.UNICODE_CHARACTER_CLASS)

    // the default Serializer is Long. Sso we use Double to make sure,
    // that if the default one is picked this test will fail.
    val aggregateByFirstLetter: KTable[String, Double] =
    textLines
      .flatMapValues(_.split("\\s+"))
      .map((_, l) => (l.substring(0, 1).toLowerCase, l))
      .groupByKey
      .aggregate(0.0, (_, _, agg) => agg + 1.0)

    // write to output topic
    aggregateByFirstLetter.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(streamBuilder.build(), streamsConfiguration)
    streams.start()

    // produce and consume synchronously
    val actualWordCounts: java.util.List[KeyValue[String, Double]] = produceNConsume(inputTopic, outputTopic)

    streams.close()

    import collection.JavaConverters._
    assertEquals(actualWordCounts.asScala.take(expectedCounts.size).sortBy(_.key), expectedCounts.sortBy(_.key))
  }

  private def getStreamsConfiguration(): Properties = {
    val streamsConfiguration: Properties = new Properties()

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-test")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot.getPath)
    streamsConfiguration
  }

  private def getProducerConfig(): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.RETRIES_CONFIG, "0")
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p
  }

  private def getConsumerConfig(): Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "wordcount-scala-integration-test-standard-consumer")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[DoubleDeserializer])
    p
  }

  private def produceNConsume(inputTopic: String, outputTopic: String): java.util.List[KeyValue[String, Double]] = {
    val linesProducerConfig: Properties = getProducerConfig()

    import collection.JavaConverters._
    IntegrationTestUtils.produceValuesSynchronously(inputTopic, inputValues.asJava, linesProducerConfig, mockTime)

    val consumerConfig = getConsumerConfig()

    IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, expectedCounts.size)
  }
}

trait AggregateTestData {
  val inputTopic = "inputTopic"
  val outputTopic = "outputTopic"

  val inputValues = List(
    "Hello Kafka Streams",
    "All streams lead to Kafka",
    "Join Kafka Summit"
  )

  val expectedCounts: List[KeyValue[String, Double]] = List(
    new KeyValue("h", 1.0),
    new KeyValue("k", 3.0),
    new KeyValue("s", 3.0),
    new KeyValue("a", 1.0),
    new KeyValue("l", 1.0),
    new KeyValue("t", 1.0),
    new KeyValue("j", 1.0)
  )
}
