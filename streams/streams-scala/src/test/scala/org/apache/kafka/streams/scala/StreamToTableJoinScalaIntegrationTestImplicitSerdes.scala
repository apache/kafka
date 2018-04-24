/*
 * Copyright (C) 2018 Lightbend Inc. <https://www.lightbend.com>
 * Copyright (C) 2017-2018 Alexis Seigneurin.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import org.scalatest.junit.JUnitSuite
import org.junit.Assert._
import org.junit.rules.TemporaryFolder
import org.junit._

import org.apache.kafka.streams.integration.utils.{EmbeddedKafkaCluster, IntegrationTestUtils}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.producer.ProducerConfig

import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.test.TestUtils
import org.apache.kafka.streams._
import org.apache.kafka.streams.scala.kstream._

import ImplicitConversions._
import com.typesafe.scalalogging.LazyLogging

/**
 * Test suite that does an example to demonstrate stream-table joins in Kafka Streams
 * <p>
 * The suite contains the test case using Scala APIs `testShouldCountClicksPerRegion` and the same test case using the
 * Java APIs `testShouldCountClicksPerRegionJava`. The idea is to demonstrate that both generate the same result.
 * <p>
 * Note: In the current project settings SAM type conversion is turned off as it's experimental in Scala 2.11.
 * Hence the native Java API based version is more verbose.
 */ 
class StreamToTableJoinScalaIntegrationTestImplicitSerdes extends JUnitSuite
  with StreamToTableJoinTestData with LazyLogging {

  private val privateCluster: EmbeddedKafkaCluster = new EmbeddedKafkaCluster(1)

  @Rule def cluster: EmbeddedKafkaCluster = privateCluster

  final val alignedTime = (System.currentTimeMillis() / 1000 + 1) * 1000
  val mockTime: MockTime = cluster.time
  mockTime.setCurrentTimeMs(alignedTime)

  val tFolder: TemporaryFolder = new TemporaryFolder(TestUtils.tempDirectory())
  @Rule def testFolder: TemporaryFolder = tFolder

  @Before
  def startKafkaCluster(): Unit = {
    cluster.createTopic(userClicksTopic)
    cluster.createTopic(userRegionsTopic)
    cluster.createTopic(outputTopic)
    cluster.createTopic(userClicksTopicJ)
    cluster.createTopic(userRegionsTopicJ)
    cluster.createTopic(outputTopicJ)
  }

  @Test def testShouldCountClicksPerRegion(): Unit = {

    // DefaultSerdes brings into scope implicit serdes (mostly for primitives) that will set up all Serialized, Produced, 
    // Consumed and Joined instances. So all APIs below that accept Serialized, Produced, Consumed or Joined will
    // get these instances automatically
    import DefaultSerdes._

    val streamsConfiguration: Properties = getStreamsConfiguration()

    val builder = new StreamsBuilder()

    val userClicksStream: KStream[String, Long] = builder.stream(userClicksTopic)

    val userRegionsTable: KTable[String, String] = builder.table(userRegionsTopic)

    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTable[String, Long] =
      userClicksStream

        // Join the stream against the table.
        .leftJoin(userRegionsTable, (clicks: Long, region: String) => (if (region == null) "UNKNOWN" else region, clicks))

        // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
        .map((_, regionWithClicks) => regionWithClicks)

        // Compute the total per region by summing the individual click counts per region.
        .groupByKey
        .reduce(_ + _)

    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopic)

    val streams: KafkaStreams = new KafkaStreams(builder.build(), streamsConfiguration)
    streams.start()


    val actualClicksPerRegion: java.util.List[KeyValue[String, Long]] = 
      produceNConsume(userClicksTopic, userRegionsTopic, outputTopic)

    streams.close()

    import collection.JavaConverters._
    assertEquals(actualClicksPerRegion.asScala.sortBy(_.key), expectedClicksPerRegion.sortBy(_.key))
  }

  @Test def testShouldCountClicksPerRegionJava(): Unit = {

    import org.apache.kafka.streams.{KafkaStreams => KafkaStreamsJ, StreamsBuilder => StreamsBuilderJ}
    import org.apache.kafka.streams.kstream.{KTable => KTableJ, KStream => KStreamJ, KGroupedStream => KGroupedStreamJ, _}
    import collection.JavaConverters._
    import java.lang.{Long => JLong}

    val streamsConfiguration: Properties = getStreamsConfiguration()

    streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())
    streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName())

    val builder: StreamsBuilderJ = new StreamsBuilderJ()

    val userClicksStream: KStreamJ[String, JLong] = 
      builder.stream[String, JLong](userClicksTopicJ, Consumed.`with`(Serdes.String(), Serdes.Long()))

    val userRegionsTable: KTableJ[String, String] = 
      builder.table[String, String](userRegionsTopicJ, Consumed.`with`(Serdes.String(), Serdes.String()))

    // Join the stream against the table.
    val userClicksJoinRegion: KStreamJ[String, (String, JLong)] = userClicksStream
      .leftJoin(userRegionsTable, 
        new ValueJoiner[JLong, String, (String, JLong)] {
          def apply(clicks: JLong, region: String): (String, JLong) = 
            (if (region == null) "UNKNOWN" else region, clicks)
        }, 
        Joined.`with`[String, JLong, String](Serdes.String(), Serdes.Long(), Serdes.String())) 

    // Change the stream from <user> -> <region, clicks> to <region> -> <clicks>
    val clicksByRegion : KStreamJ[String, JLong] = userClicksJoinRegion
      .map { 
        new KeyValueMapper[String, (String, JLong), KeyValue[String, JLong]] {
          def apply(k: String, regionWithClicks: (String, JLong)) = new KeyValue[String, JLong](regionWithClicks._1, regionWithClicks._2)
        }
      }
        
    // Compute the total per region by summing the individual click counts per region.
    val clicksPerRegion: KTableJ[String, JLong] = clicksByRegion
      .groupByKey(Serialized.`with`(Serdes.String(), Serdes.Long()))
      .reduce {
        new Reducer[JLong] {
          def apply(v1: JLong, v2: JLong) = v1 + v2
        }
      }
        
    // Write the (continuously updating) results to the output topic.
    clicksPerRegion.toStream.to(outputTopicJ, Produced.`with`(Serdes.String(), Serdes.Long()))

    val streams: KafkaStreamsJ = new KafkaStreamsJ(builder.build(), streamsConfiguration)

    streams.start()

    val actualClicksPerRegion: java.util.List[KeyValue[String, Long]] = 
      produceNConsume(userClicksTopicJ, userRegionsTopicJ, outputTopicJ)

    streams.close()
    assertEquals(actualClicksPerRegion.asScala.sortBy(_.key), expectedClicksPerRegion.sortBy(_.key))
  }

  private def getStreamsConfiguration(): Properties = {
    val streamsConfiguration: Properties = new Properties()

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-table-join-scala-integration-test")
    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000")
    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, testFolder.getRoot().getPath())

    streamsConfiguration
  }

  private def getUserRegionsProducerConfig(): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.RETRIES_CONFIG, "0")
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p
  }

  private def getUserClicksProducerConfig(): Properties = {
    val p = new Properties()
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    p.put(ProducerConfig.ACKS_CONFIG, "all")
    p.put(ProducerConfig.RETRIES_CONFIG, "0")
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    p
  }

  private def getConsumerConfig(): Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cluster.bootstrapServers())
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "join-scala-integration-test-standard-consumer")
    p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer])
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer])
    p
  }

  private def produceNConsume(userClicksTopic: String, userRegionsTopic: String, outputTopic: String): java.util.List[KeyValue[String, Long]] = {

    import collection.JavaConverters._
    
    // Publish user-region information.
    val userRegionsProducerConfig: Properties = getUserRegionsProducerConfig()
    IntegrationTestUtils.produceKeyValuesSynchronously(userRegionsTopic, userRegions.asJava, userRegionsProducerConfig, mockTime, false)

    // Publish user-click information.
    val userClicksProducerConfig: Properties = getUserClicksProducerConfig()
    IntegrationTestUtils.produceKeyValuesSynchronously(userClicksTopic, userClicks.asJava, userClicksProducerConfig, mockTime, false)

    // consume and verify result
    val consumerConfig = getConsumerConfig()

    IntegrationTestUtils.waitUntilMinKeyValueRecordsReceived(consumerConfig, outputTopic, expectedClicksPerRegion.size)
  }
}

