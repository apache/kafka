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
package kafka.tiered.storage

import java.nio.ByteBuffer
import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageSnapshot, RemoteLogSegmentFileset}
import org.apache.kafka.common.log.remote.storage.LocalTieredStorageWatcher.newWatcherBuilder
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.junit.Assert.{assertEquals, assertFalse, assertTrue}

import scala.collection.{Seq, mutable}

/**
  * Helps define the specifications of a test case to exercise the support for tiered storage in Apache Kafka.
  * This class encapsulates the execution of the tests and the assertions which ensure the tiered storage was
  * exercised as expected.
  */
final class TieredStorageTestCase(val recordsToProduce: Map[TopicPartition, Seq[ProducerRecord[String, String]]],
                                  val offloadedSegments: Map[TopicPartition, Seq[OffloadedSegmentSpec]],
                                  val producer: KafkaProducer[String, String],
                                  val consumer: KafkaConsumer[String, String],
                                  val tieredStorage: LocalTieredStorage,
                                  val kafkaStorageWatcher: StorageWatcher) {

  val offloadWaitTimeoutSec = 5

  def execute(): Unit = {
    //
    // The watcher subscribes to modifications of the local tiered storage and waits until
    // the expected segments are all found.
    //
    val tieredStorageWatcher = {
      val watcherBuilder = newWatcherBuilder()
      offloadedSegments.foreach { x => watcherBuilder.addSegmentsToWaitFor(x._1, x._2.length) }
      watcherBuilder.create(tieredStorage)
    }

    recordsToProduce.values.flatten.foreach(producer.send(_).get())

    tieredStorageWatcher.watch(offloadWaitTimeoutSec, TimeUnit.SECONDS)
  }

  def verify(): Unit = {
    import scala.collection.JavaConverters._

    val snapshot = LocalTieredStorageSnapshot.takeSnapshot(tieredStorage)

    offloadedSegments.foreach { x: (TopicPartition, Seq[OffloadedSegmentSpec]) =>
      val topicPartition = x._1
      val specs = x._2

      snapshot.getFilesets(topicPartition).asScala
        .sortWith((x, y) => x.getRecords.get(0).offset() <= y.getRecords.get(0).offset())
        .zip(specs)
        .foreach {
          pair => compareRecords(pair._1, pair._2)
        }
    }
  }

  private def compareRecords(fileset: RemoteLogSegmentFileset, spec: OffloadedSegmentSpec): Unit = {
    import scala.collection.JavaConverters._

    // Records found in the local tiered storage.
    val discoveredRecords = fileset.getRecords.asScala

    // Records expected to be found, based on what was sent by the producer.
    val producerRecords = spec.records

    assertEquals(
      s"Invalid number of records found for topic-partition ${spec.topicPartition} " +
      s"for segment of expected based offset ${spec.baseOffset}.",
      producerRecords.length,
      discoveredRecords.length
    )

    producerRecords.zip(discoveredRecords).foreach {
        _ match {
          case (producerRecord: ProducerRecord[String, String], discoveredRecord: Record) =>
            Option(producerRecord.key())
              .map { key =>
                assertTrue(discoveredRecord.hasKey)
                assertEquals(
                  s"Key mismatch. Expected: $key",
                  ByteBuffer.wrap(key.getBytes()),
                  discoveredRecord.key()
                )
              }
            .getOrElse {
              assertFalse(discoveredRecord.hasKey)
            }

            val producerValue = ByteBuffer.wrap(producerRecord.value().getBytes())
            val discoceredValue = discoveredRecord.value()

            assertEquals(
              s"Producer value mismatch. Expected: ${producerRecord.value()}",
              producerValue,
              discoceredValue
            )
       }
    }

    assertEquals(
      "Base offset of segment mismatch",
      spec.baseOffset,
      discoveredRecords(0).offset()
    )
  }

  def tearDown(): Unit = {
    producer.close(Duration.ZERO)
  }
}

final case class OffloadedSegmentSpec(val topicPartition: TopicPartition,
                                      val baseOffset: Int,
                                      val records: Seq[ProducerRecord[String, String]])

final class TieredStorageTestCaseBuilder(private val kafkaServers: Seq[KafkaServer],
                                         private val zookeeperClient: KafkaZkClient,
                                         private val producerConfig: Properties,
                                         private val consumerConfig: Properties,
                                         private val storage: LocalTieredStorage,
                                         private val kafkaStorageDirectory: String) {

  val producables: mutable.Map[TopicPartition, mutable.Buffer[ProducerRecord[String, String]]] = mutable.Map()
  val offloadables: mutable.Map[TopicPartition, mutable.Buffer[(Int, Int)]] = mutable.Map()

  def withTopic(name: String, partitions: Int,
                segmentSize: Int = -1,
                deleteAfterOffload: Boolean = true): this.type = {

    val brokerCount = kafkaServers.length
    val topicProps = new Properties()

    //
    // Ensure offset and time indexes are generated for every record.
    //
    topicProps.put(TopicConfig.INDEX_INTERVAL_BYTES_CONFIG, 1.toString)

    //
    // Leverage the use of the segment index size to create a log-segment accepting one and only one record.
    // The minimum size of the indexes is that of an entry, which is 8 for the offset index and 12 for the
    // time index. Hence, since the topic is configured to generate index entries for every record with, for
    // a "small" number of records (i.e. such that the average record size times the number of records is
    // much less than the segment size), the number of records which hold in a segment is the multiple of 12
    // defined below.
    //
    if (segmentSize != -1) {
      assert(segmentSize >= 1)
      topicProps.put(TopicConfig.SEGMENT_INDEX_BYTES_CONFIG, (12 * segmentSize).toString)
    }

    //
    // To verify records physically absent from Kafka's storage can be consumed via the tiered storage, we
    // want to delete log segments as soon as possible. When the tiered storage is active, an inactive log
    // segment is not eligible for deletion until it has been offloaded, which guarantees all segments
    // should be offloaded before deletion, and their consumption is possible thereafter.
    //
    if (deleteAfterOffload) {
      topicProps.put(TopicConfig.RETENTION_BYTES_CONFIG, 1.toString)
      topicProps.put(TopicConfig.FILE_DELETE_DELAY_MS_CONFIG, 1.toString)
    }

    TestUtils.createTopic(zookeeperClient, name, partitions, brokerCount, kafkaServers, topicProps)
    this
  }

  def producing(topic: String, partition: Int, key: String, value: String): this.type = {
    val topicPartition = new TopicPartition(topic, partition)
    if (!producables.contains(topicPartition)) {
      producables += (topicPartition -> mutable.Buffer())
    }

    producables(topicPartition) += new ProducerRecord[String, String](topic, partition, key, value)
    this
  }

  def expectingSegmentToBeOffloaded(topic: String, partition: Int, baseOffset: Int, segmentSize: Int): this.type = {
    val tp = new TopicPartition(topic, partition)

    offloadables.get(tp) match {
      case Some(buffer) => buffer += ((baseOffset, segmentSize))
      case None => offloadables += tp -> mutable.Buffer((baseOffset, segmentSize))
    }

    this
  }

  def create(): TieredStorageTestCase = {
    // Creates the map of records to produce. Order of records is preserved at partition level.
    val recordsToProduce = Map() ++ producables.mapValues(Seq() ++ _)

    /**
      * Builds a specification of an offloaded segment.
      * This method modifies this builder's sequence of records to produce.
      */
    def makeSpec(topicPartition: TopicPartition, sizeAndOffset: (Int, Int)): OffloadedSegmentSpec = {
      sizeAndOffset match {
        case (baseOffset: Int, segmentSize: Int) =>
          val segments = (0 until segmentSize).map(_ => producables(topicPartition).remove(0))
          new OffloadedSegmentSpec(topicPartition, baseOffset, segments)
        }
    }

    // Creates the map of specifications of segments expected to be offloaded.
    val recordsOffloaded = offloadables.map { case (tp, offsetAndSizes) => (tp, offsetAndSizes.map(makeSpec (tp, _))) }

    val serializer = new StringSerializer
    val deserializer = new StringDeserializer
    val producer = new KafkaProducer[String, String](producerConfig, serializer, serializer)
    val consumer = new KafkaConsumer[String, String](consumerConfig, deserializer, deserializer)

    val kafkaStorageWatcher = new StorageWatcher(kafkaStorageDirectory)

    new TieredStorageTestCase(recordsToProduce, recordsOffloaded.toMap, producer, consumer, storage, kafkaStorageWatcher)
  }
}

object TieredStorageTestCaseBuilder {

  def newTestCase(kafkaServers: Seq[KafkaServer],
                  zookeeperClient: KafkaZkClient,
                  producerConfig: Properties,
                  consumerConfig: Properties,
                  storage: LocalTieredStorage,
                  kafkaStorageDirectory: String): TieredStorageTestCaseBuilder = {

    new TieredStorageTestCaseBuilder(kafkaServers, zookeeperClient, producerConfig, consumerConfig, storage, kafkaStorageDirectory)
  }
}
