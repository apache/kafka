/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

import java.time.Duration
import java.util.Properties

import kafka.admin.AdminUtils.assignReplicasToBrokers
import kafka.admin.BrokerMetadata
import kafka.server.KafkaServer
import kafka.utils.TestUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.clients.CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG
import org.apache.kafka.clients.admin.{Admin, AdminClient}
import org.apache.kafka.clients.consumer.{ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageHistory, LocalTieredStorageSnapshot}
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.common.utils.Utils
import unit.kafka.utils.BrokerLocalStorage

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.collection.{Seq, mutable}

final class TieredStorageTestContext(private val zookeeperClient: KafkaZkClient,
                                     private val brokers: Seq[KafkaServer],
                                     private val producerConfig: Properties,
                                     private val consumerConfig: Properties,
                                     private val securityProtocol: SecurityProtocol) {

  private val (ser, de) = (Serdes.String().serializer(), Serdes.String().deserializer())
  private val topicSpecs = mutable.Map[String, TopicSpec]()

  @volatile private var producer: KafkaProducer[String, String] = _
  @volatile private var consumer: KafkaConsumer[String, String] = _
  @volatile private var adminClient: Admin = _

  @volatile private var tieredStorages: Seq[LocalTieredStorage] = _
  @volatile private var localStorages: Seq[BrokerLocalStorage] = _

  initContext()

  def initContext(): Unit = {
    val bootstrapServerString = TestUtils.getBrokerListStrFromServers(brokers, securityProtocol)
    producerConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)
    consumerConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)

    val adminConfig = new Properties()
    adminConfig.put(BOOTSTRAP_SERVERS_CONFIG, bootstrapServerString)

    producer = new KafkaProducer[String, String](producerConfig, ser, ser)
    consumer = new KafkaConsumer[String, String](consumerConfig, de, de)
    adminClient = AdminClient.create(adminConfig)

    tieredStorages = TieredStorageTestHarness.getTieredStorages(brokers)
    localStorages = TieredStorageTestHarness.getLocalStorages(brokers)
  }

  def createTopic(spec: TopicSpec): Unit = {
    val metadata = brokers.head.metadataCache.getAliveBrokers.map(b => BrokerMetadata(b.id, b.rack))
    val assignments = assignReplicasToBrokers(metadata, spec.partitionCount, spec.replicationFactor, 0, 0)
    TestUtils.createTopic(zookeeperClient, spec.topicName, assignments, brokers, spec.properties)

    topicSpecs.synchronized { topicSpecs += spec.topicName -> spec }
  }

  def produce(records: Iterable[ProducerRecord[String, String]]) = {
    records.foreach(producer.send(_).get())
  }

  def consume(topicPartition: TopicPartition,
              numberOfRecords: Int,
              fetchOffset: Long = 0): Seq[ConsumerRecord[String, String]] = {

    consumer.assign(Seq(topicPartition).asJava)
    consumer.seek(topicPartition, fetchOffset)

    val records = new ArrayBuffer[ConsumerRecord[String, String]]
    def pollAction(polledRecords: ConsumerRecords[String, String]): Boolean = {
      records ++= polledRecords.asScala
      records.size >= numberOfRecords
    }

    val timeoutMs = 60000L
    val sep = System.lineSeparator()

    TestUtils.pollRecordsUntilTrue(consumer,
      pollAction,
      waitTimeMs = timeoutMs,
      msg = s"Could not consume $numberOfRecords records of $topicPartition from offset $fetchOffset " +
        s"in $timeoutMs ms. ${records.size} message(s) consumed:$sep ${records.mkString(sep)}")

    records
  }

  def bounce(brokerId: Int): Unit = {
    val broker = brokers(brokerId)

    closeClients()

    broker.shutdown()
    broker.awaitShutdown()
    broker.startup()

    initContext()
  }

  def stop(brokerId: Int): Unit = {
    val broker = brokers(brokerId)

    closeClients()

    broker.shutdown()
    broker.awaitShutdown()

    initContext()
  }

  def start(brokerId: Int): Unit = {
    val broker = brokers(brokerId)

    closeClients()

    broker.startup()

    initContext()
  }

  def eraseBrokerStorage(brokerId: Int): Unit = {
    localStorages(brokerId).eraseStorage()
  }

  def topicSpec(topicName: String) = topicSpecs.synchronized { topicSpecs(topicName) }

  def takeTieredStorageSnapshot(): LocalTieredStorageSnapshot = {
    LocalTieredStorageSnapshot.takeSnapshot(tieredStorages.head)
  }

  def getTieredStorageHistory(brokerId: Int): LocalTieredStorageHistory = tieredStorages(brokerId).getHistory

  def getTieredStorages: Seq[LocalTieredStorage] = tieredStorages

  def getLocalStorages: Seq[BrokerLocalStorage] = localStorages

  def admin() = adminClient

  def close(): Unit = {
    getTieredStorages.find(_ => true).foreach(_.clear())
    Utils.closeAll(producer, consumer)
    adminClient.close()
  }

  private def closeClients(): Unit = {
    producer.close(Duration.ofSeconds(5))
    consumer.close(Duration.ofSeconds(5))
    adminClient.close()
  }

}
