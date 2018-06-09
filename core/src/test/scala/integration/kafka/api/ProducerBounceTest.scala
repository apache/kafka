/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package kafka.api

import java.util.Properties
import java.util.concurrent.Future

import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{ShutdownableThread, TestUtils}
import kafka.utils.Implicits._
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.Assert._
import org.junit.Test

import scala.collection.mutable.ArrayBuffer

class ProducerBounceTest extends KafkaServerTestHarness {
  private val producerBufferSize =  65536
  private val serverMessageMaxBytes =  producerBufferSize/2

  val numServers = 4

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
  overridingProps.put(KafkaConfig.MessageMaxBytesProp, serverMessageMaxBytes.toString)
  // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
  // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)
  overridingProps.put(KafkaConfig.ControlledShutdownEnableProp, true.toString)
  overridingProps.put(KafkaConfig.UncleanLeaderElectionEnableProp, false.toString)
  overridingProps.put(KafkaConfig.AutoLeaderRebalanceEnableProp, false.toString)
  // This is the one of the few tests we currently allow to preallocate ports, despite the fact that this can result in transient
  // failures due to ports getting reused. We can't use random ports because of bad behavior that can result from bouncing
  // brokers too quickly when they get new, random ports. If we're not careful, the client can end up in a situation
  // where metadata is not refreshed quickly enough, and by the time it's actually trying to, all the servers have
  // been bounced and have new addresses. None of the bootstrap nodes or current metadata can get them connected to a
  // running server.
  //
  // Since such quick rotation of servers is incredibly unrealistic, we allow this one test to preallocate ports, leaving
  // a small risk of hitting errors due to port conflicts. Hopefully this is infrequent enough to not cause problems.
  override def generateConfigs = {
    FixedPortTestUtils.createBrokerConfigs(numServers, zkConnect,enableControlledShutdown = true)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  private val topic1 = "topic-1"

  /**
   * With replication, producer should able to find new leader after it detects broker failure and consumers should
   * be able to consume all messages without duplicates.
   */
  @Test
  def testBrokerFailure() {
    val numPartitions = 3
    val topicConfig = new Properties()
    topicConfig.put(KafkaConfig.MinInSyncReplicasProp, 2.toString)
    createTopic(topic1, numPartitions, numServers, topicConfig)

    val scheduler = new ProducerScheduler()
    scheduler.start()

    // rolling bounce brokers

    for (_ <- 0 until numServers) {
      for (server <- servers) {
        info("Shutting down server : %s".format(server.config.brokerId))
        server.shutdown()
        server.awaitShutdown()
        info("Server %s shut down. Starting it up again.".format(server.config.brokerId))
        server.startup()
        info("Restarted server: %s".format(server.config.brokerId))
      }

      // Make sure the producer do not see any exception in returned metadata due to broker failures
      assertFalse(scheduler.failed)

      // Make sure the leader still exists after bouncing brokers
      (0 until numPartitions).foreach(partition => TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic1, partition))
    }

    scheduler.shutdown()

    // Make sure the producer do not see any exception
    // when draining the left messages on shutdown
    assertFalse(scheduler.failed)

    // double check that the leader info has been propagated after consecutive bounces
    (0 until numPartitions).map(i => TestUtils.waitUntilMetadataIsPropagated(servers, topic1, i))
    val consumer = TestUtils.createNewConsumer(brokerList, securityProtocol = SecurityProtocol.PLAINTEXT,
      valueDeserializer = new StringDeserializer)
    val consumerRecords =
      try TestUtils.consumeRecords(consumer, scheduler.sent)
      finally consumer.close()
    val recordValues = consumerRecords.map(_.value)
    val uniqueRecordValues = recordValues.toSet
    info(s"number of unique messages sent: ${uniqueRecordValues.size}")
    assertEquals(s"Found ${recordValues.size - uniqueRecordValues.size} duplicate messages.", uniqueRecordValues.size, recordValues.size)
  }

  private class ProducerScheduler extends ShutdownableThread("daemon-producer", false) {
    val numRecords = 1000
    var sent = 0
    var failed = false

    val producerConfig = new Properties()
    producerConfig.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    producerConfig.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")
    val producerConfigWithCompression = new Properties()
    producerConfigWithCompression ++= producerConfig
    producerConfigWithCompression.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    val producers = List(
      TestUtils.createNewProducer(brokerList, bufferSize = producerBufferSize / 4, retries = 10, props = Some(producerConfig)),
      TestUtils.createNewProducer(brokerList, bufferSize = producerBufferSize / 2, retries = 10, lingerMs = 5000, props = Some(producerConfig)),
      TestUtils.createNewProducer(brokerList, bufferSize = producerBufferSize, retries = 10, lingerMs = 10000, props = Some(producerConfigWithCompression))
    )

    override def doWork(): Unit = {
      info("Starting to send messages..")
      var producerId = 0
      val responses = new ArrayBuffer[IndexedSeq[Future[RecordMetadata]]]()
      for (producer <- producers) {
        val response =
          for (i <- sent+1 to sent+numRecords)
            yield producer.send(new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, null, ((producerId + 1) * i).toString.getBytes),
              new ErrorLoggingCallback(topic1, null, null, true))
        responses.append(response)
        producerId += 1
      }

      try {
        for (response <- responses) {
          val futures = response.toList
          futures.map(_.get)
          sent += numRecords
        }
        info(s"Sent $sent records")
      } catch {
        case e : Exception =>
          error(s"Got exception ${e.getMessage}")
          e.printStackTrace()
          failed = true
      }
    }

    override def shutdown(){
      super.shutdown()
      for (producer <- producers) {
        producer.close()
      }
    }
  }
}
