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

import kafka.consumer.SimpleConsumer
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{ShutdownableThread, TestUtils}
import org.apache.kafka.clients.producer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.junit.Assert._
import org.junit.{After, Before, Test}

class ProducerBounceTest extends KafkaServerTestHarness {
  private val producerBufferSize = 30000
  private val serverMessageMaxBytes =  producerBufferSize/2

  val numServers = 2

  val overridingProps = new Properties()
  overridingProps.put(KafkaConfig.AutoCreateTopicsEnableProp, false.toString)
  overridingProps.put(KafkaConfig.MessageMaxBytesProp, serverMessageMaxBytes.toString)
  // Set a smaller value for the number of partitions for the offset commit topic (__consumer_offset topic)
  // so that the creation of that topic/partition(s) and subsequent leader assignment doesn't take relatively long
  overridingProps.put(KafkaConfig.OffsetsTopicPartitionsProp, 1.toString)

  // This is the one of the few tests we currently allow to preallocate ports, despite the fact that this can result in transient
  // failures due to ports getting reused. We can't use random ports because of bad behavior that can result from bouncing
  // brokers too quickly when they get new, random ports. If we're not careful, the client can end up in a situation
  // where metadata is not refreshed quickly enough, and by the time it's actually trying to, all the servers have
  // been bounced and have new addresses. None of the bootstrap nodes or current metadata can get them connected to a
  // running server.
  //
  // Since such quick rotation of servers is incredibly unrealistic, we allow this one test to preallocate ports, leaving
  // a small risk of hitting errors due to port conflicts. Hopefully this is infrequent enough to not cause problems.
  override def generateConfigs() = {
    FixedPortTestUtils.createBrokerConfigs(numServers, zkConnect,enableControlledShutdown = false)
      .map(KafkaConfig.fromProps(_, overridingProps))
  }

  private var consumer1: SimpleConsumer = null
  private var consumer2: SimpleConsumer = null

  private var producer1: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer2: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer3: KafkaProducer[Array[Byte],Array[Byte]] = null
  private var producer4: KafkaProducer[Array[Byte],Array[Byte]] = null

  private val topic1 = "topic-1"
  private val topic2 = "topic-2"

  @Before
  override def setUp() {
    super.setUp()

    producer1 = TestUtils.createNewProducer(brokerList, acks = 0, bufferSize = producerBufferSize)
    producer2 = TestUtils.createNewProducer(brokerList, acks = 1, bufferSize = producerBufferSize)
    producer3 = TestUtils.createNewProducer(brokerList, acks = -1, bufferSize = producerBufferSize)
  }

  @After
  override def tearDown() {
    if (producer1 != null) producer1.close
    if (producer2 != null) producer2.close
    if (producer3 != null) producer3.close
    if (producer4 != null) producer4.close

    super.tearDown()
  }

  /**
   * With replication, producer should able able to find new leader after it detects broker failure
   */
  @Test
  def testBrokerFailure() {
    val numPartitions = 3
    val leaders = TestUtils.createTopic(zkUtils, topic1, numPartitions, numServers, servers)
    assertTrue("Leader of all partitions of the topic should exist", leaders.values.forall(leader => leader.isDefined))

    val scheduler = new ProducerScheduler()
    scheduler.start

    // rolling bounce brokers
    for (i <- 0 until numServers) {
      for (server <- servers) {
        server.shutdown()
        server.awaitShutdown()
        server.startup()
        Thread.sleep(2000)
      }

      // Make sure the producer do not see any exception
      // in returned metadata due to broker failures
      assertTrue(scheduler.failed == false)

      // Make sure the leader still exists after bouncing brokers
      (0 until numPartitions).foreach(partition => TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic1, partition))
    }

    scheduler.shutdown

    // Make sure the producer do not see any exception
    // when draining the left messages on shutdown
    assertTrue(scheduler.failed == false)

    // double check that the leader info has been propagated after consecutive bounces
    val newLeaders = (0 until numPartitions).map(i => TestUtils.waitUntilMetadataIsPropagated(servers, topic1, i))
    val fetchResponses = newLeaders.zipWithIndex.map { case (leader, partition) =>
      // Consumers must be instantiated after all the restarts since they use random ports each time they start up
      val consumer = new SimpleConsumer("localhost", servers(leader).boundPort(), 100, 1024 * 1024, "")
      val response = consumer.fetch(new FetchRequestBuilder().addFetch(topic1, partition, 0, Int.MaxValue).build()).messageSet(topic1, partition)
      consumer.close
      response
    }
    val messages = fetchResponses.flatMap(r => r.iterator.toList.map(_.message))
    val uniqueMessages = messages.toSet
    val uniqueMessageSize = uniqueMessages.size

    assertEquals("Should have fetched " + scheduler.sent + " unique messages", scheduler.sent, uniqueMessageSize)
  }

  private class ProducerScheduler extends ShutdownableThread("daemon-producer", false)
  {
    val numRecords = 1000
    var sent = 0
    var failed = false

    val producer = TestUtils.createNewProducer(brokerList, bufferSize = producerBufferSize, retries = 10)

    override def doWork(): Unit = {
      val responses =
        for (i <- sent+1 to sent+numRecords)
        yield producer.send(new ProducerRecord[Array[Byte],Array[Byte]](topic1, null, null, i.toString.getBytes),
                            new ErrorLoggingCallback(topic1, null, null, true))
      val futures = responses.toList

      try {
        futures.map(_.get)
        sent += numRecords
      } catch {
        case e : Exception => failed = true
      }
    }

    override def shutdown(){
      super.shutdown()
      producer.close
    }
  }
}
