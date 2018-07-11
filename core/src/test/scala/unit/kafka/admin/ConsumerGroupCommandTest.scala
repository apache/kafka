/*
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

package kafka.admin

import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.{Collections, Properties}

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.consumer.{KafkaConsumer, RangeAssignor}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.{After, Before}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class ConsumerGroupCommandTest extends KafkaServerTestHarness {
  import ConsumerGroupCommandTest._

  val topic = "foo"
  val group = "test.group"

  private var consumerGroupService: List[ConsumerGroupService] = List()
  private var consumerGroupExecutors: List[AbstractConsumerGroupExecutor] = List()

  // configure the servers and clients
  override def generateConfigs = {
    TestUtils.createBrokerConfigs(1, zkConnect, enableControlledShutdown = false).map { props =>
      KafkaConfig.fromProps(props)
    }
  }

  @Before
  override def setUp() {
    super.setUp()
    adminZkClient.createTopic(topic, 1, 1)
  }

  @After
  override def tearDown(): Unit = {
    consumerGroupService.foreach(_.close())
    consumerGroupExecutors.foreach(_.shutdown())
    super.tearDown()
  }

  def committedOffsets(topic: String = topic, group: String = group): Map[TopicPartition, Long] = {
    val props = new Properties
    props.put("bootstrap.servers", brokerList)
    props.put("group.id", group)
    val consumer = new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)
    try {
      consumer.partitionsFor(topic).asScala.flatMap { partitionInfo =>
        val tp = new TopicPartition(partitionInfo.topic, partitionInfo.partition)
        val committed = consumer.committed(tp)
        if (committed == null)
          None
        else
          Some(tp -> committed.offset)
      }.toMap
    } finally {
      consumer.close()
    }
  }

  def getConsumerGroupService(args: Array[String]): ConsumerGroupService = {
    val opts = new ConsumerGroupCommandOptions(args)
    val service = new ConsumerGroupService(opts)
    consumerGroupService = service :: consumerGroupService
    service
  }

  def addConsumerGroupExecutor(numConsumers: Int,
                               topic: String = topic,
                               group: String = group,
                               strategy: String = classOf[RangeAssignor].getName): ConsumerGroupExecutor = {
    val executor = new ConsumerGroupExecutor(brokerList, numConsumers, group, topic, strategy)
    addExecutor(executor)
    executor
  }

  def addSimpleGroupExecutor(partitions: Iterable[TopicPartition] = Seq(new TopicPartition(topic, 0)),
                             group: String = group): SimpleConsumerGroupExecutor = {
    val executor = new SimpleConsumerGroupExecutor(brokerList, group, partitions)
    addExecutor(executor)
    executor
  }

  private def addExecutor(executor: AbstractConsumerGroupExecutor): AbstractConsumerGroupExecutor = {
    consumerGroupExecutors = executor :: consumerGroupExecutors
    executor
  }

}

object ConsumerGroupCommandTest {

  abstract class AbstractConsumerRunnable(broker: String, groupId: String) extends Runnable {
    val props = new Properties
    configure(props)
    val consumer = new KafkaConsumer(props)

    def configure(props: Properties): Unit = {
      props.put("bootstrap.servers", broker)
      props.put("group.id", groupId)
      props.put("key.deserializer", classOf[StringDeserializer].getName)
      props.put("value.deserializer", classOf[StringDeserializer].getName)
    }

    def subscribe(): Unit

    def run() {
      try {
        subscribe()
        while (true)
          consumer.poll(Long.MaxValue)
      } catch {
        case _: WakeupException => // OK
      } finally {
        consumer.close()
      }
    }

    def shutdown() {
      consumer.wakeup()
    }
  }

  class ConsumerRunnable(broker: String, groupId: String, topic: String, strategy: String)
    extends AbstractConsumerRunnable(broker, groupId) {

    override def configure(props: Properties): Unit = {
      super.configure(props)
      props.put("partition.assignment.strategy", strategy)
    }

    override def subscribe(): Unit = {
      consumer.subscribe(Collections.singleton(topic))
    }
  }

  class SimpleConsumerRunnable(broker: String, groupId: String, partitions: Iterable[TopicPartition])
    extends AbstractConsumerRunnable(broker, groupId) {

    override def subscribe(): Unit = {
      consumer.assign(partitions.toList.asJava)
    }
  }

  class AbstractConsumerGroupExecutor(numThreads: Int) {
    private val executor: ExecutorService = Executors.newFixedThreadPool(numThreads)
    private val consumers = new ArrayBuffer[AbstractConsumerRunnable]()

    def submit(consumerThread: AbstractConsumerRunnable) {
      consumers += consumerThread
      executor.submit(consumerThread)
    }

    def shutdown() {
      consumers.foreach(_.shutdown())
      executor.shutdown()
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS)
    }
  }

  class ConsumerGroupExecutor(broker: String, numConsumers: Int, groupId: String, topic: String, strategy: String)
    extends AbstractConsumerGroupExecutor(numConsumers) {

    for (_ <- 1 to numConsumers) {
      submit(new ConsumerRunnable(broker, groupId, topic, strategy))
    }

  }

  class SimpleConsumerGroupExecutor(broker: String, groupId: String, partitions: Iterable[TopicPartition])
    extends AbstractConsumerGroupExecutor(1) {

    submit(new SimpleConsumerRunnable(broker, groupId, partitions))
  }

}

