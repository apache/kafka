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

import java.time.Duration
import java.util.concurrent.{ExecutorService, Executors, TimeUnit}
import java.util.{Collections, Properties}

import kafka.admin.ConsumerGroupCommand.{ConsumerGroupCommandOptions, ConsumerGroupService}
import kafka.integration.KafkaServerTestHarness
import kafka.server.KafkaConfig
import kafka.utils.TestUtils
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.consumer.{KafkaConsumer, RangeAssignor}
import org.apache.kafka.common.{PartitionInfo, TopicPartition}
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.junit.{After, Before}

import scala.jdk.CollectionConverters._
import scala.collection.mutable.ArrayBuffer

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
  override def setUp(): Unit = {
    super.setUp()
    createTopic(topic, 1, 1)
  }

  @After
  override def tearDown(): Unit = {
    consumerGroupService.foreach(_.close())
    consumerGroupExecutors.foreach(_.shutdown())
    super.tearDown()
  }

  def committedOffsets(topic: String = topic, group: String = group): collection.Map[TopicPartition, Long] = {
    val consumer = createNoAutoCommitConsumer(group)
    try {
      val partitions: Set[TopicPartition] = consumer.partitionsFor(topic)
        .asScala.toSet.map {partitionInfo : PartitionInfo => new TopicPartition(partitionInfo.topic, partitionInfo.partition)}
      consumer.committed(partitions.asJava).asScala.filter(_._2 != null).map { case (k, v) => k -> v.offset }
    } finally {
      consumer.close()
    }
  }

  def createNoAutoCommitConsumer(group: String): KafkaConsumer[String, String] = {
    val props = new Properties
    props.put("bootstrap.servers", brokerList)
    props.put("group.id", group)
    props.put("enable.auto.commit", "false")
    new KafkaConsumer(props, new StringDeserializer, new StringDeserializer)
  }

  def getConsumerGroupService(args: Array[String]): ConsumerGroupService = {
    val opts = new ConsumerGroupCommandOptions(args)
    val service = new ConsumerGroupService(opts, Map(AdminClientConfig.RETRIES_CONFIG -> Int.MaxValue.toString))
    consumerGroupService = service :: consumerGroupService
    service
  }

  def addConsumerGroupExecutor(numConsumers: Int,
                               topic: String = topic,
                               group: String = group,
                               strategy: String = classOf[RangeAssignor].getName,
                               customPropsOpt: Option[Properties] = None,
                               syncCommit: Boolean = false): ConsumerGroupExecutor = {
    val executor = new ConsumerGroupExecutor(brokerList, numConsumers, group, topic, strategy, customPropsOpt, syncCommit)
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

  abstract class AbstractConsumerRunnable(broker: String, groupId: String, customPropsOpt: Option[Properties] = None,
                                          syncCommit: Boolean = false) extends Runnable {
    val props = new Properties
    configure(props)
    customPropsOpt.foreach(props.asScala ++= _.asScala)
    val consumer = new KafkaConsumer(props)

    def configure(props: Properties): Unit = {
      props.put("bootstrap.servers", broker)
      props.put("group.id", groupId)
      props.put("key.deserializer", classOf[StringDeserializer].getName)
      props.put("value.deserializer", classOf[StringDeserializer].getName)
    }

    def subscribe(): Unit

    def run(): Unit = {
      try {
        subscribe()
        while (true) {
          consumer.poll(Duration.ofMillis(Long.MaxValue))
          if (syncCommit)
            consumer.commitSync()
        }
      } catch {
        case _: WakeupException => // OK
      } finally {
        consumer.close()
      }
    }

    def shutdown(): Unit = {
      consumer.wakeup()
    }
  }

  class ConsumerRunnable(broker: String, groupId: String, topic: String, strategy: String,
                         customPropsOpt: Option[Properties] = None, syncCommit: Boolean = false)
    extends AbstractConsumerRunnable(broker, groupId, customPropsOpt, syncCommit) {

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

    def submit(consumerThread: AbstractConsumerRunnable): Unit = {
      consumers += consumerThread
      executor.submit(consumerThread)
    }

    def shutdown(): Unit = {
      consumers.foreach(_.shutdown())
      executor.shutdown()
      executor.awaitTermination(5000, TimeUnit.MILLISECONDS)
    }
  }

  class ConsumerGroupExecutor(broker: String, numConsumers: Int, groupId: String, topic: String, strategy: String,
                              customPropsOpt: Option[Properties] = None, syncCommit: Boolean = false)
    extends AbstractConsumerGroupExecutor(numConsumers) {

    for (_ <- 1 to numConsumers) {
      submit(new ConsumerRunnable(broker, groupId, topic, strategy, customPropsOpt, syncCommit))
    }

  }

  class SimpleConsumerGroupExecutor(broker: String, groupId: String, partitions: Iterable[TopicPartition])
    extends AbstractConsumerGroupExecutor(1) {

    submit(new SimpleConsumerRunnable(broker, groupId, partitions))
  }

}

