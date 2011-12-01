/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import kafka.utils.{Utils, Logging}
import kafka.consumer._
import kafka.producer.{ProducerData, ProducerConfig, Producer}
import kafka.message.Message
import java.util.concurrent.CountDownLatch

import scala.collection.Map

class KafkaServerStartable(val serverConfig: KafkaConfig,
                           val consumerConfig: ConsumerConfig,
                           val producerConfig: ProducerConfig) {
  private var server : KafkaServer = null
  private var embeddedConsumer : EmbeddedConsumer = null

  init

  def this(serverConfig: KafkaConfig) = this(serverConfig, null, null)

  private def init() {
    server = new KafkaServer(serverConfig)
    if (consumerConfig != null)
      embeddedConsumer =
        new EmbeddedConsumer(consumerConfig, producerConfig, server)
  }

  def startup() {
    server.startup
    if (embeddedConsumer != null)
      embeddedConsumer.startup
  }

  def shutdown() {
    if (embeddedConsumer != null)
      embeddedConsumer.shutdown
    server.shutdown
  }

  def awaitShutdown() {
    server.awaitShutdown
  }
}

class EmbeddedConsumer(private val consumerConfig: ConsumerConfig,
                       private val producerConfig: ProducerConfig,
                       private val kafkaServer: KafkaServer) extends TopicEventHandler[String] with Logging {

  private val whiteListTopics =
    consumerConfig.mirrorTopicsWhitelist.split(",").toList.map(_.trim)

  private val blackListTopics =
    consumerConfig.mirrorTopicsBlackList.split(",").toList.map(_.trim)

  // mirrorTopics should be accessed by handleTopicEvent only
  private var mirrorTopics:Seq[String] = List()

  private var consumerConnector: ConsumerConnector = null
  private var topicEventWatcher:ZookeeperTopicEventWatcher = null

  private val producer = new Producer[Null, Message](producerConfig)

  var threadList = List[MirroringThread]()

  private def isTopicAllowed(topic: String) = {
    if (consumerConfig.mirrorTopicsWhitelist.nonEmpty)
      whiteListTopics.contains(topic)
    else
      !blackListTopics.contains(topic)
  }

  // TopicEventHandler call-back only
  @Override
  def handleTopicEvent(allTopics: Seq[String]) {
    val newMirrorTopics = allTopics.filter(isTopicAllowed)

    val addedTopics = newMirrorTopics filterNot (mirrorTopics contains)
    if (addedTopics.nonEmpty)
      info("topic event: added topics = %s".format(addedTopics))

    val deletedTopics = mirrorTopics filterNot (newMirrorTopics contains)
    if (deletedTopics.nonEmpty)
      info("topic event: deleted topics = %s".format(deletedTopics))

    mirrorTopics = newMirrorTopics

    if (addedTopics.nonEmpty || deletedTopics.nonEmpty) {
      info("mirror topics = %s".format(mirrorTopics))
      startNewConsumerThreads(makeTopicMap(mirrorTopics))
    }
  }

  private def makeTopicMap(mirrorTopics: Seq[String]) = {
    if (mirrorTopics.nonEmpty)
      Utils.getConsumerTopicMap(mirrorTopics.mkString(
        "", ":%d,".format(consumerConfig.mirrorConsumerNumThreads),
        ":%d".format(consumerConfig.mirrorConsumerNumThreads)))
    else
      Utils.getConsumerTopicMap("")
  }

  private def startNewConsumerThreads(topicMap: Map[String, Int]) {
    if (topicMap.nonEmpty) {
      if (consumerConnector != null)
        consumerConnector.shutdown()

      /**
       * Before starting new consumer threads for the updated set of topics,
       * shutdown the existing mirroring threads. Since the consumer connector
       * is already shutdown, the mirroring threads should finish their task almost
       * instantaneously. If they don't, this points to an error that needs to be looked
       * into, and further mirroring should stop
       */
      threadList.foreach(_.shutdown)

      // KAFKA: 212: clear the thread list to remove the older thread references that are already shutdown
      threadList = Nil

      consumerConnector = Consumer.create(consumerConfig)
      val topicMessageStreams =  consumerConnector.createMessageStreams(topicMap)
      for ((topic, streamList) <- topicMessageStreams)
        for (i <- 0 until streamList.length)
          threadList ::= new MirroringThread(streamList(i), topic, i)

      threadList.foreach(_.start)
    }
    else
      info("Not starting mirroring threads (mirror topic list is empty)")
  }

  def startup() {
    topicEventWatcher = new ZookeeperTopicEventWatcher(consumerConfig, this)
    /*
    * consumer threads are (re-)started upon topic events (which includes an
    * initial startup event which lists the current topics)
    */
  }

  def shutdown() {
    // first shutdown the topic watcher to prevent creating new consumer streams
    if (topicEventWatcher != null)
      topicEventWatcher.shutdown()
    info("Stopped the ZK watcher for new topics, now stopping the Kafka consumers")
    // stop pulling more data for mirroring
    if (consumerConnector != null)
      consumerConnector.shutdown()
    info("Stopped the kafka consumer threads for existing topics, now stopping the existing mirroring threads")
    // wait for all mirroring threads to stop
    threadList.foreach(_.shutdown)
    info("Stopped all existing mirroring threads, now stopping the producer")
    // only then, shutdown the producer
    producer.close()
    info("Successfully shutdown this Kafka mirror")
  }

  class MirroringThread(val stream: KafkaMessageStream[Message], val topic: String, val threadId: Int) extends Thread with Logging {
    val shutdownComplete = new CountDownLatch(1)
    val name = "kafka-embedded-consumer-%s-%d".format(topic, threadId)
    this.setDaemon(false)
    this.setName(name)


    override def run = {
      info("Starting mirroring thread %s for topic %s and stream %d".format(name, topic, threadId))

      try {
        for (message <- stream) {
          val pd = new ProducerData[Null, Message](topic, message)
          producer.send(pd)
        }
      }
      catch {
        case e =>
          fatal(e + Utils.stackTrace(e))
          fatal(topic + " stream " + threadId + " unexpectedly exited")
      }finally {
        shutdownComplete.countDown
        info("Stopped mirroring thread %s for topic %s and stream %d".format(name, topic, threadId))
      }
    }

    def shutdown = {
      try {
        shutdownComplete.await
      }catch {
        case e: InterruptedException => fatal("Shutdown of thread " + name + " interrupted. " +
          "Mirroring thread might leak data!")
      }
    }
  }
}


