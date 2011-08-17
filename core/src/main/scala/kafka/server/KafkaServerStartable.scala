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

import kafka.utils.Utils
import kafka.consumer._
import kafka.producer.{ProducerData, ProducerConfig, Producer}
import kafka.message.Message
import org.apache.log4j.Logger

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
                       private val kafkaServer: KafkaServer) extends TopicEventHandler[String] {

  private val logger = Logger.getLogger(getClass)

  private val whiteListTopics =
    consumerConfig.mirrorTopicsWhitelist.split(",").toList.map(_.trim)

  private val blackListTopics =
    consumerConfig.mirrorTopicsBlackList.split(",").toList.map(_.trim)

  // mirrorTopics should be accessed by handleTopicEvent only
  private var mirrorTopics:Seq[String] = List()

  private var consumerConnector: ConsumerConnector = null
  private var topicEventWatcher:ZookeeperTopicEventWatcher = null

  private val producer = new Producer[Null, Message](producerConfig)


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
      logger.info("topic event: added topics = %s".format(addedTopics))

    val deletedTopics = mirrorTopics filterNot (newMirrorTopics contains)
    if (deletedTopics.nonEmpty)
      logger.info("topic event: deleted topics = %s".format(deletedTopics))

    mirrorTopics = newMirrorTopics

    if (addedTopics.nonEmpty || deletedTopics.nonEmpty) {
      logger.info("mirror topics = %s".format(mirrorTopics))
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
      consumerConnector = Consumer.create(consumerConfig)
      val topicMessageStreams =  consumerConnector.createMessageStreams(topicMap)
      var threadList = List[Thread]()
      for ((topic, streamList) <- topicMessageStreams)
        for (i <- 0 until streamList.length)
          threadList ::= Utils.newThread("kafka-embedded-consumer-%s-%d".format(topic, i), new Runnable() {
            def run() {
              logger.info("Starting consumer thread %d for topic %s".format(i, topic))

              try {
                for (message <- streamList(i)) {
                  val pd = new ProducerData[Null, Message](topic, message)
                  producer.send(pd)
                }
              }
              catch {
                case e =>
                  logger.fatal(e + Utils.stackTrace(e))
                  logger.fatal(topic + " stream " + i + " unexpectedly exited")
              }
            }
          }, false)

      for (thread <- threadList)
        thread.start()
    }
    else
      logger.info("Not starting consumer threads (mirror topic list is empty)")
  }

  def startup() {
    topicEventWatcher = new ZookeeperTopicEventWatcher(consumerConfig, this)
    /*
     * consumer threads are (re-)started upon topic events (which includes an
     * initial startup event which lists the current topics)
     */
   }

  def shutdown() {
    producer.close()
    if (consumerConnector != null)
      consumerConnector.shutdown()
    if (topicEventWatcher != null)
      topicEventWatcher.shutdown()
  }
}

