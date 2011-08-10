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

import org.apache.log4j.Logger
import kafka.consumer.{Consumer, ConsumerConnector, ConsumerConfig}
import kafka.utils.{SystemTime, Utils}
import kafka.api.RequestKeys
import kafka.message.{NoCompressionCodec, ByteBufferMessageSet}

class KafkaServerStartable(val serverConfig: KafkaConfig, val consumerConfig: ConsumerConfig) {
  private var server : KafkaServer = null
  private var embeddedConsumer : EmbeddedConsumer = null

  init

  def this(serverConfig: KafkaConfig) = this(serverConfig, null)

  private def init() {
    server = new KafkaServer(serverConfig)
    if (consumerConfig != null)
      embeddedConsumer = new EmbeddedConsumer(consumerConfig, server)
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
                       private val kafkaServer: KafkaServer) {
  private val logger = Logger.getLogger(getClass())
  private val consumerConnector: ConsumerConnector = Consumer.create(consumerConfig)
  private val topicMessageStreams = consumerConnector.createMessageStreams(consumerConfig.embeddedConsumerTopicMap)

  def startup() = {
    var threadList = List[Thread]()
    for ((topic, streamList) <- topicMessageStreams)
      for (i <- 0 until streamList.length)
        threadList ::= Utils.newThread("kafka-embedded-consumer-" + topic + "-" + i, new Runnable() {
          def run() {
            logger.info("starting consumer thread " + i + " for topic " + topic)
            val logManager = kafkaServer.getLogManager
            val stats = kafkaServer.getStats
            try {
              for (message <- streamList(i)) {
                val partition = logManager.chooseRandomPartition(topic)
                val start = SystemTime.nanoseconds
                logManager.getOrCreateLog(topic, partition).append(
                                                          new ByteBufferMessageSet(compressionCodec = NoCompressionCodec,
                                                          messages = message))
                stats.recordRequest(RequestKeys.Produce, SystemTime.nanoseconds - start)
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
      thread.start
  }

  def shutdown() = {
    consumerConnector.shutdown
  }
}
