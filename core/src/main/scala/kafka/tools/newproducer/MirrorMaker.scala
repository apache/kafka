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

package kafka.tools.newproducer

import joptsimple.OptionParser
import kafka.utils.{Utils, CommandLineUtils, Logging}
import java.util.concurrent.CountDownLatch
import kafka.consumer._
import collection.mutable.ListBuffer
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord, KafkaProducer}
import java.util.concurrent.atomic.AtomicInteger


object MirrorMaker extends Logging {

  private var connector: ZookeeperConsumerConnector = null
  private var mirroringThreads: Seq[MirrorMakerThread] = null
  private var producerChannel: ProducerDataChannel = null

  def main(args: Array[String]) {
    info ("Starting mirror maker")
    val parser = new OptionParser

    val consumerConfigOpt = parser.accepts("consumer.config",
      "Consumer config file to consume from a source cluster.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val producerConfigOpt = parser.accepts("producer.config",
      "Embedded producer config file for target cluster.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val numStreamsOpt = parser.accepts("num.streams",
      "Number of mirroring streams.")
      .withRequiredArg()
      .describedAs("Number of threads")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)

    val whitelistOpt = parser.accepts("whitelist",
      "Whitelist of topics to mirror.")
      .withRequiredArg()
      .describedAs("Java regex (String)")
      .ofType(classOf[String])

    val blacklistOpt = parser.accepts("blacklist",
            "Blacklist of topics to mirror.")
            .withRequiredArg()
            .describedAs("Java regex (String)")
            .ofType(classOf[String])

    val helpOpt = parser.accepts("help", "Print this message.")
    val options = parser.parse(args : _*)
    if (options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      System.exit(0)
    }
    CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt)
    if (List(whitelistOpt, blacklistOpt).count(options.has) != 1) {
      fatal("Exactly one of whitelist or blacklist is required.")
      System.exit(1)
    }
    val filterSpec = if (options.has(whitelistOpt))
      new Whitelist(options.valueOf(whitelistOpt))
    else
      new Blacklist(options.valueOf(blacklistOpt))
    val producerConfig = options.valueOf(producerConfigOpt)
    val producerProps = Utils.loadProps(producerConfig)
    producerProps.setProperty(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    val consumerConfig = options.valueOf(consumerConfigOpt)
    val numStreams = options.valueOf(numStreamsOpt)
    producerChannel = new ProducerDataChannel()
    connector = new ZookeeperConsumerConnector(new ConsumerConfig(Utils.loadProps(consumerConfig)))
    var streams: Seq[KafkaStream[Array[Byte], Array[Byte]]] = null
    try {
      streams = connector.createMessageStreamsByFilter(filterSpec, numStreams.intValue())
      debug("%d consumer streams created".format(streams.size))
    } catch {
      case t: Throwable =>
        fatal("Unable to create stream - shutting down mirror maker.")
        connector.shutdown()
        System.exit(1)
    }
    val streamIndex = new AtomicInteger()
    streams.foreach(stream => producerChannel.addProducer(new KafkaProducer(producerProps)))
    mirroringThreads = streams.map(stream => new MirrorMakerThread(stream, streamIndex.getAndIncrement))
    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        cleanShutdown()
      }
    })
    // start the mirroring threads
    mirroringThreads.foreach(_.start)
    // in case the consumer threads hit a timeout/other exception
    mirroringThreads.foreach(_.awaitShutdown)
    cleanShutdown()
  }

  def cleanShutdown() {
    if (connector != null) connector.shutdown()
    if (mirroringThreads != null) mirroringThreads.foreach(_.awaitShutdown)
    if (producerChannel != null) producerChannel.close()
    info("Kafka mirror maker shutdown successfully")
  }

  class MirrorMakerThread(stream: KafkaStream[Array[Byte], Array[Byte]],
                          threadId: Int)
          extends Thread with Logging {

    private val shutdownLatch = new CountDownLatch(1)
    private val threadName = "mirrormaker-" + threadId
    this.logIdent = "[%s] ".format(threadName)

    this.setName(threadName)

    override def run() {
      info("Starting mirror maker thread " + threadName)
      try {
        for (msgAndMetadata <- stream) {
          producerChannel.send(new ProducerRecord(msgAndMetadata.topic, msgAndMetadata.key(), msgAndMetadata.message()))
        }
      } catch {
        case e: Throwable =>
          fatal("Stream unexpectedly exited.", e)
      } finally {
        shutdownLatch.countDown()
        info("Stopped thread.")
      }
    }

    def awaitShutdown() {
      try {
        shutdownLatch.await()
      } catch {
        case e: InterruptedException => fatal("Shutdown of thread %s interrupted. This might leak data!".format(threadName))
      }
    }
  }

  class ProducerDataChannel extends Logging {
    val producers = new ListBuffer[KafkaProducer]
    var producerIndex = new AtomicInteger(0)

    def addProducer(producer: KafkaProducer) {
      producers += producer
    }

    def send(producerRecord: ProducerRecord) {
      if(producerRecord.key() != null) {
        val producerId = Utils.abs(java.util.Arrays.hashCode(producerRecord.key())) % producers.size
        trace("Send message with key %s to producer %d.".format(java.util.Arrays.toString(producerRecord.key()), producerId))
        val producer = producers(producerId)
        producer.send(producerRecord)
      } else {
        val producerId = producerIndex.getAndSet((producerIndex.get() + 1) % producers.size)
        producers(producerId).send(producerRecord)
        trace("Sent message to producer " + producerId)
      }
    }

    def close() {
      producers.foreach(_.close())
    }
  }
}

