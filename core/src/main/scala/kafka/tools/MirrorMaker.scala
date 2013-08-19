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

package kafka.tools

import joptsimple.OptionParser
import kafka.utils.{Utils, CommandLineUtils, Logging}
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import scala.collection.JavaConversions._
import java.util.concurrent.CountDownLatch
import java.nio.ByteBuffer
import kafka.consumer._
import kafka.serializer._
import collection.mutable.ListBuffer
import kafka.tools.KafkaMigrationTool.{ProducerThread, ProducerDataChannel}
import kafka.javaapi

object MirrorMaker extends Logging {

  private var connectors: Seq[ZookeeperConsumerConnector] = null
  private var consumerThreads: Seq[MirrorMakerThread] = null
  private var producerThreads: ListBuffer[ProducerThread] = null

  def main(args: Array[String]) {
    
    info ("Starting mirror maker")
    val parser = new OptionParser

    val consumerConfigOpt = parser.accepts("consumer.config",
      "Consumer config to consume from a source cluster. " +
      "You may specify multiple of these.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val producerConfigOpt = parser.accepts("producer.config",
      "Embedded producer config.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val numProducersOpt = parser.accepts("num.producers",
      "Number of producer instances")
      .withRequiredArg()
      .describedAs("Number of producers")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)
    
    val numStreamsOpt = parser.accepts("num.streams",
      "Number of consumption streams.")
      .withRequiredArg()
      .describedAs("Number of threads")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(1)

    val bufferSizeOpt =  parser.accepts("queue.size", "Number of messages that are buffered between the consumer and producer")
      .withRequiredArg()
      .describedAs("Queue size in terms of number of messages")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10000);

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
      println("Exactly one of whitelist or blacklist is required.")
      System.exit(1)
    }

    val numStreams = options.valueOf(numStreamsOpt)
    val bufferSize = options.valueOf(bufferSizeOpt).intValue()

    val producers = (1 to options.valueOf(numProducersOpt).intValue()).map(_ => {
      val props = Utils.loadProps(options.valueOf(producerConfigOpt))
      val config = props.getProperty("partitioner.class") match {
        case null =>
          new ProducerConfig(props) {
            override val partitionerClass = "kafka.producer.ByteArrayPartitioner"
          }
        case pClass : String =>
          new ProducerConfig(props)
      }
      new Producer[Array[Byte], Array[Byte]](config)
    })

    connectors = options.valuesOf(consumerConfigOpt).toList
            .map(cfg => new ConsumerConfig(Utils.loadProps(cfg.toString)))
            .map(new ZookeeperConsumerConnector(_))

    val filterSpec = if (options.has(whitelistOpt))
      new Whitelist(options.valueOf(whitelistOpt))
    else
      new Blacklist(options.valueOf(blacklistOpt))

    var streams: Seq[KafkaStream[Array[Byte], Array[Byte]]] = Nil
    try {
      streams = connectors.map(_.createMessageStreamsByFilter(filterSpec, numStreams.intValue(), new DefaultDecoder(), new DefaultDecoder())).flatten
    } catch {
      case t =>
        fatal("Unable to create stream - shutting down mirror maker.")
        connectors.foreach(_.shutdown)
    }

    val producerDataChannel = new ProducerDataChannel[KeyedMessage[Array[Byte], Array[Byte]]](bufferSize);

    consumerThreads = streams.zipWithIndex.map(streamAndIndex => new MirrorMakerThread(streamAndIndex._1, producerDataChannel, producers, streamAndIndex._2))

    producerThreads = new ListBuffer[ProducerThread]()

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        cleanShutdown()
      }
    })

    // create producer threads
    var i: Int = 1
    for(producer <- producers) {
      val producerThread: KafkaMigrationTool.ProducerThread = new KafkaMigrationTool.ProducerThread(producerDataChannel,
        new javaapi.producer.Producer[Array[Byte], Array[Byte]](producer), i)
      producerThreads += producerThread
      i += 1
    }

    consumerThreads.foreach(_.start)
    producerThreads.foreach(_.start)

    // in case the consumer threads hit a timeout/other exception
    consumerThreads.foreach(_.awaitShutdown)
    cleanShutdown()
  }

  def cleanShutdown() {
    if (connectors != null) connectors.foreach(_.shutdown)
    if (consumerThreads != null) consumerThreads.foreach(_.awaitShutdown)
    if (producerThreads != null) {
      producerThreads.foreach(_.shutdown)
      producerThreads.foreach(_.awaitShutdown)
    }
    info("Kafka mirror maker shutdown successfully")
  }

  class MirrorMakerThread(stream: KafkaStream[Array[Byte], Array[Byte]],
                          producerDataChannel: ProducerDataChannel[KeyedMessage[Array[Byte], Array[Byte]]],
                          producers: Seq[Producer[Array[Byte], Array[Byte]]],
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
          // If the key of the message is empty, put it into the universal channel
          // Otherwise use a pre-assigned producer to send the message
          if (msgAndMetadata.key == null) {
            trace("Send the non-keyed message the producer channel.")
            val pd = new KeyedMessage[Array[Byte], Array[Byte]](msgAndMetadata.topic, msgAndMetadata.message)
            producerDataChannel.sendRequest(pd)
          } else {
            val producerId = Utils.abs(java.util.Arrays.hashCode(msgAndMetadata.key)) % producers.size()
            trace("Send message with key %s to producer %d.".format(java.util.Arrays.toString(msgAndMetadata.key), producerId))
            val producer = producers(producerId)
            val pd = new KeyedMessage[Array[Byte], Array[Byte]](msgAndMetadata.topic, msgAndMetadata.key, msgAndMetadata.message)
            producer.send(pd)
          }
        }
      } catch {
        case e =>
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
}

