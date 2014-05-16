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

import kafka.utils.{SystemTime, Utils, CommandLineUtils, Logging}
import kafka.consumer._
import kafka.serializer._
import kafka.producer.{OldProducer, NewShinyProducer, BaseProducer}
import org.apache.kafka.clients.producer.ProducerRecord

import scala.collection.mutable.ListBuffer
import scala.collection.JavaConversions._

import java.util.concurrent.{TimeUnit, BlockingQueue, ArrayBlockingQueue, CountDownLatch}

import joptsimple.OptionParser
import kafka.metrics.KafkaMetricsGroup
import com.yammer.metrics.core.Gauge

object MirrorMaker extends Logging {

  private var connectors: Seq[ZookeeperConsumerConnector] = null
  private var consumerThreads: Seq[ConsumerThread] = null
  private var producerThreads: ListBuffer[ProducerThread] = null

  private val shutdownMessage : ProducerRecord = new ProducerRecord("shutdown", "shutdown".getBytes)

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

    val useNewProducerOpt = parser.accepts("new.producer",
      "Use the new producer implementation.")

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

    val bufferSizeOpt =  parser.accepts("queue.size",
      "Number of messages that are buffered between the consumer and producer")
      .withRequiredArg()
      .describedAs("Queue size in terms of number of messages")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(10000)

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

    val numProducers = options.valueOf(numProducersOpt).intValue()
    val numStreams = options.valueOf(numStreamsOpt).intValue()
    val bufferSize = options.valueOf(bufferSizeOpt).intValue()

    val useNewProducer = options.has(useNewProducerOpt)
    val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))

    // create data channel
    val mirrorDataChannel = new DataChannel(bufferSize)

    // create producer threads
    val producers = (1 to numProducers).map(_ => {
        if (useNewProducer)
          new NewShinyProducer(producerProps)
        else
          new OldProducer(producerProps)
      })

    producerThreads = new ListBuffer[ProducerThread]()
    var producerIndex: Int = 1
    for(producer <- producers) {
      val producerThread = new ProducerThread(mirrorDataChannel, producer, producerIndex)
      producerThreads += producerThread
      producerIndex += 1
    }

    // create consumer streams
    connectors = options.valuesOf(consumerConfigOpt).toList
            .map(cfg => new ConsumerConfig(Utils.loadProps(cfg)))
            .map(new ZookeeperConsumerConnector(_))

    val filterSpec = if (options.has(whitelistOpt))
      new Whitelist(options.valueOf(whitelistOpt))
    else
      new Blacklist(options.valueOf(blacklistOpt))

    var streams: Seq[KafkaStream[Array[Byte], Array[Byte]]] = Nil
    try {
      streams = connectors.map(_.createMessageStreamsByFilter(filterSpec, numStreams, new DefaultDecoder(), new DefaultDecoder())).flatten
    } catch {
      case t: Throwable =>
        fatal("Unable to create stream - shutting down mirror maker.")
        connectors.foreach(_.shutdown)
    }
    consumerThreads = streams.zipWithIndex.map(streamAndIndex => new ConsumerThread(streamAndIndex._1, mirrorDataChannel, producers, streamAndIndex._2))

    Runtime.getRuntime.addShutdownHook(new Thread() {
      override def run() {
        cleanShutdown()
      }
    })

    consumerThreads.foreach(_.start)
    producerThreads.foreach(_.start)

    // we wait on producer's shutdown latch instead of consumers
    // since the consumer threads can hit a timeout/other exception;
    // but in this case the producer should still be able to shutdown
    // based on the shutdown message in the channel
    producerThreads.foreach(_.awaitShutdown)
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

  class DataChannel(capacity: Int) extends KafkaMetricsGroup {

    val queue = new ArrayBlockingQueue[ProducerRecord](capacity)

    newGauge(
      "MirrorMaker-DataChannel-Size",
      new Gauge[Int] {
        def value = queue.size
      }
    )

    private val waitPut = newMeter("MirrorMaker-DataChannel-WaitOnPut", "percent", TimeUnit.MILLISECONDS)
    private val waitTake = newMeter("MirrorMaker-DataChannel-WaitOnTake", "percent", TimeUnit.MILLISECONDS)


    def put(record: ProducerRecord) {
      var putSucceed = false
      while (!putSucceed) {
        val startPutTime = SystemTime.milliseconds
        putSucceed = queue.offer(record, 500, TimeUnit.MILLISECONDS)
        waitPut.mark(SystemTime.milliseconds - startPutTime)
      }
    }

    def take(): ProducerRecord = {
      var data: ProducerRecord = null
      while (data == null) {
        val startTakeTime = SystemTime.milliseconds
        data = queue.poll(500, TimeUnit.MILLISECONDS)
        waitTake.mark(SystemTime.milliseconds - startTakeTime)
      }
      data
    }
  }

  class ConsumerThread(stream: KafkaStream[Array[Byte], Array[Byte]],
                       mirrorDataChannel: DataChannel,
                       producers: Seq[BaseProducer],
                       threadId: Int)
          extends Thread with Logging with KafkaMetricsGroup {

    private val shutdownLatch = new CountDownLatch(1)
    private val threadName = "mirrormaker-consumer-" + threadId
    this.logIdent = "[%s] ".format(threadName)

    this.setName(threadName)

    override def run() {
      info("Starting mirror maker consumer thread " + threadName)
      try {
        for (msgAndMetadata <- stream) {
          // If the key of the message is empty, put it into the universal channel
          // Otherwise use a pre-assigned producer to send the message
          if (msgAndMetadata.key == null) {
            trace("Send the non-keyed message the producer channel.")
            val data = new ProducerRecord(msgAndMetadata.topic, msgAndMetadata.message)
            mirrorDataChannel.put(data)
          } else {
            val producerId = Utils.abs(java.util.Arrays.hashCode(msgAndMetadata.key)) % producers.size()
            trace("Send message with key %s to producer %d.".format(java.util.Arrays.toString(msgAndMetadata.key), producerId))
            val producer = producers(producerId)
            producer.send(msgAndMetadata.topic, msgAndMetadata.key, msgAndMetadata.message)
          }
        }
      } catch {
        case e: Throwable =>
          fatal("Stream unexpectedly exited.", e)
      } finally {
        shutdownLatch.countDown()
        info("Consumer thread stopped")
      }
    }

    def awaitShutdown() {
      try {
        shutdownLatch.await()
        info("Consumer thread shutdown complete")
      } catch {
        case e: InterruptedException => fatal("Shutdown of the consumer thread interrupted. This might leak data!")
      }
    }
  }

  class ProducerThread (val dataChannel: DataChannel,
                        val producer: BaseProducer,
                        val threadId: Int) extends Thread with Logging with KafkaMetricsGroup {
    private val threadName = "mirrormaker-producer-" + threadId
    private val shutdownComplete: CountDownLatch = new CountDownLatch(1)
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    override def run {
      info("Starting mirror maker producer thread " + threadName)
      try {
        while (true) {
          val data: ProducerRecord = dataChannel.take
          trace("Sending message with value size %d".format(data.value().size))
          if(data eq shutdownMessage) {
            info("Received shutdown message")
            return
          }
          producer.send(data.topic(), data.key(), data.value())
        }
      } catch {
        case t: Throwable => {
          fatal("Producer thread failure due to ", t)
        }
      } finally {
        shutdownComplete.countDown
        info("Producer thread stopped")
      }
    }

    def shutdown {
      try {
        info("Producer thread " + threadName + " shutting down")
        dataChannel.put(shutdownMessage)
      }
      catch {
        case ie: InterruptedException => {
          warn("Interrupt during shutdown of ProducerThread")
        }
      }
    }

    def awaitShutdown {
      try {
        shutdownComplete.await
        producer.close
        info("Producer thread shutdown complete")
      } catch {
        case ie: InterruptedException => {
          warn("Shutdown of the producer thread interrupted")
        }
      }
    }
  }
}

