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

import kafka.consumer._
import kafka.metrics.KafkaMetricsGroup
import kafka.producer.{BaseProducer, NewShinyProducer, OldProducer}
import kafka.serializer._
import kafka.utils._
import org.apache.kafka.clients.producer.{ProducerConfig, ProducerRecord}

import java.util.Random
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{BlockingQueue, ArrayBlockingQueue, CountDownLatch, TimeUnit}

import scala.collection.JavaConversions._

import joptsimple.OptionParser

object MirrorMaker extends Logging {

  private var connectors: Seq[ZookeeperConsumerConnector] = null
  private var consumerThreads: Seq[ConsumerThread] = null
  private var producerThreads: Seq[ProducerThread] = null
  private val isShuttingdown: AtomicBoolean = new AtomicBoolean(false)

  private val shutdownMessage : ProducerRecord[Array[Byte],Array[Byte]] = new ProducerRecord[Array[Byte],Array[Byte]]("shutdown", "shutdown".getBytes)

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
    
    if(args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Continuously copy data between two Kafka clusters.")

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

    // create consumer streams
    connectors = options.valuesOf(consumerConfigOpt).toList
      .map(cfg => new ConsumerConfig(Utils.loadProps(cfg)))
      .map(new ZookeeperConsumerConnector(_))
    val numConsumers = connectors.size * numStreams

    // create a data channel btw the consumers and the producers
    val mirrorDataChannel = new DataChannel(bufferSize, numConsumers, numProducers)

    // create producer threads
    val useNewProducer = options.has(useNewProducerOpt)
    val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))
    val clientId = producerProps.getProperty("client.id", "")
    producerThreads = (0 until numProducers).map(i => {
      producerProps.setProperty("client.id", clientId + "-" + i)
      val producer =
      if (useNewProducer) {
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
        new NewShinyProducer(producerProps)
      }
      else
        new OldProducer(producerProps)
      new ProducerThread(mirrorDataChannel, producer, i)
    })

    // create consumer threads
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
    consumerThreads = streams.zipWithIndex.map(streamAndIndex => new ConsumerThread(streamAndIndex._1, mirrorDataChannel, streamAndIndex._2))
    assert(consumerThreads.size == numConsumers)

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
    if (isShuttingdown.compareAndSet(false, true)) {
      if (connectors != null) connectors.foreach(_.shutdown)
      if (consumerThreads != null) consumerThreads.foreach(_.awaitShutdown)
      if (producerThreads != null) {
        producerThreads.foreach(_.shutdown)
        producerThreads.foreach(_.awaitShutdown)
      }
      info("Kafka mirror maker shutdown successfully")
    }
  }

  class DataChannel(capacity: Int, numInputs: Int, numOutputs: Int) extends KafkaMetricsGroup {

    val queues = new Array[BlockingQueue[ProducerRecord[Array[Byte],Array[Byte]]]](numOutputs)
    for (i <- 0 until numOutputs)
      queues(i) = new ArrayBlockingQueue[ProducerRecord[Array[Byte],Array[Byte]]](capacity)

    private val counter = new AtomicInteger(new Random().nextInt())

    // We use a single meter for aggregated wait percentage for the data channel.
    // Since meter is calculated as total_recorded_value / time_window and
    // time_window is independent of the number of threads, each recorded wait
    // time should be discounted by # threads.
    private val waitPut = newMeter("MirrorMaker-DataChannel-WaitOnPut", "percent", TimeUnit.NANOSECONDS)
    private val waitTake = newMeter("MirrorMaker-DataChannel-WaitOnTake", "percent", TimeUnit.NANOSECONDS)
    private val channelSizeHist = newHistogram("MirrorMaker-DataChannel-Size")

    def put(record: ProducerRecord[Array[Byte],Array[Byte]]) {
      // If the key of the message is empty, use round-robin to select the queue
      // Otherwise use the queue based on the key value so that same key-ed messages go to the same queue
      val queueId =
        if(record.key() != null) {
          Utils.abs(java.util.Arrays.hashCode(record.key())) % numOutputs
        } else {
          Utils.abs(counter.getAndIncrement()) % numOutputs
        }
      put(record, queueId)
    }

    def put(record: ProducerRecord[Array[Byte],Array[Byte]], queueId: Int) {
      val queue = queues(queueId)

      var putSucceed = false
      while (!putSucceed) {
        val startPutTime = SystemTime.nanoseconds
        putSucceed = queue.offer(record, 500, TimeUnit.MILLISECONDS)
        waitPut.mark((SystemTime.nanoseconds - startPutTime) / numInputs)
      }
      channelSizeHist.update(queue.size)
    }

    def take(queueId: Int): ProducerRecord[Array[Byte],Array[Byte]] = {
      val queue = queues(queueId)
      var data: ProducerRecord[Array[Byte],Array[Byte]] = null
      while (data == null) {
        val startTakeTime = SystemTime.nanoseconds
        data = queue.poll(500, TimeUnit.MILLISECONDS)
        waitTake.mark((SystemTime.nanoseconds - startTakeTime) / numOutputs)
      }
      channelSizeHist.update(queue.size)
      data
    }
  }

  class ConsumerThread(stream: KafkaStream[Array[Byte], Array[Byte]],
                       mirrorDataChannel: DataChannel,
                       threadId: Int)
          extends Thread with Logging with KafkaMetricsGroup {

    private val shutdownLatch = new CountDownLatch(1)
    private val threadName = "mirrormaker-consumer-" + threadId
    private var isCleanShutdown: Boolean = true
    this.logIdent = "[%s] ".format(threadName)

    this.setName(threadName)

    override def run() {
      info("Starting mirror maker consumer thread " + threadName)
      try {
        for (msgAndMetadata <- stream) {
          val data = new ProducerRecord[Array[Byte],Array[Byte]](msgAndMetadata.topic, msgAndMetadata.key, msgAndMetadata.message)
          mirrorDataChannel.put(data)
        }
      } catch {
        case e: Throwable => {
          fatal("Stream unexpectedly exited.", e)
          isCleanShutdown = false
        }
      } finally {
        shutdownLatch.countDown()
        info("Consumer thread stopped")
        // If it exits accidentally, stop the entire mirror maker.
        if (!isCleanShutdown) {
          fatal("Consumer thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
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
    private var isCleanShutdown: Boolean = true
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    override def run {
      info("Starting mirror maker producer thread " + threadName)
      try {
        while (true) {
          val data: ProducerRecord[Array[Byte],Array[Byte]] = dataChannel.take(threadId)
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
          isCleanShutdown = false
        }
      } finally {
        shutdownComplete.countDown
        info("Producer thread stopped")
        // If it exits accidentally, stop the entire mirror maker.
        if (!isCleanShutdown) {
          fatal("Producer thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
      }
    }

    def shutdown {
      try {
        info("Producer thread " + threadName + " shutting down")
        dataChannel.put(shutdownMessage, threadId)
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