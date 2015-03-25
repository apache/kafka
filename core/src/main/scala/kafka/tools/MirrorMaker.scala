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

import java.util
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Collections, Properties}

import com.yammer.metrics.core.Gauge
import joptsimple.OptionParser
import kafka.consumer.{KafkaStream, Blacklist, ConsumerConfig, ConsumerThreadId, ConsumerTimeoutException, TopicFilter, Whitelist, ZookeeperConsumerConnector}
import kafka.javaapi.consumer.ConsumerRebalanceListener
import kafka.message.MessageAndMetadata
import kafka.metrics.KafkaMetricsGroup
import kafka.serializer.DefaultDecoder
import kafka.utils.{CommandLineUtils, Logging, Utils}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}

import scala.collection.JavaConversions._

/**
 * The mirror maker has the following architecture:
 * - There are N mirror maker thread shares one ZookeeperConsumerConnector and each owns a Kafka stream.
 * - All the mirror maker threads share one producer.
 * - Each mirror maker thread periodically flushes the producer and then commits all offsets.
 *
 * @note For mirror maker, the following settings are set by default to make sure there is no data loss:
 *       1. use new producer with following settings
 *            acks=all
 *            retries=max integer
 *            block.on.buffer.full=true
 *            max.in.flight.requests.per.connection=1
 *       2. Consumer Settings
 *            auto.commit.enable=false
 *       3. Mirror Maker Setting:
 *            abort.on.send.failure=true
 */
object MirrorMaker extends Logging with KafkaMetricsGroup {

  private var connectors: Seq[ZookeeperConsumerConnector] = null
  private var producer: MirrorMakerProducer = null
  private var mirrorMakerThreads: Seq[MirrorMakerThread] = null
  private val isShuttingdown: AtomicBoolean = new AtomicBoolean(false)
  // Track the messages not successfully sent by mirror maker.
  private var numDroppedMessages: AtomicInteger = new AtomicInteger(0)
  private var messageHandler: MirrorMakerMessageHandler = null
  private var offsetCommitIntervalMs = 0
  private var abortOnSendFailure: Boolean = true
  @volatile private var exitingOnSendFailure: Boolean = false

  // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
  // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
  // message was not really acked, but was skipped. This metric records the number of skipped offsets.
  newGauge("MirrorMaker-numDroppedMessages",
    new Gauge[Int] {
      def value = numDroppedMessages.get()
    })

  def main(args: Array[String]) {

    info("Starting mirror maker")
    val parser = new OptionParser

    val consumerConfigOpt = parser.accepts("consumer.config",
      "Embedded consumer config for consuming from the source cluster.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val producerConfigOpt = parser.accepts("producer.config",
      "Embedded producer config.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val numStreamsOpt = parser.accepts("num.streams",
      "Number of consumption streams.")
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

    val offsetCommitIntervalMsOpt = parser.accepts("offset.commit.interval.ms",
      "Offset commit interval in ms")
      .withRequiredArg()
      .describedAs("offset commit interval in millisecond")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(60000)

    val consumerRebalanceListenerOpt = parser.accepts("consumer.rebalance.listener",
      "The consumer rebalance listener to use for mirror maker consumer.")
      .withRequiredArg()
      .describedAs("A custom rebalance listener of type ConsumerRebalanceListener")
      .ofType(classOf[String])

    val rebalanceListenerArgsOpt = parser.accepts("rebalance.listener.args",
      "Arguments used by custom rebalance listener for mirror maker consumer")
      .withRequiredArg()
      .describedAs("Arguments passed to custom rebalance listener constructor as a string.")
      .ofType(classOf[String])

    val messageHandlerOpt = parser.accepts("message.handler",
      "The consumer rebalance listener to use for mirror maker consumer.")
      .withRequiredArg()
      .describedAs("A custom rebalance listener of type MirrorMakerMessageHandler")
      .ofType(classOf[String])

    val messageHandlerArgsOpt = parser.accepts("message.handler.args",
      "Arguments used by custom rebalance listener for mirror maker consumer")
      .withRequiredArg()
      .describedAs("Arguments passed to message handler constructor.")
      .ofType(classOf[String])

    val abortOnSendFailureOpt = parser.accepts("abort.on.send.failure",
      "Configure the mirror maker to exit on a failed send.")
      .withRequiredArg()
      .describedAs("Stop the entire mirror maker when a send failure occurs")
      .ofType(classOf[String])
      .defaultsTo("true")

    val helpOpt = parser.accepts("help", "Print this message.")

    if (args.length == 0)
      CommandLineUtils.printUsageAndDie(parser, "Continuously copy data between two Kafka clusters.")


    val options = parser.parse(args: _*)

    if (options.has(helpOpt)) {
      parser.printHelpOn(System.out)
      System.exit(0)
    }

    CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt)
    if (List(whitelistOpt, blacklistOpt).count(options.has) != 1) {
      println("Exactly one of whitelist or blacklist is required.")
      System.exit(1)
    }

    abortOnSendFailure = options.valueOf(abortOnSendFailureOpt).toBoolean
    offsetCommitIntervalMs = options.valueOf(offsetCommitIntervalMsOpt).intValue()
    val numStreams = options.valueOf(numStreamsOpt).intValue()

    Runtime.getRuntime.addShutdownHook(new Thread("MirrorMakerShutdownHook") {
      override def run() {
        cleanShutdown()
      }
    })
    
    // create producer
    val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))
    // Defaults to no data loss settings.
    maybeSetDefaultProperty(producerProps, ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
    maybeSetDefaultProperty(producerProps, ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, "true")
    maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all")
    maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    producer = new MirrorMakerProducer(producerProps)

    // Create consumer connector
    val consumerConfigProps = Utils.loadProps(options.valueOf(consumerConfigOpt))
    // Disable consumer auto offsets commit to prevent data loss.
    maybeSetDefaultProperty(consumerConfigProps, "auto.commit.enable", "false")
    // Set the consumer timeout so we will not block for low volume pipeline. The timeout is necessary to make sure
    // Offsets are still committed for those low volume pipelines.
    maybeSetDefaultProperty(consumerConfigProps, "consumer.timeout.ms", "10000")
    // The default client id is group id, we manually set client id to groupId-index to avoid metric collision
    val groupIdString = consumerConfigProps.getProperty("group.id")
    connectors = (0 until numStreams) map { i =>
      consumerConfigProps.setProperty("client.id", groupIdString + "-" + i.toString)
      val consumerConfig = new ConsumerConfig(consumerConfigProps)
      new ZookeeperConsumerConnector(consumerConfig)
    }

    // Set consumer rebalance listener.
    // Custom rebalance listener will be invoked after internal listener finishes its work.
    val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
    val rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt)
    val customRebalanceListener = {
      if (customRebalanceListenerClass != null) {
        if (rebalanceListenerArgs != null)
          Some(Utils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass, rebalanceListenerArgs))
        else
          Some(Utils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass))
      } else {
        None
      }
    }
    connectors.foreach {
      connector =>
        val consumerRebalanceListener = new InternalRebalanceListener(connector, customRebalanceListener)
        connector.setConsumerRebalanceListener(consumerRebalanceListener)
    }

    // create Kafka streams
    val filterSpec = if (options.has(whitelistOpt))
      new Whitelist(options.valueOf(whitelistOpt))
    else
      new Blacklist(options.valueOf(blacklistOpt))

    // Create mirror maker threads
    mirrorMakerThreads = (0 until numStreams) map ( i =>
        new MirrorMakerThread(connectors(i), filterSpec, i)
    )

    // Create and initialize message handler
    val customMessageHandlerClass = options.valueOf(messageHandlerOpt)
    val messageHandlerArgs = options.valueOf(messageHandlerArgsOpt)
    messageHandler = {
      if (customMessageHandlerClass != null) {
        if (messageHandlerArgs != null)
          Utils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass, messageHandlerArgs)
        else
          Utils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass)
      } else {
        defaultMirrorMakerMessageHandler
      }
    }

    mirrorMakerThreads.foreach(_.start())
    mirrorMakerThreads.foreach(_.awaitShutdown())
  }

  def commitOffsets(connector: ZookeeperConsumerConnector) {
    if (!exitingOnSendFailure) {
      trace("Committing offsets.")
      connector.commitOffsets
    } else {
      info("Exiting on send failure, skip committing offsets.")
    }
  }

  def cleanShutdown() {
    if (isShuttingdown.compareAndSet(false, true)) {
      info("Start clean shutdown.")
      // Shutdown consumer threads.
      info("Shutting down consumer threads.")
      if (mirrorMakerThreads != null) {
        mirrorMakerThreads.foreach(_.shutdown())
        mirrorMakerThreads.foreach(_.awaitShutdown())
      }
      info("Closing producer.")
      producer.close()
      connectors.foreach(commitOffsets)
      // Connector should only be shutdown after offsets are committed.
      info("Shutting down consumer connectors.")
      connectors.foreach(_.shutdown())
      info("Kafka mirror maker shutdown successfully")
    }
  }

  private def maybeSetDefaultProperty(properties: Properties, propertyName: String, defaultValue: String) {
    val propertyValue = properties.getProperty(propertyName)
    properties.setProperty(propertyName, Option(propertyValue).getOrElse(defaultValue))
    if (properties.getProperty(propertyName) != defaultValue)
      info("Property %s is overridden to %s - data loss or message reordering is possible.".format(propertyName, propertyValue))
  }

  class MirrorMakerThread(connector: ZookeeperConsumerConnector,
                          filterSpec: TopicFilter,
                          val threadId: Int) extends Thread with Logging with KafkaMetricsGroup {
    private val threadName = "mirrormaker-thread-" + threadId
    private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
    private var lastOffsetCommitMs = System.currentTimeMillis()
    @volatile private var shuttingDown: Boolean = false
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    override def run() {
      info("Starting mirror maker thread " + threadName)
      try {
        // Creating one stream per each connector instance
        val streams = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())
        require(streams.size == 1)
        val stream = streams(0)
        val iter = stream.iterator()

        // TODO: Need to be changed after KAFKA-1660 is available.
        while (!exitingOnSendFailure && !shuttingDown) {
          try {
            while (!exitingOnSendFailure && !shuttingDown && iter.hasNext()) {
              val data = iter.next()
              trace("Sending message with value size %d".format(data.message().size))
              val records = messageHandler.handle(data)
              records.foreach(producer.send)
              maybeFlushAndCommitOffsets()
            }
          } catch {
            case e: ConsumerTimeoutException =>
              trace("Caught ConsumerTimeoutException, continue iteration.")
          }
          maybeFlushAndCommitOffsets()
        }
      } catch {
        case t: Throwable =>
          fatal("Mirror maker thread failure due to ", t)
      } finally {
        shutdownLatch.countDown()
        info("Mirror maker thread stopped")
        // if it exits accidentally, stop the entire mirror maker
        if (!isShuttingdown.get()) {
          fatal("Mirror maker thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
      }
    }

    def maybeFlushAndCommitOffsets() {
      if (System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
        producer.flush()
        commitOffsets(connector)
        lastOffsetCommitMs = System.currentTimeMillis()
      }
    }

    def shutdown() {
      try {
        info(threadName + " shutting down")
        shuttingDown = true
      }
      catch {
        case ie: InterruptedException =>
          warn("Interrupt during shutdown of the mirror maker thread")
      }
    }

    def awaitShutdown() {
      try {
        shutdownLatch.await()
        info("Mirror maker thread shutdown complete")
      } catch {
        case ie: InterruptedException =>
          warn("Shutdown of the mirror maker thread interrupted")
      }
    }
  }

  private class MirrorMakerProducer(val producerProps: Properties) {

    val sync = producerProps.getProperty("producer.type", "async").equals("sync")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def send(record: ProducerRecord[Array[Byte], Array[Byte]]) {
      if (sync) {
        this.producer.send(record).get()
      } else {
          this.producer.send(record,
            new MirrorMakerProducerCallback(record.topic(), record.key(), record.value()))
      }
    }

    def flush() {
      this.producer.flush()
    }

    def close() {
      this.producer.close()
    }
  }

  private class MirrorMakerProducerCallback (topic: String, key: Array[Byte], value: Array[Byte])
    extends ErrorLoggingCallback(topic, key, value, false) {

    override def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null) {
        // Use default call back to log error. This means the max retries of producer has reached and message
        // still could not be sent.
        super.onCompletion(metadata, exception)
        // If abort.on.send.failure is set, stop the mirror maker. Otherwise log skipped message and move on.
        if (abortOnSendFailure)
          exitingOnSendFailure = true
        numDroppedMessages.incrementAndGet()
      }
    }
  }

  private class InternalRebalanceListener(connector: ZookeeperConsumerConnector,
                                          customRebalanceListener: Option[ConsumerRebalanceListener])
    extends ConsumerRebalanceListener {

    override def beforeReleasingPartitions(partitionOwnership: java.util.Map[String, java.util.Set[java.lang.Integer]]) {
      producer.flush()
      commitOffsets(connector)
      // invoke custom consumer rebalance listener
      if (customRebalanceListener.isDefined)
        customRebalanceListener.get.beforeReleasingPartitions(partitionOwnership)
    }

    override def beforeStartingFetchers(consumerId: String,
                                        partitionAssignment: java.util.Map[String, java.util.Map[java.lang.Integer, ConsumerThreadId]]) {
      if (customRebalanceListener.isDefined)
        customRebalanceListener.get.beforeStartingFetchers(consumerId, partitionAssignment)
    }
  }

  /**
   * If message.handler.args is specified. A constructor that takes in a String as argument must exist.
   */
  trait MirrorMakerMessageHandler {
    def handle(record: MessageAndMetadata[Array[Byte], Array[Byte]]): util.List[ProducerRecord[Array[Byte], Array[Byte]]]
  }

  private object defaultMirrorMakerMessageHandler extends MirrorMakerMessageHandler {
    override def handle(record: MessageAndMetadata[Array[Byte], Array[Byte]]): util.List[ProducerRecord[Array[Byte], Array[Byte]]] = {
      Collections.singletonList(new ProducerRecord[Array[Byte], Array[Byte]](record.topic, record.key(), record.message()))
    }
  }

}
