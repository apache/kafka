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

import java.time.Duration
import java.util
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.regex.Pattern
import java.util.{Collections, Properties}

import com.yammer.metrics.core.Gauge
import kafka.consumer.BaseConsumerRecord
import kafka.metrics.KafkaMetricsGroup
import kafka.utils._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.errors.{TimeoutException, WakeupException}
import org.apache.kafka.common.record.RecordBatch
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.control.ControlThrowable
import scala.util.{Failure, Success, Try}

/**
 * The mirror maker has the following architecture:
 * - There are N mirror maker thread, each of which is equipped with a separate KafkaConsumer instance.
 * - All the mirror maker threads share one producer.
 * - Each mirror maker thread periodically flushes the producer and then commits all offsets.
 *
 * @note For mirror maker, the following settings are set by default to make sure there is no data loss:
 *       1. use producer with following settings
 *            acks=all
 *            delivery.timeout.ms=max integer
 *            max.block.ms=max long
 *            max.in.flight.requests.per.connection=1
 *       2. Consumer Settings
 *            enable.auto.commit=false
 *       3. Mirror Maker Setting:
 *            abort.on.send.failure=true
 */
object MirrorMaker extends Logging with KafkaMetricsGroup {

  private[tools] var producer: MirrorMakerProducer = null
  private var mirrorMakerThreads: Seq[MirrorMakerThread] = null
  private val isShuttingDown: AtomicBoolean = new AtomicBoolean(false)
  // Track the messages not successfully sent by mirror maker.
  private val numDroppedMessages: AtomicInteger = new AtomicInteger(0)
  private var messageHandler: MirrorMakerMessageHandler = null
  private var offsetCommitIntervalMs = 0
  private var abortOnSendFailure: Boolean = true
  @volatile private var exitingOnSendFailure: Boolean = false
  private var lastSuccessfulCommitTime = -1L
  private val time = Time.SYSTEM

  // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
  // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
  // message was not really acked, but was skipped. This metric records the number of skipped offsets.
  newGauge("MirrorMaker-numDroppedMessages",
    new Gauge[Int] {
      def value = numDroppedMessages.get()
    })

  def main(args: Array[String]) {

    info("Starting mirror maker")
    try {
      val opts = new MirrorMakerOptions(args)
      CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool helps to continuously copy data between two Kafka clusters.")
      opts.checkArgs()
    } catch {
      case ct: ControlThrowable => throw ct
      case t: Throwable =>
        error("Exception when starting mirror maker.", t)
    }

    mirrorMakerThreads.foreach(_.start())
    mirrorMakerThreads.foreach(_.awaitShutdown())
  }

  def createConsumers(numStreams: Int,
                      consumerConfigProps: Properties,
                      customRebalanceListener: Option[ConsumerRebalanceListener],
                      whitelist: Option[String]): Seq[ConsumerWrapper] = {
    // Disable consumer auto offsets commit to prevent data loss.
    maybeSetDefaultProperty(consumerConfigProps, "enable.auto.commit", "false")
    // Hardcode the deserializer to ByteArrayDeserializer
    consumerConfigProps.setProperty("key.deserializer", classOf[ByteArrayDeserializer].getName)
    consumerConfigProps.setProperty("value.deserializer", classOf[ByteArrayDeserializer].getName)
    // The default client id is group id, we manually set client id to groupId-index to avoid metric collision
    val groupIdString = consumerConfigProps.getProperty("group.id")
    val consumers = (0 until numStreams) map { i =>
      consumerConfigProps.setProperty("client.id", groupIdString + "-" + i.toString)
      new KafkaConsumer[Array[Byte], Array[Byte]](consumerConfigProps)
    }
    whitelist.getOrElse(throw new IllegalArgumentException("White list cannot be empty"))
    consumers.map(consumer => new ConsumerWrapper(consumer, customRebalanceListener, whitelist))
  }

  def commitOffsets(consumerWrapper: ConsumerWrapper): Unit = {
    if (!exitingOnSendFailure) {
      var retry = 0
      var retryNeeded = true
      while (retryNeeded) {
        trace("Committing offsets.")
        try {
          consumerWrapper.commit()
          lastSuccessfulCommitTime = time.milliseconds
          retryNeeded = false
        } catch {
          case e: WakeupException =>
            // we only call wakeup() once to close the consumer,
            // so if we catch it in commit we can safely retry
            // and re-throw to break the loop
            commitOffsets(consumerWrapper)
            throw e

          case _: TimeoutException =>
            Try(consumerWrapper.consumer.listTopics) match {
              case Success(visibleTopics) =>
                consumerWrapper.offsets.retain((tp, _) => visibleTopics.containsKey(tp.topic))
              case Failure(e) =>
                warn("Failed to list all authorized topics after committing offsets timed out: ", e)
            }

            retry += 1
            warn("Failed to commit offsets because the offset commit request processing can not be completed in time. " +
              s"If you see this regularly, it could indicate that you need to increase the consumer's ${ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG} " +
              s"Last successful offset commit timestamp=$lastSuccessfulCommitTime, retry count=$retry")
            Thread.sleep(100)

          case _: CommitFailedException =>
            retryNeeded = false
            warn("Failed to commit offsets because the consumer group has rebalanced and assigned partitions to " +
              "another instance. If you see this regularly, it could indicate that you need to either increase " +
              s"the consumer's ${ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG} or reduce the number of records " +
              s"handled on each iteration with ${ConsumerConfig.MAX_POLL_RECORDS_CONFIG}")
        }
      }
    } else {
      info("Exiting on send failure, skip committing offsets.")
    }
  }

  def cleanShutdown() {
    if (isShuttingDown.compareAndSet(false, true)) {
      info("Start clean shutdown.")
      // Shutdown consumer threads.
      info("Shutting down consumer threads.")
      if (mirrorMakerThreads != null) {
        mirrorMakerThreads.foreach(_.shutdown())
        mirrorMakerThreads.foreach(_.awaitShutdown())
      }
      info("Closing producer.")
      producer.close()
      info("Kafka mirror maker shutdown successfully")
    }
  }

  private def maybeSetDefaultProperty(properties: Properties, propertyName: String, defaultValue: String) {
    val propertyValue = properties.getProperty(propertyName)
    properties.setProperty(propertyName, Option(propertyValue).getOrElse(defaultValue))
    if (properties.getProperty(propertyName) != defaultValue)
      info("Property %s is overridden to %s - data loss or message reordering is possible.".format(propertyName, propertyValue))
  }

  class MirrorMakerThread(consumerWrapper: ConsumerWrapper,
                          val threadId: Int) extends Thread with Logging with KafkaMetricsGroup {
    private val threadName = "mirrormaker-thread-" + threadId
    private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
    private var lastOffsetCommitMs = System.currentTimeMillis()
    @volatile private var shuttingDown: Boolean = false
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    private def toBaseConsumerRecord(record: ConsumerRecord[Array[Byte], Array[Byte]]): BaseConsumerRecord =
      BaseConsumerRecord(record.topic,
        record.partition,
        record.offset,
        record.timestamp,
        record.timestampType,
        record.key,
        record.value,
        record.headers)

    override def run() {
      info(s"Starting mirror maker thread $threadName")
      try {
        consumerWrapper.init()

        // We needed two while loops due to the old consumer semantics, this can now be simplified
        while (!exitingOnSendFailure && !shuttingDown) {
          try {
            while (!exitingOnSendFailure && !shuttingDown) {
              val data = consumerWrapper.receive()
              if (data.value != null) {
                trace("Sending message with value size %d and offset %d.".format(data.value.length, data.offset))
              } else {
                trace("Sending message with null value and offset %d.".format(data.offset))
              }
              val records = messageHandler.handle(toBaseConsumerRecord(data))
              records.asScala.foreach(producer.send)
              maybeFlushAndCommitOffsets()
            }
          } catch {
            case _: NoRecordsException =>
              trace("Caught NoRecordsException, continue iteration.")
            case _: WakeupException =>
              trace("Caught WakeupException, continue iteration.")
            case e: KafkaException if (shuttingDown || exitingOnSendFailure) =>
              trace(s"Ignoring caught KafkaException during shutdown. sendFailure: $exitingOnSendFailure.", e)
          }
          maybeFlushAndCommitOffsets()
        }
      } catch {
        case t: Throwable =>
          exitingOnSendFailure = true
          fatal("Mirror maker thread failure due to ", t)
      } finally {
        CoreUtils.swallow ({
          info("Flushing producer.")
          producer.flush()

          // note that this commit is skipped if flush() fails which ensures that we don't lose messages
          info("Committing consumer offsets.")
          commitOffsets(consumerWrapper)
        }, this)

        info("Shutting down consumer connectors.")
        CoreUtils.swallow(consumerWrapper.wakeup(), this)
        CoreUtils.swallow(consumerWrapper.close(), this)
        shutdownLatch.countDown()
        info("Mirror maker thread stopped")
        // if it exits accidentally, stop the entire mirror maker
        if (!isShuttingDown.get()) {
          fatal("Mirror maker thread exited abnormally, stopping the whole mirror maker.")
          sys.exit(-1)
        }
      }
    }

    def maybeFlushAndCommitOffsets() {
      if (System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
        debug("Committing MirrorMaker state.")
        producer.flush()
        commitOffsets(consumerWrapper)
        lastOffsetCommitMs = System.currentTimeMillis()
      }
    }

    def shutdown() {
      try {
        info(s"$threadName shutting down")
        shuttingDown = true
        consumerWrapper.wakeup()
      }
      catch {
        case _: InterruptedException =>
          warn("Interrupt during shutdown of the mirror maker thread")
      }
    }

    def awaitShutdown() {
      try {
        shutdownLatch.await()
        info("Mirror maker thread shutdown complete")
      } catch {
        case _: InterruptedException =>
          warn("Shutdown of the mirror maker thread interrupted")
      }
    }
  }

  // Visible for testing
  private[tools] class ConsumerWrapper(private[tools] val consumer: Consumer[Array[Byte], Array[Byte]],
                                       customRebalanceListener: Option[ConsumerRebalanceListener],
                                       whitelistOpt: Option[String]) {
    val regex = whitelistOpt.getOrElse(throw new IllegalArgumentException("New consumer only supports whitelist."))
    var recordIter: java.util.Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = null

    // We manually maintain the consumed offsets for historical reasons and it could be simplified
    // Visible for testing
    private[tools] val offsets = new HashMap[TopicPartition, Long]()

    def init() {
      debug("Initiating consumer")
      val consumerRebalanceListener = new InternalRebalanceListener(this, customRebalanceListener)
      whitelistOpt.foreach { whitelist =>
        try {
          consumer.subscribe(Pattern.compile(Whitelist(whitelist).regex), consumerRebalanceListener)
        } catch {
          case pse: RuntimeException =>
            error(s"Invalid expression syntax: $whitelist")
            throw pse
        }
      }
    }

    def receive(): ConsumerRecord[Array[Byte], Array[Byte]] = {
      if (recordIter == null || !recordIter.hasNext) {
        // In scenarios where data does not arrive within offsetCommitIntervalMs and
        // offsetCommitIntervalMs is less than poll's timeout, offset commit will be delayed for any
        // uncommitted record since last poll. Using one second as poll's timeout ensures that
        // offsetCommitIntervalMs, of value greater than 1 second, does not see delays in offset
        // commit.
        recordIter = consumer.poll(Duration.ofSeconds(1)).iterator
        if (!recordIter.hasNext)
          throw new NoRecordsException
      }

      val record = recordIter.next()
      val tp = new TopicPartition(record.topic, record.partition)

      offsets.put(tp, record.offset + 1)
      record
    }

    def wakeup() {
      consumer.wakeup()
    }

    def close() {
      consumer.close()
    }

    def commit() {
      consumer.commitSync(offsets.map { case (tp, offset) => (tp, new OffsetAndMetadata(offset)) }.asJava)
      offsets.clear()
    }
  }

  private class InternalRebalanceListener(consumerWrapper: ConsumerWrapper,
                                          customRebalanceListener: Option[ConsumerRebalanceListener])
    extends ConsumerRebalanceListener {

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]) {
      producer.flush()
      commitOffsets(consumerWrapper)
      customRebalanceListener.foreach(_.onPartitionsRevoked(partitions))
    }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]) {
      customRebalanceListener.foreach(_.onPartitionsAssigned(partitions))
    }
  }

  private[tools] class MirrorMakerProducer(val sync: Boolean, val producerProps: Properties) {

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

    def close(timeout: Long) {
      this.producer.close(timeout, TimeUnit.MILLISECONDS)
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
        if (abortOnSendFailure) {
          info("Closing producer due to send failure.")
          exitingOnSendFailure = true
          producer.close(0)
        }
        numDroppedMessages.incrementAndGet()
      }
    }
  }

  /**
   * If message.handler.args is specified. A constructor that takes in a String as argument must exist.
   */
  trait MirrorMakerMessageHandler {
    def handle(record: BaseConsumerRecord): util.List[ProducerRecord[Array[Byte], Array[Byte]]]
  }

  private[tools] object defaultMirrorMakerMessageHandler extends MirrorMakerMessageHandler {
    override def handle(record: BaseConsumerRecord): util.List[ProducerRecord[Array[Byte], Array[Byte]]] = {
      val timestamp: java.lang.Long = if (record.timestamp == RecordBatch.NO_TIMESTAMP) null else record.timestamp
      Collections.singletonList(new ProducerRecord(record.topic, null, timestamp, record.key, record.value, record.headers))
    }
  }

  // package-private for tests
  private[tools] class NoRecordsException extends RuntimeException

  class MirrorMakerOptions(args: Array[String]) extends CommandDefaultOptions(args) {

    val consumerConfigOpt = parser.accepts("consumer.config",
      "Embedded consumer config for consuming from the source cluster.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    parser.accepts("new.consumer",
      "DEPRECATED Use new consumer in mirror maker (this is the default so this option will be removed in " +
        "a future version).")

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

    val offsetCommitIntervalMsOpt = parser.accepts("offset.commit.interval.ms",
      "Offset commit interval in ms.")
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
      "Arguments used by custom rebalance listener for mirror maker consumer.")
      .withRequiredArg()
      .describedAs("Arguments passed to custom rebalance listener constructor as a string.")
      .ofType(classOf[String])

    val messageHandlerOpt = parser.accepts("message.handler",
      "Message handler which will process every record in-between consumer and producer.")
      .withRequiredArg()
      .describedAs("A custom message handler of type MirrorMakerMessageHandler")
      .ofType(classOf[String])

    val messageHandlerArgsOpt = parser.accepts("message.handler.args",
      "Arguments used by custom message handler for mirror maker.")
      .withRequiredArg()
      .describedAs("Arguments passed to message handler constructor.")
      .ofType(classOf[String])

    val abortOnSendFailureOpt = parser.accepts("abort.on.send.failure",
      "Configure the mirror maker to exit on a failed send.")
      .withRequiredArg()
      .describedAs("Stop the entire mirror maker when a send failure occurs")
      .ofType(classOf[String])
      .defaultsTo("true")

    options = parser.parse(args: _*)

    def checkArgs() = {
      CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt)
      val consumerProps = Utils.loadProps(options.valueOf(consumerConfigOpt))

      if (!options.has(whitelistOpt)) {
        error("whitelist must be specified")
        sys.exit(1)
      }

      if (!consumerProps.containsKey(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG))
        System.err.println("WARNING: The default partition assignment strategy of the mirror maker will " +
          "change from 'range' to 'roundrobin' in an upcoming release (so that better load balancing can be achieved). If " +
          "you prefer to make this switch in advance of that release add the following to the corresponding " +
          "config: 'partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor'")

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
      val sync = producerProps.getProperty("producer.type", "async").equals("sync")
      producerProps.remove("producer.type")
      // Defaults to no data loss settings.
      maybeSetDefaultProperty(producerProps, ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, Int.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all")
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
      // Always set producer key and value serializer to ByteArraySerializer.
      producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer].getName)
      producer = new MirrorMakerProducer(sync, producerProps)

      // Create consumers
      val customRebalanceListener: Option[ConsumerRebalanceListener] = {
        val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
        if (customRebalanceListenerClass != null) {
          val rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt)
          if (rebalanceListenerArgs != null)
            Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass, rebalanceListenerArgs))
          else
            Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass))
        } else {
          None
        }
      }
      val mirrorMakerConsumers = createConsumers(
        numStreams,
        consumerProps,
        customRebalanceListener,
        Option(options.valueOf(whitelistOpt)))

      // Create mirror maker threads.
      mirrorMakerThreads = (0 until numStreams) map (i =>
        new MirrorMakerThread(mirrorMakerConsumers(i), i))

      // Create and initialize message handler
      val customMessageHandlerClass = options.valueOf(messageHandlerOpt)
      val messageHandlerArgs = options.valueOf(messageHandlerArgsOpt)
      messageHandler = {
        if (customMessageHandlerClass != null) {
          if (messageHandlerArgs != null)
            CoreUtils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass, messageHandlerArgs)
          else
            CoreUtils.createObject[MirrorMakerMessageHandler](customMessageHandlerClass)
        } else {
          defaultMirrorMakerMessageHandler
        }
      }
    }
  }
}
