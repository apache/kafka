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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.regex.Pattern
import java.util.{Collections, Properties}

import com.yammer.metrics.core.Gauge
import joptsimple.OptionParser
import kafka.consumer.{BaseConsumer, BaseConsumerRecord, Blacklist, ConsumerIterator, ConsumerThreadId, ConsumerTimeoutException, TopicFilter, Whitelist, ZookeeperConsumerConnector, ConsumerConfig => OldConsumerConfig}
import kafka.javaapi.consumer.ConsumerRebalanceListener
import kafka.metrics.KafkaMetricsGroup
import kafka.serializer.DefaultDecoder
import kafka.utils.{CommandLineUtils, CoreUtils, Logging, ZKConfig}
import org.apache.kafka.clients.consumer
import org.apache.kafka.clients.consumer.{CommitFailedException, Consumer, ConsumerRecord, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.utils.Utils
import org.apache.kafka.common.errors.WakeupException

import scala.collection.JavaConverters._
import scala.collection.mutable.HashMap
import scala.util.control.ControlThrowable
import org.apache.kafka.clients.consumer.{ConsumerConfig => NewConsumerConfig}
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.record.RecordBatch

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

  // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
  // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
  // message was not really acked, but was skipped. This metric records the number of skipped offsets.
  newGauge("MirrorMaker-numDroppedMessages",
    new Gauge[Int] {
      def value = numDroppedMessages.get()
    })

  def main(args: Array[String]): Unit = {

    info("Starting mirror maker")
    try {
      val parser = new OptionParser(false)

      val consumerConfigOpt = parser.accepts("consumer.config",
        "Embedded consumer config for consuming from the source cluster.")
        .withRequiredArg()
        .describedAs("config file")
        .ofType(classOf[String])

      val useNewConsumerOpt = parser.accepts("new.consumer",
        "Use new consumer in mirror maker (this is the default).")

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
        "Blacklist of topics to mirror. Only old consumer supports blacklist.")
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

      val helpOpt = parser.accepts("help", "Print this message.")

      if (args.length == 0)
        CommandLineUtils.printUsageAndDie(parser, "Continuously copy data between two Kafka clusters.")


      val options = parser.parse(args: _*)

      if (options.has(helpOpt)) {
        parser.printHelpOn(System.out)
        sys.exit(0)
      }

      CommandLineUtils.checkRequiredArgs(parser, options, consumerConfigOpt, producerConfigOpt)

      val consumerProps = Utils.loadProps(options.valueOf(consumerConfigOpt))
      val useOldConsumer = consumerProps.containsKey(ZKConfig.ZkConnectProp)

      if (useOldConsumer) {
        if (options.has(useNewConsumerOpt)) {
          error(s"The consumer configuration parameter `${ZKConfig.ZkConnectProp}` is not valid when using --new.consumer")
          sys.exit(1)
        }

        if (consumerProps.containsKey(NewConsumerConfig.BOOTSTRAP_SERVERS_CONFIG)) {
          error(s"The configuration parameters `${ZKConfig.ZkConnectProp}` (old consumer) and " +
            s"`${NewConsumerConfig.BOOTSTRAP_SERVERS_CONFIG}` (new consumer) cannot be used together.")
          sys.exit(1)
        }

        if (List(whitelistOpt, blacklistOpt).count(options.has) != 1) {
          error("Exactly one of whitelist or blacklist is required.")
          sys.exit(1)
        }
      } else {
        if (options.has(blacklistOpt)) {
          error("blacklist can not be used when using new consumer in mirror maker. Use whitelist instead.")
          sys.exit(1)
        }

        if (!options.has(whitelistOpt)) {
          error("whitelist must be specified when using new consumer in mirror maker.")
          sys.exit(1)
        }

        if (!consumerProps.containsKey(NewConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG))
          System.err.println("WARNING: The default partition assignment strategy of the new-consumer-based mirror maker will " +
            "change from 'range' to 'roundrobin' in an upcoming release (so that better load balancing can be achieved). If " +
            "you prefer to make this switch in advance of that release add the following to the corresponding new-consumer " +
            "config: 'partition.assignment.strategy=org.apache.kafka.clients.consumer.RoundRobinAssignor'")

      }

      abortOnSendFailure = options.valueOf(abortOnSendFailureOpt).toBoolean
      offsetCommitIntervalMs = options.valueOf(offsetCommitIntervalMsOpt).intValue()
      val numStreams = options.valueOf(numStreamsOpt).intValue()

      Runtime.getRuntime.addShutdownHook(new Thread("MirrorMakerShutdownHook") {
        override def run(): Unit = {
          cleanShutdown()
        }
      })

      // create producer
      val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))
      val sync = producerProps.getProperty("producer.type", "async").equals("sync")
      producerProps.remove("producer.type")
      // Defaults to no data loss settings.
      maybeSetDefaultProperty(producerProps, ProducerConfig.RETRIES_CONFIG, Int.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_BLOCK_MS_CONFIG, Long.MaxValue.toString)
      maybeSetDefaultProperty(producerProps, ProducerConfig.ACKS_CONFIG, "all")
      maybeSetDefaultProperty(producerProps, ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
      // Always set producer key and value serializer to ByteArraySerializer.
      producerProps.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
      producerProps.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
      producer = new MirrorMakerProducer(sync, producerProps)

      // Create consumers
      val mirrorMakerConsumers = if (useOldConsumer) {
        val customRebalanceListener: Option[ConsumerRebalanceListener] = {
          val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
          if (customRebalanceListenerClass != null) {
            val rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt)
            if (rebalanceListenerArgs != null) {
              Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass, rebalanceListenerArgs))
            } else {
              Some(CoreUtils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass))
            }
          } else {
            None
          }
        }
        createOldConsumers(
          numStreams,
          consumerProps,
          customRebalanceListener,
          Option(options.valueOf(whitelistOpt)),
          Option(options.valueOf(blacklistOpt)))
      } else {
        val customRebalanceListener: Option[org.apache.kafka.clients.consumer.ConsumerRebalanceListener] = {
          val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
          if (customRebalanceListenerClass != null) {
            val rebalanceListenerArgs = options.valueOf(rebalanceListenerArgsOpt)
            if (rebalanceListenerArgs != null) {
                Some(CoreUtils.createObject[org.apache.kafka.clients.consumer.ConsumerRebalanceListener](customRebalanceListenerClass, rebalanceListenerArgs))
            } else {
                Some(CoreUtils.createObject[org.apache.kafka.clients.consumer.ConsumerRebalanceListener](customRebalanceListenerClass))
            }
          } else {
            None
          }
        }
        createNewConsumers(
          numStreams,
          consumerProps,
          customRebalanceListener,
          Option(options.valueOf(whitelistOpt)))
      }

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
    } catch {
      case ct : ControlThrowable => throw ct
      case t : Throwable =>
        error("Exception when starting mirror maker.", t)
    }

    mirrorMakerThreads.foreach(_.start())
    mirrorMakerThreads.foreach(_.awaitShutdown())
  }

  private def createOldConsumers(numStreams: Int,
                                 consumerConfigProps: Properties,
                                 customRebalanceListener: Option[ConsumerRebalanceListener],
                                 whitelist: Option[String],
                                 blacklist: Option[String]) : Seq[MirrorMakerBaseConsumer] = {
    // Disable consumer auto offsets commit to prevent data loss.
    maybeSetDefaultProperty(consumerConfigProps, "auto.commit.enable", "false")
    // Set the consumer timeout so we will not block for low volume pipeline. The timeout is necessary to make sure
    // Offsets are still committed for those low volume pipelines.
    maybeSetDefaultProperty(consumerConfigProps, "consumer.timeout.ms", "10000")
    // The default client id is group id, we manually set client id to groupId-index to avoid metric collision
    val groupIdString = consumerConfigProps.getProperty("group.id")
    val connectors = (0 until numStreams) map { i =>
      consumerConfigProps.setProperty("client.id", groupIdString + "-" + i.toString)
      val consumerConfig = new OldConsumerConfig(consumerConfigProps)
      new ZookeeperConsumerConnector(consumerConfig)
    }

    // create filters
    val filterSpec = if (whitelist.isDefined)
      new Whitelist(whitelist.get)
    else if (blacklist.isDefined)
      new Blacklist(blacklist.get)
    else
      throw new IllegalArgumentException("Either whitelist or blacklist should be defined!")
    (0 until numStreams) map { i =>
      val consumer = new MirrorMakerOldConsumer(connectors(i), filterSpec)
      val consumerRebalanceListener = new InternalRebalanceListenerForOldConsumer(consumer, customRebalanceListener)
      connectors(i).setConsumerRebalanceListener(consumerRebalanceListener)
      consumer
    }
  }

  def createNewConsumers(numStreams: Int,
                         consumerConfigProps: Properties,
                         customRebalanceListener: Option[org.apache.kafka.clients.consumer.ConsumerRebalanceListener],
                         whitelist: Option[String]) : Seq[MirrorMakerBaseConsumer] = {
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
    whitelist.getOrElse(throw new IllegalArgumentException("White list cannot be empty for new consumer"))
    consumers.map(consumer => new MirrorMakerNewConsumer(consumer, customRebalanceListener, whitelist))
  }

  def commitOffsets(mirrorMakerConsumer: MirrorMakerBaseConsumer): Unit = {
    if (!exitingOnSendFailure) {
      trace("Committing offsets.")
      try {
        mirrorMakerConsumer.commit()
      } catch {
        case e: WakeupException =>
          // we only call wakeup() once to close the consumer,
          // so if we catch it in commit we can safely retry
          // and re-throw to break the loop
          mirrorMakerConsumer.commit()
          throw e

        case _: CommitFailedException =>
          warn("Failed to commit offsets because the consumer group has rebalanced and assigned partitions to " +
            "another instance. If you see this regularly, it could indicate that you need to either increase " +
            s"the consumer's ${consumer.ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG} or reduce the number of records " +
            s"handled on each iteration with ${consumer.ConsumerConfig.MAX_POLL_RECORDS_CONFIG}")
      }
    } else {
      info("Exiting on send failure, skip committing offsets.")
    }
  }

  def cleanShutdown(): Unit = {
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

  private def maybeSetDefaultProperty(properties: Properties, propertyName: String, defaultValue: String): Unit = {
    val propertyValue = properties.getProperty(propertyName)
    properties.setProperty(propertyName, Option(propertyValue).getOrElse(defaultValue))
    if (properties.getProperty(propertyName) != defaultValue)
      info("Property %s is overridden to %s - data loss or message reordering is possible.".format(propertyName, propertyValue))
  }

  class MirrorMakerThread(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                          val threadId: Int) extends Thread with Logging with KafkaMetricsGroup {
    private val threadName = "mirrormaker-thread-" + threadId
    private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
    private var lastOffsetCommitMs = System.currentTimeMillis()
    @volatile private var shuttingDown: Boolean = false
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    override def run(): Unit = {
      info("Starting mirror maker thread " + threadName)
      try {
        mirrorMakerConsumer.init()

        // We need the two while loop to make sure when old consumer is used, even there is no message we
        // still commit offset. When new consumer is used, this is handled by poll(timeout).
        while (!exitingOnSendFailure && !shuttingDown) {
          try {
            while (!exitingOnSendFailure && !shuttingDown && mirrorMakerConsumer.hasData) {
              val data = mirrorMakerConsumer.receive()
              if (data.value != null) {
                trace("Sending message with value size %d and offset %d.".format(data.value.length, data.offset))
              } else {
                trace("Sending message with null value and offset %d.".format(data.offset))
              }
              val records = messageHandler.handle(data)
              records.asScala.foreach(producer.send)
              maybeFlushAndCommitOffsets()
            }
          } catch {
            case _: ConsumerTimeoutException =>
              trace("Caught ConsumerTimeoutException, continue iteration.")
            case _: WakeupException =>
              trace("Caught ConsumerWakeupException, continue iteration.")
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
          commitOffsets(mirrorMakerConsumer)
        }, this)

        info("Shutting down consumer connectors.")
        CoreUtils.swallow(mirrorMakerConsumer.stop(), this)
        CoreUtils.swallow(mirrorMakerConsumer.cleanup(), this)
        shutdownLatch.countDown()
        info("Mirror maker thread stopped")
        // if it exits accidentally, stop the entire mirror maker
        if (!isShuttingDown.get()) {
          fatal("Mirror maker thread exited abnormally, stopping the whole mirror maker.")
          sys.exit(-1)
        }
      }
    }

    def maybeFlushAndCommitOffsets(): Unit = {
      val commitRequested = mirrorMakerConsumer.commitRequested()
      if (commitRequested || System.currentTimeMillis() - lastOffsetCommitMs > offsetCommitIntervalMs) {
        debug("Committing MirrorMaker state.")
        producer.flush()
        commitOffsets(mirrorMakerConsumer)
        lastOffsetCommitMs = System.currentTimeMillis()
        if (commitRequested)
          mirrorMakerConsumer.notifyCommit()
      }
    }

    def shutdown(): Unit = {
      try {
        info(threadName + " shutting down")
        shuttingDown = true
        mirrorMakerConsumer.stop()
      }
      catch {
        case _: InterruptedException =>
          warn("Interrupt during shutdown of the mirror maker thread")
      }
    }

    def awaitShutdown(): Unit = {
      try {
        shutdownLatch.await()
        info("Mirror maker thread shutdown complete")
      } catch {
        case _: InterruptedException =>
          warn("Shutdown of the mirror maker thread interrupted")
      }
    }
  }

  private[kafka] trait MirrorMakerBaseConsumer extends BaseConsumer {
    def init(): Unit
    def commitRequested(): Boolean
    def notifyCommit(): Unit
    def requestAndWaitForCommit(): Unit
    def hasData : Boolean
  }

  private class MirrorMakerOldConsumer(connector: ZookeeperConsumerConnector,
                                       filterSpec: TopicFilter) extends MirrorMakerBaseConsumer {
    private var iter: ConsumerIterator[Array[Byte], Array[Byte]] = null
    private var immediateCommitRequested: Boolean = false
    private var numCommitsNotified: Long = 0

    override def init(): Unit = {
      // Creating one stream per each connector instance
      val streams = connector.createMessageStreamsByFilter(filterSpec, 1, new DefaultDecoder(), new DefaultDecoder())
      require(streams.size == 1)
      val stream = streams.head
      iter = stream.iterator()
    }

    override def requestAndWaitForCommit(): Unit = {
      this.synchronized {
        // only wait() if mirrorMakerConsumer has been initialized and it has not been cleaned up.
        if (iter != null) {
          immediateCommitRequested = true
          val nextNumCommitsNotified = numCommitsNotified + 1
          do {
            this.wait()
          } while (numCommitsNotified < nextNumCommitsNotified)
        }
      }
    }

    override def notifyCommit(): Unit = {
      this.synchronized {
        immediateCommitRequested = false
        numCommitsNotified = numCommitsNotified + 1
        this.notifyAll()
      }
    }

    override def commitRequested(): Boolean = {
      this.synchronized {
        immediateCommitRequested
      }
    }

    override def hasData = iter.hasNext()

    override def receive() : BaseConsumerRecord = {
      val messageAndMetadata = iter.next()
      BaseConsumerRecord(messageAndMetadata.topic,
                         messageAndMetadata.partition,
                         messageAndMetadata.offset,
                         messageAndMetadata.timestamp,
                         messageAndMetadata.timestampType,
                         messageAndMetadata.key,
                         messageAndMetadata.message,
                         new RecordHeaders())
    }

    override def stop(): Unit = {
      // Do nothing
    }

    override def cleanup(): Unit = {
      // We need to set the iterator to null and notify the rebalance listener thread.
      // This is to handle the case that the consumer rebalance is triggered when the
      // mirror maker thread is shutting down and the rebalance listener is waiting for the offset commit.
      this.synchronized {
        iter = null
        if (immediateCommitRequested) {
          notifyCommit()
        }
      }
      connector.shutdown()
    }

    override def commit(): Unit = {
      connector.commitOffsets
    }
  }

  // Only for testing
  private[tools] class MirrorMakerNewConsumer(consumer: Consumer[Array[Byte], Array[Byte]],
                                       customRebalanceListener: Option[org.apache.kafka.clients.consumer.ConsumerRebalanceListener],
                                       whitelistOpt: Option[String])
    extends MirrorMakerBaseConsumer {
    val regex = whitelistOpt.getOrElse(throw new IllegalArgumentException("New consumer only supports whitelist."))
    var recordIter: java.util.Iterator[ConsumerRecord[Array[Byte], Array[Byte]]] = null

    // TODO: we need to manually maintain the consumed offsets for new consumer
    // since its internal consumed position is updated in batch rather than one
    // record at a time, this can be resolved when we break the unification of both consumers
    private val offsets = new HashMap[TopicPartition, Long]()

    override def init(): Unit = {
      debug("Initiating new consumer")
      val consumerRebalanceListener = new InternalRebalanceListenerForNewConsumer(this, customRebalanceListener)
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

    override def requestAndWaitForCommit(): Unit = {
      // Do nothing
    }

    override def notifyCommit(): Unit = {
      // Do nothing
    }
    
    override def commitRequested(): Boolean = {
      false
    }

    override def hasData = true

    override def receive() : BaseConsumerRecord = {
      if (recordIter == null || !recordIter.hasNext) {
        // In scenarios where data does not arrive within offsetCommitIntervalMs and
        // offsetCommitIntervalMs is less than poll's timeout, offset commit will be delayed for any
        // uncommitted record since last poll. Using one second as poll's timeout ensures that
        // offsetCommitIntervalMs, of value greater than 1 second, does not see delays in offset
        // commit.
        recordIter = consumer.poll(1000).iterator
        if (!recordIter.hasNext)
          throw new ConsumerTimeoutException
      }

      val record = recordIter.next()
      val tp = new TopicPartition(record.topic, record.partition)

      offsets.put(tp, record.offset + 1)

      BaseConsumerRecord(record.topic,
                         record.partition,
                         record.offset,
                         record.timestamp,
                         record.timestampType,
                         record.key,
                         record.value,
                         record.headers)
    }

    override def stop(): Unit = {
      consumer.wakeup()
    }

    override def cleanup(): Unit = {
      consumer.close()
    }

    override def commit(): Unit = {
      consumer.commitSync(offsets.map { case (tp, offset) =>  (tp, new OffsetAndMetadata(offset, ""))}.asJava)
      offsets.clear()
    }
  }

  private class InternalRebalanceListenerForNewConsumer(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                                                        customRebalanceListenerForNewConsumer: Option[org.apache.kafka.clients.consumer.ConsumerRebalanceListener])
    extends org.apache.kafka.clients.consumer.ConsumerRebalanceListener {

    override def onPartitionsRevoked(partitions: util.Collection[TopicPartition]): Unit = {
      producer.flush()
      commitOffsets(mirrorMakerConsumer)
      customRebalanceListenerForNewConsumer.foreach(_.onPartitionsRevoked(partitions))
    }

    override def onPartitionsAssigned(partitions: util.Collection[TopicPartition]): Unit = {
      customRebalanceListenerForNewConsumer.foreach(_.onPartitionsAssigned(partitions))
    }
  }

  private class InternalRebalanceListenerForOldConsumer(mirrorMakerConsumer: MirrorMakerBaseConsumer,
                                                        customRebalanceListenerForOldConsumer: Option[ConsumerRebalanceListener])
    extends ConsumerRebalanceListener {

    override def beforeReleasingPartitions(partitionOwnership: java.util.Map[String, java.util.Set[java.lang.Integer]]): Unit = {
      // The zookeeper listener thread, which executes this method, needs to wait for MirrorMakerThread to flush data and commit offset
      mirrorMakerConsumer.requestAndWaitForCommit()
      // invoke custom consumer rebalance listener
      customRebalanceListenerForOldConsumer.foreach(_.beforeReleasingPartitions(partitionOwnership))
    }

    override def beforeStartingFetchers(consumerId: String,
                                        partitionAssignment: java.util.Map[String, java.util.Map[java.lang.Integer, ConsumerThreadId]]): Unit = {
      customRebalanceListenerForOldConsumer.foreach(_.beforeStartingFetchers(consumerId, partitionAssignment))
    }
  }

  private[tools] class MirrorMakerProducer(val sync: Boolean, val producerProps: Properties) {

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    def send(record: ProducerRecord[Array[Byte], Array[Byte]]): Unit = {
      if (sync) {
        this.producer.send(record).get()
      } else {
          this.producer.send(record,
            new MirrorMakerProducerCallback(record.topic(), record.key(), record.value()))
      }
    }

    def flush(): Unit = {
      this.producer.flush()
    }

    def close(): Unit = {
      this.producer.close()
    }

    def close(timeout: Long): Unit = {
      this.producer.close(timeout, TimeUnit.MILLISECONDS)
    }
  }

  private class MirrorMakerProducerCallback (topic: String, key: Array[Byte], value: Array[Byte])
    extends ErrorLoggingCallback(topic, key, value, false) {

    override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
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

}
