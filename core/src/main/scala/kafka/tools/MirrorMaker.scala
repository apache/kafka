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

import com.yammer.metrics.core._
import kafka.common.{TopicAndPartition, OffsetAndMetadata}
import kafka.javaapi.consumer.ConsumerRebalanceListener
import kafka.utils._
import kafka.consumer._
import kafka.serializer._
import kafka.producer.{KeyedMessage, ProducerConfig}
import kafka.metrics.KafkaMetricsGroup
import org.apache.kafka.clients.producer.internals.ErrorLoggingCallback
import org.apache.kafka.clients.producer.{KafkaProducer, RecordMetadata, ProducerRecord}

import joptsimple.OptionParser
import java.util.Properties
import java.util.concurrent.atomic.{AtomicInteger, AtomicBoolean}
import java.util.concurrent._

/**
 * The mirror maker consists of three major modules:
 *  Consumer Threads - The consumer threads consume messages from source Kafka cluster through
 *                     ZookeeperConsumerConnector and put them into corresponding data channel queue based on hash value
 *                     of source topic-partitionId string. This guarantees the message order in source partition is
 *                     preserved.
 *  Producer Threads - Producer threads take messages out of data channel queues and send them to target cluster. Each
 *                     producer thread is bound to one data channel queue, so that the message order is preserved.
 *  Data Channel - The data channel has multiple queues. The number of queue is same as number of producer threads.
 *
 * If new producer is used, the offset will be committed based on the new producer's callback. An offset map is
 * maintained and updated on each send() callback. A separate offset commit thread will commit the offset periodically.
 * @note For mirror maker, the following settings are required to make sure there is no data loss:
 *       1. use new producer with following settings
 *            acks=all
 *            retries=max integer
 *            block.on.buffer.full=true
 *       2. Consumer Settings
 *            auto.commit.enable=false
 *       If --no.data.loss flag is set in option, then those settings are automatically applied.
 */
object MirrorMaker extends Logging with KafkaMetricsGroup {

  private var connector: ZookeeperConsumerConnector = null
  private var consumerThreads: Seq[ConsumerThread] = null
  private var producerThreads: Seq[ProducerThread] = null
  private val isShuttingdown: AtomicBoolean = new AtomicBoolean(false)
  private val scheduler: KafkaScheduler = new KafkaScheduler(threads = 1)

  private val unackedOffsetsMap: Pool[TopicAndPartition, UnackedOffsets] =
      new Pool[TopicAndPartition, UnackedOffsets](valueFactory = Some((k: TopicAndPartition) => new UnackedOffsets))
  // Track the messages unacked for consumer rebalance
  private var numUnackedMessages: AtomicInteger = new AtomicInteger(0)
  private var numSkippedUnackedMessages: AtomicInteger = new AtomicInteger(0)
  private var consumerRebalanceListener: ConsumerRebalanceListener = null
  // This is to indicate whether the rebalance is going on so the producer callback knows if
  // the flag indicates internal consumer rebalance callback is waiting for all the messages sent to be acked.
  private var waitingForMessageAcks: Boolean = false

  private val shutdownMessage : MirrorMakerRecord = new MirrorMakerRecord("shutdown", 0, 0, null, "shutdown".getBytes)

  newGauge("MirrorMaker-NumUnackedMessages",
    new Gauge[Int] {
      def value = numUnackedMessages.get()
    })

  // The number of unacked offsets in the unackedOffsetsMap
  newGauge("MirrorMaker-UnackedOffsetListsSize",
    new Gauge[Int] {
      def value = unackedOffsetsMap.iterator.map{
        case(_, unackedOffsets) => unackedOffsets.size
      }.sum
    })

  // If a message send failed after retries are exhausted. The offset of the messages will also be removed from
  // the unacked offset list to avoid offset commit being stuck on that offset. In this case, the offset of that
  // message was not really acked, but was skipped. This metric records the number of skipped offsets.
  newGauge("MirrorMaker-NumSkippedOffsets",
    new Gauge[Int] {
      def value = numSkippedUnackedMessages.get()
    })

  def main(args: Array[String]) {
    
    info ("Starting mirror maker")
    val parser = new OptionParser

    val consumerConfigOpt = parser.accepts("consumer.config",
      "Embedded consumer config for consuming from the source cluster.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    // Please see note about MaxInflightRequests
    val producerConfigOpt = parser.accepts("producer.config",
      "Embedded producer config.")
      .withRequiredArg()
      .describedAs("config file")
      .ofType(classOf[String])

    val useNewProducerOpt = parser.accepts("new.producer",
      "Use the new producer implementation.")

    val noDataLossOpt = parser.accepts("no.data.loss",
      "Configure the mirror maker to have no data loss.")

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

    val bufferByteSizeOpt =  parser.accepts("queue.byte.size",
      "Maximum bytes that can be buffered in each data channel queue")
      .withRequiredArg()
      .describedAs("Data channel queue size in terms of number of bytes")
      .ofType(classOf[java.lang.Integer])
      .defaultsTo(100000000)

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
    val bufferByteSize = options.valueOf(bufferByteSizeOpt).intValue()
    val offsetCommitIntervalMs = options.valueOf(offsetCommitIntervalMsOpt).intValue()

    // create consumer connector
    val consumerConfigProps = Utils.loadProps(options.valueOf(consumerConfigOpt))
    val noDataLoss = options.has(noDataLossOpt)
    // disable consumer auto commit because offset will be committed by offset commit thread.
    if (noDataLoss)
      consumerConfigProps.setProperty("auto.commit.enable","false")
    val consumerConfig = new ConsumerConfig(consumerConfigProps)
    connector = new ZookeeperConsumerConnector(consumerConfig)

    // create a data channel btw the consumers and the producers
    val mirrorDataChannel = new DataChannel(bufferSize, bufferByteSize, numInputs = numStreams, numOutputs = numProducers)

    // set consumer rebalance listener
    // custom rebalance listener will be invoked after internal listener finishes its work.
    val customRebalanceListenerClass = options.valueOf(consumerRebalanceListenerOpt)
    val customRebalanceListener = {
      if (customRebalanceListenerClass != null)
        Some(Utils.createObject[ConsumerRebalanceListener](customRebalanceListenerClass))
      else
        None
    }
    consumerRebalanceListener = new InternalRebalanceListener(mirrorDataChannel, customRebalanceListener)
    connector.setConsumerRebalanceListener(consumerRebalanceListener)

    // create producer threads
    val producerProps = Utils.loadProps(options.valueOf(producerConfigOpt))
    val useNewProducer = {
      // Override producer settings if no.data.loss is set
      if (noDataLoss) {
        producerProps.setProperty("retries",Int.MaxValue.toString)
        producerProps.setProperty("block.on.buffer.full", "true")
        producerProps.setProperty("acks","all")
        true
      } else {
        options.has(useNewProducerOpt)
      }
    }
    val clientId = producerProps.getProperty("client.id", "")
    producerThreads = (0 until numProducers).map(i => {
      producerProps.setProperty("client.id", clientId + "-" + i)
      val producer =
      if (useNewProducer) {
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                          "org.apache.kafka.common.serialization.ByteArraySerializer")
        producerProps.put(org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                          "org.apache.kafka.common.serialization.ByteArraySerializer")
        new MirrorMakerNewProducer(producerProps)
      }
      else
        new MirrorMakerOldProducer(producerProps)
      new ProducerThread(mirrorDataChannel, producer, i)
    })

    // start offset commit thread
    if (noDataLoss) {
      /**
       * The offset commit thread periodically commit consumed offsets. With the new producer,
       * the offsets are updated upon the returned future metadata of the send() call; with the old producer,
       * the offsets are updated upon the consumer's iterator advances. By doing this, it is guaranteed no data
       * loss even when mirror maker is uncleanly shutdown with the new producer, while with the old producer
       * messages inside the data channel could be lost upon mirror maker unclean shutdown.
       */
      scheduler.startup()
      scheduler.schedule("offset-commit", commitOffsets, 0, offsetCommitIntervalMs, TimeUnit.MILLISECONDS)
    }

    // create consumer threads
    val filterSpec = if (options.has(whitelistOpt))
      new Whitelist(options.valueOf(whitelistOpt))
    else
      new Blacklist(options.valueOf(blacklistOpt))

    var streams: Seq[KafkaStream[Array[Byte], Array[Byte]]] = Nil
    try {
      streams = connector.createMessageStreamsByFilter(filterSpec, numStreams, new DefaultDecoder(), new DefaultDecoder())
    } catch {
      case t: Throwable =>
        fatal("Unable to create stream - shutting down mirror maker.", t)
        connector.shutdown()
    }
    consumerThreads = streams.zipWithIndex.map(streamAndIndex => new ConsumerThread(streamAndIndex._1, mirrorDataChannel, streamAndIndex._2))
    assert(consumerThreads.size == numStreams)

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
    producerThreads.foreach(_.awaitShutdown())
  }

  def cleanShutdown() {
    if (isShuttingdown.compareAndSet(false, true)) {
      info("Start clean shutdown.")
      // Shutdown consumer threads.
      info("Shutting down consumer threads.")
      if (consumerThreads != null) {
        consumerThreads.foreach(_.shutdown())
        consumerThreads.foreach(_.awaitShutdown())
      }
      // After consumer threads exit, shutdown producer.
      info("Shutting down producer threads.")
      if (producerThreads != null) {
        producerThreads.foreach(_.shutdown())
        producerThreads.foreach(_.awaitShutdown())
      }
      // offset commit thread should only be shutdown after producer threads are shutdown, so we don't lose offsets.
      scheduler.shutdown()
      swallow(commitOffsets())

      // connector should only be shutdown after offsets are committed.
      info("Shutting down consumer connectors.")
      if (connector != null)
        connector.shutdown()
      info("Kafka mirror maker shutdown successfully")
    }
  }

  class DataChannel(messageCapacity: Int, byteCapacity: Int, numInputs: Int, numOutputs: Int)
      extends KafkaMetricsGroup {

    val queues = new Array[ByteBoundedBlockingQueue[MirrorMakerRecord]](numOutputs)
    val channelSizeHists = new Array[Histogram](numOutputs)
    val channelByteSizeHists = new Array[Histogram](numOutputs)
    val sizeFunction = (record: MirrorMakerRecord) => record.size
    for (i <- 0 until numOutputs) {
      queues(i) = new ByteBoundedBlockingQueue[MirrorMakerRecord](messageCapacity, byteCapacity, Some(sizeFunction))
      channelSizeHists(i) = newHistogram("MirrorMaker-DataChannel-queue-%d-NumMessages".format(i))
      channelByteSizeHists(i) = newHistogram("MirrorMaker-DataChannel-queue-%d-Bytes".format(i))
    }
    private val channelRecordSizeHist = newHistogram("MirrorMaker-DataChannel-Record-Size")

    // We use a single meter for aggregated wait percentage for the data channel.
    // Since meter is calculated as total_recorded_value / time_window and
    // time_window is independent of the number of threads, each recorded wait
    // time should be discounted by # threads.
    private val waitPut = newMeter("MirrorMaker-DataChannel-WaitOnPut", "percent", TimeUnit.NANOSECONDS)
    private val waitTake = newMeter("MirrorMaker-DataChannel-WaitOnTake", "percent", TimeUnit.NANOSECONDS)

    def put(record: MirrorMakerRecord) {
      // Use hash of source topic-partition to decide which queue to put the message in. The benefit is that
      // we can maintain the message order for both keyed and non-keyed messages.
      val queueId =
        Utils.abs(java.util.Arrays.hashCode((record.sourceTopic + record.sourcePartition).toCharArray)) % numOutputs
      put(record, queueId)
    }

    def put(record: MirrorMakerRecord, queueId: Int) {
      val queue = queues(queueId)

      var putSucceed = false
      while (!putSucceed) {
        val startPutTime = SystemTime.nanoseconds
        putSucceed = queue.offer(record, 500, TimeUnit.MILLISECONDS)
        waitPut.mark((SystemTime.nanoseconds - startPutTime) / numInputs)
      }
      channelSizeHists(queueId).update(queue.size())
      channelByteSizeHists(queueId).update(queue.byteSize())
      channelRecordSizeHist.update(sizeFunction(record))
    }

    def take(queueId: Int): MirrorMakerRecord = {
      val queue = queues(queueId)
      var data: MirrorMakerRecord = null
      while (data == null) {
        val startTakeTime = SystemTime.nanoseconds
        data = queue.poll(500, TimeUnit.MILLISECONDS)
        waitTake.mark((SystemTime.nanoseconds - startTakeTime) / numOutputs)
      }
      channelSizeHists(queueId).update(queue.size())
      channelByteSizeHists(queueId).update(queue.byteSize())
      data
    }

    def clear() {
      queues.foreach(queue => queue.clear())
    }
  }

  class ConsumerThread(stream: KafkaStream[Array[Byte], Array[Byte]],
                       mirrorDataChannel: DataChannel,
                       threadId: Int)
          extends Thread with Logging with KafkaMetricsGroup {

    private val shutdownLatch = new CountDownLatch(1)
    private val threadName = "mirrormaker-consumer-" + threadId
    this.logIdent = "[%s] ".format(threadName)
    private var shutdownFlag: Boolean = false

    this.setName(threadName)

    override def run() {
      info("Starting mirror maker consumer thread " + threadName)
      try {
        val iter = stream.iterator()
        while (!shutdownFlag && iter.hasNext()) {
          val msgAndMetadata = iter.next()
          val data = new MirrorMakerRecord(msgAndMetadata.topic,
                                           msgAndMetadata.partition,
                                           msgAndMetadata.offset,
                                           msgAndMetadata.key(),
                                           msgAndMetadata.message())
          mirrorDataChannel.put(data)
        }
      } catch {
        case e: Throwable => {
          fatal("Stream unexpectedly exited.", e)
        }
      } finally {
        shutdownLatch.countDown()
        info("Consumer thread stopped")

        // If it exits accidentally, stop the entire mirror maker.
        if (!isShuttingdown.get()) {
          fatal("Consumer thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
      }
    }

    def shutdown() {
      shutdownFlag = true
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
                        val producer: MirrorMakerBaseProducer,
                        val threadId: Int) extends Thread with Logging with KafkaMetricsGroup {
    private val threadName = "mirrormaker-producer-" + threadId
    private val shutdownLatch: CountDownLatch = new CountDownLatch(1)
    this.logIdent = "[%s] ".format(threadName)

    setName(threadName)

    override def run() {
      info("Starting mirror maker producer thread " + threadName)
      try {
        while (true) {
          val data: MirrorMakerRecord = dataChannel.take(threadId)
          trace("Sending message with value size %d".format(data.value.size))
          if(data eq shutdownMessage) {
            info("Received shutdown message")
            return
          }
          producer.send(new TopicAndPartition(data.sourceTopic, data.sourcePartition),
                        data.sourceOffset,
                        data.key,
                        data.value)
        }
      } catch {
        case t: Throwable =>
          fatal("Producer thread failure due to ", t)
      } finally {
        shutdownLatch.countDown()
        info("Producer thread stopped")
        // if it exits accidentally, stop the entire mirror maker
        if (!isShuttingdown.get()) {
          fatal("Producer thread exited abnormally, stopping the whole mirror maker.")
          System.exit(-1)
        }
      }
    }

    def shutdown() {
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

    def awaitShutdown() {
      try {
        shutdownLatch.await()
        producer.close()
        info("Producer thread shutdown complete")
      } catch {
        case ie: InterruptedException => {
          warn("Shutdown of the producer thread interrupted")
        }
      }
    }
  }

  private def commitOffsets()  {
    try {
      info("Committing offsets")
      val offsetsToCommit = collection.immutable.Map(unackedOffsetsMap.map {
        case (topicPartition, unackedOffsets) =>
          topicPartition -> OffsetAndMetadata(unackedOffsets.getOffsetToCommit, null)
      }.toSeq: _*)
      if (connector == null) {
        warn("No consumer connector available to commit offset.")
      } else {
        connector.commitOffsets(offsetsToCommit, isAutoCommit = false)
      }
    } catch {
      case e: OutOfMemoryError =>
        fatal("Shutting down mirror maker due to error when committing offsets.", e)
        System.exit(-1)
      case t: Throwable =>
        warn("Offsets commit failed due to ", t)
    }
  }

  private[kafka] trait MirrorMakerBaseProducer {
    def send(sourceTopicPartition: TopicAndPartition, sourceOffset: Long, key: Array[Byte], value: Array[Byte])
    def close()
  }

  private class MirrorMakerNewProducer (val producerProps: Properties) extends MirrorMakerBaseProducer {

    val sync = producerProps.getProperty("producer.type", "async").equals("sync")

    val producer = new KafkaProducer[Array[Byte], Array[Byte]](producerProps)

    override def send(sourceTopicPartition: TopicAndPartition, sourceOffset: Long, key: Array[Byte], value: Array[Byte]) {
      val record = new ProducerRecord[Array[Byte], Array[Byte]](sourceTopicPartition.topic, key, value)
      if(sync) {
        this.producer.send(record).get()
        unackedOffsetsMap.getAndMaybePut(sourceTopicPartition).maybeUpdateMaxOffsetSeen(sourceOffset)
      } else {

        val unackedOffsets = unackedOffsetsMap.getAndMaybePut(sourceTopicPartition)
        // synchronize to ensure that addOffset precedes removeOffset
        unackedOffsets synchronized {
          val unackedOffset = new UnackedOffset(sourceOffset)
          this.producer.send(record,
            new MirrorMakerProducerCallback(sourceTopicPartition, unackedOffset, key, value))
          // add offset to unackedOffsets
          unackedOffsets.addOffset(unackedOffset)
          numUnackedMessages.incrementAndGet()
        }
      }
    }

    override def close() {
      this.producer.close()
    }
  }

  private class MirrorMakerOldProducer (val producerProps: Properties) extends MirrorMakerBaseProducer {

    // default to byte array partitioner
    if (producerProps.getProperty("partitioner.class") == null)
      producerProps.setProperty("partitioner.class", classOf[kafka.producer.ByteArrayPartitioner].getName)
    val producer = new kafka.producer.Producer[Array[Byte], Array[Byte]](new ProducerConfig(producerProps))

    override def send(topicPartition: TopicAndPartition, offset: Long, key: Array[Byte], value: Array[Byte]) {
      this.producer.send(new KeyedMessage[Array[Byte], Array[Byte]](topicPartition.topic, key, value))
    }

    override def close() {
      this.producer.close()
    }
  }

  private class MirrorMakerProducerCallback (val topicPartition: TopicAndPartition,
                                             val offset: UnackedOffset,
                                             val key: Array[Byte],
                                             val value: Array[Byte])
    extends ErrorLoggingCallback(topicPartition.topic, key, value, false) {

    override def onCompletion(metadata: RecordMetadata, exception: Exception) {
      if (exception != null) {
        // Use default call back to log error. This means the max retries of producer has reached and message
        // still could not be sent. In this case we have to remove the offsets from list to let the mirror maker
        // move on. The message failed to be sent will be lost in target cluster.
        warn("Not be able to send message, offset of "+ topicPartition + " will not advance. Total number" +
          "of skipped unacked messages is" + numSkippedUnackedMessages.incrementAndGet())
        super.onCompletion(metadata, exception)
      } else {
        trace("Updating offset for %s to %d".format(topicPartition, offset))
      }
      // remove the offset from the unackedOffsets
      val unackedOffsets = unackedOffsetsMap.get(topicPartition)
      unackedOffsets.removeOffset(offset)
      // Notify the rebalance callback only when all the messages handed to producer are acked.
      // There is a very slight chance that one message is held by producer thread and not handed to producer.
      // That message might have duplicate. We are not handling that here.
      numUnackedMessages synchronized {
        if (numUnackedMessages.decrementAndGet() == 0 && waitingForMessageAcks) {
            numUnackedMessages.notify()
        }
      }
    }
  }

  class InternalRebalanceListener (dataChannel: DataChannel, customRebalanceListener: Option[ConsumerRebalanceListener])
      extends ConsumerRebalanceListener {

    override def beforeReleasingPartitions(partitionOwnership: java.util.Map[String, java.util.Set[java.lang.Integer]]) {
      info("Clearing data channel.")
      dataChannel.clear()
      info("Waiting until all the messages are acked.")
      numUnackedMessages synchronized {
        waitingForMessageAcks = true
        while (numUnackedMessages.get() > 0) {
          try {
            numUnackedMessages.wait()
          } catch {
            case e: InterruptedException => info("Ignoring interrupt while waiting.")
          }
        }
        waitingForMessageAcks = false
      }
      info("Committing offsets.")
      commitOffsets()

      // invoke custom consumer rebalance listener
      if (customRebalanceListener.isDefined)
        customRebalanceListener.get.beforeReleasingPartitions(partitionOwnership)
    }
  }

  private[kafka] class MirrorMakerRecord (val sourceTopic: String,
                                          val sourcePartition: Int,
                                          val sourceOffset: Long,
                                          val key: Array[Byte],
                                          val value: Array[Byte]) {
    def size = {if (value == null) 0 else value.length} + {if (key == null) 0 else key.length}
  }

  private class UnackedOffset(offset: Long) extends DoublyLinkedListNode[Long](offset) {

  }

  private class UnackedOffsets {
    val offsetList = new DoublyLinkedList[Long]
    var maxOffsetSeen: Long = -1L

    def maybeUpdateMaxOffsetSeen(offset: Long) {
      this synchronized {
        maxOffsetSeen = math.max(maxOffsetSeen, offset)
      }
    }

    def addOffset(offset: DoublyLinkedListNode[Long]) {
      this synchronized {
        offsetList.add(offset)
        maybeUpdateMaxOffsetSeen(offset.element)
      }
    }

    def removeOffset(offset: DoublyLinkedListNode[Long]) {
      offsetList.remove(offset)
    }

    def getOffsetToCommit: Long = {
      this synchronized {
        val smallestUnackedOffset = offsetList.peek()
        if (smallestUnackedOffset == null)
          // list is empty, commit maxOffsetSeen + 1
          maxOffsetSeen + 1
        else
          // commit the smallest unacked offset
          smallestUnackedOffset.element
      }
    }

    def size: Int = offsetList.size
  }
}