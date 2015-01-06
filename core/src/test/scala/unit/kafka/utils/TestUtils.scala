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

package kafka.utils

import java.io._
import java.net._
import java.nio._
import java.nio.channels._
import java.util.Random
import java.util.Properties

import org.apache.kafka.common.utils.Utils._

import collection.mutable.ListBuffer

import org.I0Itec.zkclient.ZkClient

import kafka.server._
import kafka.producer._
import kafka.message._
import kafka.api._
import kafka.cluster.Broker
import kafka.consumer.{KafkaStream, ConsumerConfig}
import kafka.serializer.{StringEncoder, DefaultEncoder, Encoder}
import kafka.common.TopicAndPartition
import kafka.admin.AdminUtils
import kafka.producer.ProducerConfig
import kafka.log._

import junit.framework.AssertionFailedError
import junit.framework.Assert._
import org.apache.kafka.clients.producer.KafkaProducer

import scala.collection.Map

/**
 * Utility functions to help with testing
 */
object TestUtils extends Logging {

  val IoTmpDir = System.getProperty("java.io.tmpdir")

  val Letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
  val Digits = "0123456789"
  val LettersAndDigits = Letters + Digits

  /* A consistent random number generator to make tests repeatable */
  val seededRandom = new Random(192348092834L)
  val random = new Random()

  /**
   * Choose a number of random available ports
   */
  def choosePorts(count: Int): List[Int] = {
    val sockets =
      for(i <- 0 until count)
      yield new ServerSocket(0)
    val socketList = sockets.toList
    val ports = socketList.map(_.getLocalPort)
    socketList.map(_.close)
    ports
  }

  /**
   * Choose an available port
   */
  def choosePort(): Int = choosePorts(1).head

  /**
   * Create a temporary directory
   */
  def tempDir(): File = {
    val f = new File(IoTmpDir, "kafka-" + random.nextInt(1000000))
    f.mkdirs()
    f.deleteOnExit()

    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        Utils.rm(f)
      }
    })
    
    f
  }

  def tempTopic(): String = "testTopic" + random.nextInt(1000000)

  /**
   * Create a temporary relative directory
   */
  def tempRelativeDir(parent: String): File = {
    val f = new File(parent, "kafka-" + random.nextInt(1000000))
    f.mkdirs()
    f.deleteOnExit()
    f
  }

  /**
   * Create a temporary file
   */
  def tempFile(): File = {
    val f = File.createTempFile("kafka", ".tmp")
    f.deleteOnExit()
    f
  }

  /**
   * Create a temporary file and return an open file channel for this file
   */
  def tempChannel(): FileChannel = new RandomAccessFile(tempFile(), "rw").getChannel()

  /**
   * Create a kafka server instance with appropriate test settings
   * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
   * @param config The configuration of the server
   */
  def createServer(config: KafkaConfig, time: Time = SystemTime): KafkaServer = {
    val server = new KafkaServer(config, time)
    server.startup()
    server
  }

  /**
   * Create a test config for the given node id
   */
  def createBrokerConfigs(numConfigs: Int,
    enableControlledShutdown: Boolean = true): List[Properties] = {
    for((port, node) <- choosePorts(numConfigs).zipWithIndex)
    yield createBrokerConfig(node, port, enableControlledShutdown)
  }

  def getBrokerListStrFromConfigs(configs: Seq[KafkaConfig]): String = {
    configs.map(c => formatAddress(c.hostName, c.port)).mkString(",")
  }

  /**
   * Create a test config for the given node id
   */
  def createBrokerConfig(nodeId: Int, port: Int = choosePort(),
    enableControlledShutdown: Boolean = true): Properties = {
    val props = new Properties
    props.put("broker.id", nodeId.toString)
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("log.dir", TestUtils.tempDir().getAbsolutePath)
    props.put("zookeeper.connect", TestZKUtils.zookeeperConnect)
    props.put("replica.socket.timeout.ms", "1500")
    props.put("controlled.shutdown.enable", enableControlledShutdown.toString)
    props
  }

  /**
   * Create a topic in zookeeper.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(zkClient: ZkClient,
                  topic: String,
                  numPartitions: Int = 1,
                  replicationFactor: Int = 1,
                  servers: Seq[KafkaServer],
                  topicConfig: Properties = new Properties) : scala.collection.immutable.Map[Int, Option[Int]] = {
    // create topic
    AdminUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, topicConfig)
    // wait until the update metadata request for new topic reaches all servers
    (0 until numPartitions).map { case i =>
      TestUtils.waitUntilMetadataIsPropagated(servers, topic, i)
      i -> TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i)
    }.toMap
  }

  /**
   * Create a topic in zookeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(zkClient: ZkClient, topic: String, partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
                  servers: Seq[KafkaServer]) : scala.collection.immutable.Map[Int, Option[Int]] = {
    // create topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkClient, topic, partitionReplicaAssignment)
    // wait until the update metadata request for new topic reaches all servers
    partitionReplicaAssignment.keySet.map { case i =>
      TestUtils.waitUntilMetadataIsPropagated(servers, topic, i)
      i -> TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, i)
    }.toMap
  }

  /**
   * Create a test config for a consumer
   */
  def createConsumerProperties(zkConnect: String, groupId: String, consumerId: String,
                               consumerTimeout: Long = -1): Properties = {
    val props = new Properties
    props.put("zookeeper.connect", zkConnect)
    props.put("group.id", groupId)
    props.put("consumer.id", consumerId)
    props.put("consumer.timeout.ms", consumerTimeout.toString)
    props.put("zookeeper.session.timeout.ms", "6000")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    props.put("rebalance.max.retries", "4")
    props.put("auto.offset.reset", "smallest")
    props.put("num.consumer.fetchers", "2")

    props
  }

  /**
   * Wrap the message in a message set
   * @param payload The bytes of the message
   */
  def singleMessageSet(payload: Array[Byte], codec: CompressionCodec = NoCompressionCodec, key: Array[Byte] = null) =
    new ByteBufferMessageSet(compressionCodec = codec, messages = new Message(payload, key))

  /**
   * Generate an array of random bytes
   * @param numBytes The size of the array
   */
  def randomBytes(numBytes: Int): Array[Byte] = {
    val bytes = new Array[Byte](numBytes)
    seededRandom.nextBytes(bytes)
    bytes
  }

  /**
   * Generate a random string of letters and digits of the given length
   * @param len The length of the string
   * @return The random string
   */
  def randomString(len: Int): String = {
    val b = new StringBuilder()
    for(i <- 0 until len)
      b.append(LettersAndDigits.charAt(seededRandom.nextInt(LettersAndDigits.length)))
    b.toString
  }

  /**
   * Check that the buffer content from buffer.position() to buffer.limit() is equal
   */
  def checkEquals(b1: ByteBuffer, b2: ByteBuffer) {
    assertEquals("Buffers should have equal length", b1.limit - b1.position, b2.limit - b2.position)
    for(i <- 0 until b1.limit - b1.position)
      assertEquals("byte " + i + " byte not equal.", b1.get(b1.position + i), b2.get(b1.position + i))
  }

  /**
   * Throw an exception if the two iterators are of differing lengths or contain
   * different messages on their Nth element
   */
  def checkEquals[T](expected: Iterator[T], actual: Iterator[T]) {
    var length = 0
    while(expected.hasNext && actual.hasNext) {
      length += 1
      assertEquals(expected.next, actual.next)
    }

    // check if the expected iterator is longer
    if (expected.hasNext) {
      var length1 = length;
      while (expected.hasNext) {
        expected.next
        length1 += 1
      }
      assertFalse("Iterators have uneven length-- first has more: "+length1 + " > " + length, true);
    }

    // check if the actual iterator was longer
    if (actual.hasNext) {
      var length2 = length;
      while (actual.hasNext) {
        actual.next
        length2 += 1
      }
      assertFalse("Iterators have uneven length-- second has more: "+length2 + " > " + length, true);
    }
  }

  /**
   *  Throw an exception if an iterable has different length than expected
   *
   */
  def checkLength[T](s1: Iterator[T], expectedLength:Int) {
    var n = 0
    while (s1.hasNext) {
      n+=1
      s1.next
    }
    assertEquals(expectedLength, n)
  }

  /**
   * Throw an exception if the two iterators are of differing lengths or contain
   * different messages on their Nth element
   */
  def checkEquals[T](s1: java.util.Iterator[T], s2: java.util.Iterator[T]) {
    while(s1.hasNext && s2.hasNext)
      assertEquals(s1.next, s2.next)
    assertFalse("Iterators have uneven length--first has more", s1.hasNext)
    assertFalse("Iterators have uneven length--second has more", s2.hasNext)
  }

  def stackedIterator[T](s: Iterator[T]*): Iterator[T] = {
    new Iterator[T] {
      var cur: Iterator[T] = null
      val topIterator = s.iterator

      def hasNext() : Boolean = {
        while (true) {
          if (cur == null) {
            if (topIterator.hasNext)
              cur = topIterator.next
            else
              return false
          }
          if (cur.hasNext)
            return true
          cur = null
        }
        // should never reach her
        throw new RuntimeException("should not reach here")
      }

      def next() : T = cur.next
    }
  }

  /**
   * Create a hexidecimal string for the given bytes
   */
  def hexString(bytes: Array[Byte]): String = hexString(ByteBuffer.wrap(bytes))

  /**
   * Create a hexidecimal string for the given bytes
   */
  def hexString(buffer: ByteBuffer): String = {
    val builder = new StringBuilder("0x")
    for(i <- 0 until buffer.limit)
      builder.append(String.format("%x", Integer.valueOf(buffer.get(buffer.position + i))))
    builder.toString
  }

  /**
   * Create a producer with a few pre-configured properties.
   * If certain properties need to be overridden, they can be provided in producerProps.
   */
  def createProducer[K, V](brokerList: String,
                           encoder: String = classOf[DefaultEncoder].getName,
                           keyEncoder: String = classOf[DefaultEncoder].getName,
                           partitioner: String = classOf[DefaultPartitioner].getName,
                           producerProps: Properties = null): Producer[K, V] = {
    val props: Properties = getProducerConfig(brokerList)

    //override any explicitly specified properties
    if (producerProps != null)
      props.putAll(producerProps)

    props.put("serializer.class", encoder)
    props.put("key.serializer.class", keyEncoder)
    props.put("partitioner.class", partitioner)
    new Producer[K, V](new ProducerConfig(props))
  }

  /**
   * Create a (new) producer with a few pre-configured properties.
   */
  def createNewProducer(brokerList: String,
                        acks: Int = -1,
                        metadataFetchTimeout: Long = 3000L,
                        blockOnBufferFull: Boolean = true,
                        bufferSize: Long = 1024L * 1024L,
                        retries: Int = 0) : KafkaProducer[Array[Byte],Array[Byte]] = {
    import org.apache.kafka.clients.producer.ProducerConfig

    val producerProps = new Properties()
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, acks.toString)
    producerProps.put(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG, metadataFetchTimeout.toString)
    producerProps.put(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG, blockOnBufferFull.toString)
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
    producerProps.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    producerProps.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "100")
    producerProps.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG, "200")
    producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.ByteArraySerializer")
    return new KafkaProducer[Array[Byte],Array[Byte]](producerProps)
  }

  /**
   * Create a default producer config properties map with the given metadata broker list
   */
  def getProducerConfig(brokerList: String): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("message.send.max.retries", "5")
    props.put("retry.backoff.ms", "1000")
    props.put("request.timeout.ms", "2000")
    props.put("request.required.acks", "-1")
    props.put("send.buffer.bytes", "65536")
    props.put("connect.timeout.ms", "100000")
    props.put("reconnect.interval", "10000")

    props
  }

  def getSyncProducerConfig(port: Int): Properties = {
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port.toString)
    props.put("request.timeout.ms", "500")
    props.put("request.required.acks", "1")
    props.put("serializer.class", classOf[StringEncoder].getName)
    props
  }

  def updateConsumerOffset(config : ConsumerConfig, path : String, offset : Long) = {
    val zkClient = new ZkClient(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, ZKStringSerializer)
    ZkUtils.updatePersistentPath(zkClient, path, offset.toString)

  }

  def getMessageIterator(iter: Iterator[MessageAndOffset]): Iterator[Message] = {
    new IteratorTemplate[Message] {
      override def makeNext(): Message = {
        if (iter.hasNext)
          return iter.next.message
        else
          return allDone()
      }
    }
  }

  def createBrokersInZk(zkClient: ZkClient, ids: Seq[Int]): Seq[Broker] = {
    val brokers = ids.map(id => new Broker(id, "localhost", 6667))
    brokers.foreach(b => ZkUtils.registerBrokerInZk(zkClient, b.id, b.host, b.port, 6000, jmxPort = -1))
    brokers
  }

  def deleteBrokersInZk(zkClient: ZkClient, ids: Seq[Int]): Seq[Broker] = {
    val brokers = ids.map(id => new Broker(id, "localhost", 6667))
    brokers.foreach(b => ZkUtils.deletePath(zkClient, ZkUtils.BrokerIdsPath + "/" + b))
    brokers
  }

  def getMsgStrings(n: Int): Seq[String] = {
    val buffer = new ListBuffer[String]
    for (i <- 0 until  n)
      buffer += ("msg" + i)
    buffer
  }

  /**
   * Create a wired format request based on simple basic information
   */
  def produceRequest(topic: String,
                     partition: Int,
                     message: ByteBufferMessageSet,
                     acks: Int = SyncProducerConfig.DefaultRequiredAcks,
                     timeout: Int = SyncProducerConfig.DefaultAckTimeoutMs,
                     correlationId: Int = 0,
                     clientId: String = SyncProducerConfig.DefaultClientId): ProducerRequest = {
    produceRequestWithAcks(Seq(topic), Seq(partition), message, acks, timeout, correlationId, clientId)
  }

  def produceRequestWithAcks(topics: Seq[String],
                             partitions: Seq[Int],
                             message: ByteBufferMessageSet,
                             acks: Int = SyncProducerConfig.DefaultRequiredAcks,
                             timeout: Int = SyncProducerConfig.DefaultAckTimeoutMs,
                             correlationId: Int = 0,
                             clientId: String = SyncProducerConfig.DefaultClientId): ProducerRequest = {
    val data = topics.flatMap(topic =>
      partitions.map(partition => (TopicAndPartition(topic,  partition), message))
    )
    new ProducerRequest(correlationId, clientId, acks.toShort, timeout, collection.mutable.Map(data:_*))
  }

  def makeLeaderForPartition(zkClient: ZkClient, topic: String,
                             leaderPerPartitionMap: scala.collection.immutable.Map[Int, Int],
                             controllerEpoch: Int) {
    leaderPerPartitionMap.foreach
    {
      leaderForPartition => {
        val partition = leaderForPartition._1
        val leader = leaderForPartition._2
        try{
          val currentLeaderAndIsrOpt = ZkUtils.getLeaderAndIsrForPartition(zkClient, topic, partition)
          var newLeaderAndIsr: LeaderAndIsr = null
          if(currentLeaderAndIsrOpt == None)
            newLeaderAndIsr = new LeaderAndIsr(leader, List(leader))
          else{
            newLeaderAndIsr = currentLeaderAndIsrOpt.get
            newLeaderAndIsr.leader = leader
            newLeaderAndIsr.leaderEpoch += 1
            newLeaderAndIsr.zkVersion += 1
          }
          ZkUtils.updatePersistentPath(zkClient, ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition),
            ZkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch))
        } catch {
          case oe: Throwable => error("Error while electing leader for partition [%s,%d]".format(topic, partition), oe)
        }
      }
    }
  }

  /**
   *  If neither oldLeaderOpt nor newLeaderOpt is defined, wait until the leader of a partition is elected.
   *  If oldLeaderOpt is defined, it waits until the new leader is different from the old leader.
   *  If newLeaderOpt is defined, it waits until the new leader becomes the expected new leader.
   * @return The new leader or assertion failure if timeout is reached.
   */
  def waitUntilLeaderIsElectedOrChanged(zkClient: ZkClient, topic: String, partition: Int, timeoutMs: Long = 5000L,
                                        oldLeaderOpt: Option[Int] = None, newLeaderOpt: Option[Int] = None): Option[Int] = {
    require(!(oldLeaderOpt.isDefined && newLeaderOpt.isDefined), "Can't define both the old and the new leader")
    val startTime = System.currentTimeMillis()
    var isLeaderElectedOrChanged = false

    trace("Waiting for leader to be elected or changed for partition [%s,%d], older leader is %s, new leader is %s"
          .format(topic, partition, oldLeaderOpt, newLeaderOpt))

    var leader: Option[Int] = None
    while (!isLeaderElectedOrChanged && System.currentTimeMillis() < startTime + timeoutMs) {
      // check if leader is elected
      leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
      leader match {
        case Some(l) =>
          if (newLeaderOpt.isDefined && newLeaderOpt.get == l) {
            trace("Expected new leader %d is elected for partition [%s,%d]".format(l, topic, partition))
            isLeaderElectedOrChanged = true
          } else if (oldLeaderOpt.isDefined && oldLeaderOpt.get != l) {
            trace("Leader for partition [%s,%d] is changed from %d to %d".format(topic, partition, oldLeaderOpt.get, l))
            isLeaderElectedOrChanged = true
          } else if (!oldLeaderOpt.isDefined) {
            trace("Leader %d is elected for partition [%s,%d]".format(l, topic, partition))
            isLeaderElectedOrChanged = true
          } else {
            trace("Current leader for partition [%s,%d] is %d".format(topic, partition, l))
          }
        case None =>
          trace("Leader for partition [%s,%d] is not elected yet".format(topic, partition))
      }
      Thread.sleep(timeoutMs.min(100L))
    }
    if (!isLeaderElectedOrChanged)
      fail("Timing out after %d ms since leader is not elected or changed for partition [%s,%d]"
           .format(timeoutMs, topic, partition))

    return leader
  }

  /**
   * Execute the given block. If it throws an assert error, retry. Repeat
   * until no error is thrown or the time limit ellapses
   */
  def retry(maxWaitMs: Long)(block: => Unit) {
    var wait = 1L
    val startTime = System.currentTimeMillis()
    while(true) {
      try {
        block
        return
      } catch {
        case e: AssertionFailedError =>
          val ellapsed = System.currentTimeMillis - startTime
          if(ellapsed > maxWaitMs) {
            throw e
          } else {
            info("Attempt failed, sleeping for " + wait + ", and then retrying.")
            Thread.sleep(wait)
            wait += math.min(wait, 1000)
          }
      }
    }
  }

  /**
   * Wait until the given condition is true or throw an exception if the given wait time elapses.
   */
  def waitUntilTrue(condition: () => Boolean, msg: String, waitTime: Long = 5000L): Boolean = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return true
      if (System.currentTimeMillis() > startTime + waitTime)
        fail(msg)
      Thread.sleep(waitTime.min(100L))
    }
    // should never hit here
    throw new RuntimeException("unexpected error")
  }

  def isLeaderLocalOnBroker(topic: String, partitionId: Int, server: KafkaServer): Boolean = {
    val partitionOpt = server.replicaManager.getPartition(topic, partitionId)
    partitionOpt match {
      case None => false
      case Some(partition) =>
        val replicaOpt = partition.leaderReplicaIfLocal
        replicaOpt match {
          case None => false
          case Some(_) => true
        }
    }
  }

  def createRequestByteBuffer(request: RequestOrResponse): ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(request.sizeInBytes + 2)
    byteBuffer.putShort(request.requestId.get)
    request.writeTo(byteBuffer)
    byteBuffer.rewind()
    byteBuffer
  }


  /**
   * Wait until a valid leader is propagated to the metadata cache in each broker.
   * It assumes that the leader propagated to each broker is the same.
   * @param servers The list of servers that the metadata should reach to
   * @param topic The topic name
   * @param partition The partition Id
   * @param timeout The amount of time waiting on this condition before assert to fail
   * @return The leader of the partition.
   */
  def waitUntilMetadataIsPropagated(servers: Seq[KafkaServer], topic: String, partition: Int, timeout: Long = 5000L): Int = {
    var leader: Int = -1
    TestUtils.waitUntilTrue(() =>
      servers.foldLeft(true) {
        (result, server) =>
          val partitionStateOpt = server.apis.metadataCache.getPartitionInfo(topic, partition)
          partitionStateOpt match {
            case None => false
            case Some(partitionState) =>
              leader = partitionState.leaderIsrAndControllerEpoch.leaderAndIsr.leader
              result && Request.isValidBrokerId(leader)
          }
      },
      "Partition [%s,%d] metadata not propagated after %d ms".format(topic, partition, timeout),
      waitTime = timeout)

    leader
  }

  def writeNonsenseToFile(fileName: File, position: Long, size: Int) {
    val file = new RandomAccessFile(fileName, "rw")
    file.seek(position)
    for(i <- 0 until size)
      file.writeByte(random.nextInt(255))
    file.close()
  }

  def appendNonsenseToFile(fileName: File, size: Int) {
    val file = new FileOutputStream(fileName, true)
    for(i <- 0 until size)
      file.write(random.nextInt(255))
    file.close()
  }

  def checkForPhantomInSyncReplicas(zkClient: ZkClient, topic: String, partitionToBeReassigned: Int, assignedReplicas: Seq[Int]) {
    val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    // in sync replicas should not have any replica that is not in the new assigned replicas
    val phantomInSyncReplicas = inSyncReplicas.toSet -- assignedReplicas.toSet
    assertTrue("All in sync replicas %s must be in the assigned replica list %s".format(inSyncReplicas, assignedReplicas),
      phantomInSyncReplicas.size == 0)
  }

  def ensureNoUnderReplicatedPartitions(zkClient: ZkClient, topic: String, partitionToBeReassigned: Int, assignedReplicas: Seq[Int],
                                                servers: Seq[KafkaServer]) {
    TestUtils.waitUntilTrue(() => {
        val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionToBeReassigned)
        inSyncReplicas.size == assignedReplicas.size
      },
      "Reassigned partition [%s,%d] is under replicated".format(topic, partitionToBeReassigned))
    var leader: Option[Int] = None
    TestUtils.waitUntilTrue(() => {
        leader = ZkUtils.getLeaderForPartition(zkClient, topic, partitionToBeReassigned)
        leader.isDefined
      },
      "Reassigned partition [%s,%d] is unavailable".format(topic, partitionToBeReassigned))
    TestUtils.waitUntilTrue(() => {
        val leaderBroker = servers.filter(s => s.config.brokerId == leader.get).head
        leaderBroker.replicaManager.underReplicatedPartitionCount() == 0
      },
      "Reassigned partition [%s,%d] is under-replicated as reported by the leader %d".format(topic, partitionToBeReassigned, leader.get))
  }

  def checkIfReassignPartitionPathExists(zkClient: ZkClient): Boolean = {
    ZkUtils.pathExists(zkClient, ZkUtils.ReassignPartitionsPath)
  }


  /**
   * Create new LogManager instance with default configuration for testing
   */
  def createLogManager(
    logDirs: Array[File] = Array.empty[File],
    defaultConfig: LogConfig = LogConfig(),
    cleanerConfig: CleanerConfig = CleanerConfig(enableCleaner = false),
    time: MockTime = new MockTime()) =
  {
    new LogManager(
      logDirs = logDirs,
      topicConfigs = Map(),
      defaultConfig = defaultConfig,
      cleanerConfig = cleanerConfig,
      ioThreads = 4,
      flushCheckMs = 1000L,
      flushCheckpointMs = 10000L,
      retentionCheckMs = 1000L,
      scheduler = time.scheduler,
      time = time,
      brokerState = new BrokerState())
  }

  def sendMessagesToPartition(configs: Seq[KafkaConfig],
                              topic: String,
                              partition: Int,
                              numMessages: Int,
                              compression: CompressionCodec = NoCompressionCodec): List[String] = {
    val header = "test-%d".format(partition)
    val props = new Properties()
    props.put("compression.codec", compression.codec.toString)
    val producer: Producer[Int, String] =
      createProducer(TestUtils.getBrokerListStrFromConfigs(configs),
        encoder = classOf[StringEncoder].getName,
        keyEncoder = classOf[IntEncoder].getName,
        partitioner = classOf[FixedValuePartitioner].getName,
        producerProps = props)

    val ms = 0.until(numMessages).map(x => header + "-" + x)
    producer.send(ms.map(m => new KeyedMessage[Int, String](topic, partition, m)):_*)
    debug("Sent %d messages for partition [%s,%d]".format(ms.size, topic, partition))
    producer.close()
    ms.toList
  }

  def sendMessages(configs: Seq[KafkaConfig],
                   topic: String,
                   producerId: String,
                   messagesPerNode: Int,
                   header: String,
                   compression: CompressionCodec,
                   numParts: Int): List[String]= {
    var messages: List[String] = Nil
    val props = new Properties()
    props.put("compression.codec", compression.codec.toString)
    props.put("client.id", producerId)
    val   producer: Producer[Int, String] =
      createProducer(brokerList = TestUtils.getBrokerListStrFromConfigs(configs),
        encoder = classOf[StringEncoder].getName,
        keyEncoder = classOf[IntEncoder].getName,
        partitioner = classOf[FixedValuePartitioner].getName,
        producerProps = props)

    for (partition <- 0 until numParts) {
      val ms = 0.until(messagesPerNode).map(x => header + "-" + partition + "-" + x)
      producer.send(ms.map(m => new KeyedMessage[Int, String](topic, partition, m)):_*)
      messages ++= ms
      debug("Sent %d messages for partition [%s,%d]".format(ms.size, topic, partition))
    }
    producer.close()
    messages
  }

  def getMessages(nMessagesPerThread: Int,
                  topicMessageStreams: Map[String, List[KafkaStream[String, String]]]): List[String] = {
    var messages: List[String] = Nil
    for ((topic, messageStreams) <- topicMessageStreams) {
      for (messageStream <- messageStreams) {
        val iterator = messageStream.iterator
        for (i <- 0 until nMessagesPerThread) {
          assertTrue(iterator.hasNext)
          val message = iterator.next.message
          messages ::= message
          debug("received message: " + message)
        }
      }
    }
    messages.reverse
  }
}

object TestZKUtils {
  val zookeeperConnect = "127.0.0.1:" + TestUtils.choosePort()
}

class IntEncoder(props: VerifiableProperties = null) extends Encoder[Int] {
  override def toBytes(n: Int) = n.toString.getBytes
}

class StaticPartitioner(props: VerifiableProperties = null) extends Partitioner{
  def partition(data: Any, numPartitions: Int): Int = {
    (data.asInstanceOf[String].length % numPartitions)
  }
}

class HashPartitioner(props: VerifiableProperties = null) extends Partitioner {
  def partition(data: Any, numPartitions: Int): Int = {
    (data.hashCode % numPartitions)
  }
}

class FixedValuePartitioner(props: VerifiableProperties = null) extends Partitioner {
  def partition(data: Any, numPartitions: Int): Int = data.asInstanceOf[Int]
}
