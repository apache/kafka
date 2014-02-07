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
import junit.framework.AssertionFailedError
import junit.framework.Assert._
import kafka.server._
import kafka.producer._
import kafka.message._
import org.I0Itec.zkclient.ZkClient
import kafka.cluster.Broker
import collection.mutable.ListBuffer
import kafka.consumer.ConsumerConfig
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.TimeUnit
import kafka.api._
import collection.mutable.Map
import kafka.serializer.{StringEncoder, DefaultEncoder, Encoder}
import kafka.common.TopicAndPartition
import junit.framework.Assert


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
  def createBrokerConfigs(numConfigs: Int): List[Properties] = {
    for((port, node) <- choosePorts(numConfigs).zipWithIndex)
    yield createBrokerConfig(node, port)
  }

  def getBrokerListStrFromConfigs(configs: Seq[KafkaConfig]): String = {
    configs.map(c => c.hostName + ":" + c.port).mkString(",")
  }

  /**
   * Create a test config for the given node id
   */
  def createBrokerConfig(nodeId: Int, port: Int = choosePort()): Properties = {
    val props = new Properties
    props.put("broker.id", nodeId.toString)
    props.put("host.name", "localhost")
    props.put("port", port.toString)
    props.put("log.dir", TestUtils.tempDir().getAbsolutePath)
    props.put("zookeeper.connect", TestZKUtils.zookeeperConnect)
    props.put("replica.socket.timeout.ms", "1500")
    props
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
    props.put("zookeeper.session.timeout.ms", "400")
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
   * Create a producer for the given host and port
   */
  def createProducer[K, V](brokerList: String, 
                           encoder: Encoder[V] = new DefaultEncoder(), 
                           keyEncoder: Encoder[K] = new DefaultEncoder()): Producer[K, V] = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("send.buffer.bytes", "65536")
    props.put("connect.timeout.ms", "100000")
    props.put("reconnect.interval", "10000")
    props.put("serializer.class", encoder.getClass.getCanonicalName)
    props.put("key.serializer.class", keyEncoder.getClass.getCanonicalName)
    new Producer[K, V](new ProducerConfig(props))
  }

  def getProducerConfig(brokerList: String, partitioner: String = "kafka.producer.DefaultPartitioner"): Properties = {
    val props = new Properties()
    props.put("metadata.broker.list", brokerList)
    props.put("partitioner.class", partitioner)
    props.put("message.send.max.retries", "3")
    props.put("retry.backoff.ms", "1000")
    props.put("request.timeout.ms", "500")
    props.put("request.required.acks", "-1")
    props.put("serializer.class", classOf[StringEncoder].getName.toString)

    props
  }

  def getSyncProducerConfig(port: Int): Properties = {
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port.toString)
    props.put("request.timeout.ms", "500")
    props.put("request.required.acks", "1")
    props.put("serializer.class", classOf[StringEncoder].getName.toString)
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
    new ProducerRequest(correlationId, clientId, acks.toShort, timeout, Map(data:_*))
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

  def waitUntilLeaderIsElectedOrChanged(zkClient: ZkClient, topic: String, partition: Int, timeoutMs: Long, oldLeaderOpt: Option[Int] = None): Option[Int] = {
    val leaderLock = new ReentrantLock()
    val leaderExistsOrChanged = leaderLock.newCondition()

    if(oldLeaderOpt == None)
      info("Waiting for leader to be elected for partition [%s,%d]".format(topic, partition))
    else
      info("Waiting for leader for partition [%s,%d] to be changed from old leader %d".format(topic, partition, oldLeaderOpt.get))

    leaderLock.lock()
    try {
      zkClient.subscribeDataChanges(ZkUtils.getTopicPartitionLeaderAndIsrPath(topic, partition), new LeaderExistsOrChangedListener(topic, partition, leaderLock, leaderExistsOrChanged, oldLeaderOpt, zkClient))
      leaderExistsOrChanged.await(timeoutMs, TimeUnit.MILLISECONDS)
      // check if leader is elected
      val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partition)
      leader match {
        case Some(l) =>
          if(oldLeaderOpt == None)
            info("Leader %d is elected for partition [%s,%d]".format(l, topic, partition))
          else
            info("Leader for partition [%s,%d] is changed from %d to %d".format(topic, partition, oldLeaderOpt.get, l))
        case None => error("Timing out after %d ms since leader is not elected for partition [%s,%d]"
                                   .format(timeoutMs, topic, partition))
      }
      leader
    } finally {
      leaderLock.unlock()
    }
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
   * Wait until the given condition is true or the given wait time ellapses
   */
  def waitUntilTrue(condition: () => Boolean, waitTime: Long): Boolean = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return true
      if (System.currentTimeMillis() > startTime + waitTime)
        return false
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

  def waitUntilMetadataIsPropagated(servers: Seq[KafkaServer], topic: String, partition: Int, timeout: Long) = {
    Assert.assertTrue("Partition [%s,%d] metadata not propagated after timeout".format(topic, partition),
      TestUtils.waitUntilTrue(() =>
        servers.foldLeft(true)(_ && _.apis.metadataCache.keySet.contains(TopicAndPartition(topic, partition))), timeout))
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
    val inSyncReplicas = ZkUtils.getInSyncReplicasForPartition(zkClient, topic, partitionToBeReassigned)
    assertFalse("Reassigned partition [%s,%d] is underreplicated".format(topic, partitionToBeReassigned),
      inSyncReplicas.size < assignedReplicas.size)
    val leader = ZkUtils.getLeaderForPartition(zkClient, topic, partitionToBeReassigned)
    assertTrue("Reassigned partition [%s,%d] is unavailable".format(topic, partitionToBeReassigned), leader.isDefined)
    val leaderBroker = servers.filter(s => s.config.brokerId == leader.get).head
    assertTrue("Reassigned partition [%s,%d] is underreplicated as reported by the leader %d".format(topic, partitionToBeReassigned, leader.get),
      leaderBroker.replicaManager.underReplicatedPartitionCount() == 0)
  }

  def checkIfReassignPartitionPathExists(zkClient: ZkClient): Boolean = {
    ZkUtils.pathExists(zkClient, ZkUtils.ReassignPartitionsPath)
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
