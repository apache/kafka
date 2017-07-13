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
import java.nio._
import java.nio.channels._
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.attribute.BasicFileAttributes
import java.nio.file._
import java.security.cert.X509Certificate
import java.util.{Collections, Properties}
import java.util.concurrent.{Callable, Executors, TimeUnit}
import javax.net.ssl.X509TrustManager

import kafka.admin.AdminUtils
import kafka.api._
import kafka.cluster.{Broker, EndPoint}
import kafka.common.TopicAndPartition
import kafka.consumer.{ConsumerConfig, ConsumerTimeoutException, KafkaStream}
import kafka.log._
import kafka.message._
import kafka.producer._
import kafka.security.auth.{Acl, Authorizer, Resource}
import kafka.serializer.{DefaultEncoder, Encoder, StringEncoder}
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpointFile
import kafka.utils.ZkUtils._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.consumer.{ConsumerRecord, KafkaConsumer, OffsetAndMetadata, RangeAssignor}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.record._
import org.apache.kafka.common.serialization.{ByteArraySerializer, Serializer}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.common.utils.Utils._
import org.apache.kafka.test.{TestSslUtils, TestUtils => JTestUtils}
import org.apache.zookeeper.ZooDefs._
import org.apache.zookeeper.data.ACL
import org.junit.Assert._

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.{Map, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.util.Try

/**
 * Utility functions to help with testing
 */
object TestUtils extends Logging {

  val random = JTestUtils.RANDOM

  /* 0 gives a random port; you can then retrieve the assigned port from the Socket object. */
  val RandomPort = 0

  /** Port to use for unit tests that mock/don't require a real ZK server. */
  val MockZkPort = 1
  /** Zookeeper connection string to use for unit tests that mock/don't require a real ZK server. */
  val MockZkConnect = "127.0.0.1:" + MockZkPort

  private val transactionStatusKey = "transactionStatus"
  private val committedValue : Array[Byte] = "committed".getBytes(StandardCharsets.UTF_8)
  private val abortedValue : Array[Byte] = "aborted".getBytes(StandardCharsets.UTF_8)

  /**
   * Create a temporary directory
   */
  def tempDir(): File = JTestUtils.tempDirectory()

  def tempTopic(): String = "testTopic" + random.nextInt(1000000)

  /**
   * Create a temporary relative directory
   */
  def tempRelativeDir(parent: String): File = {
    val parentFile = new File(parent)
    parentFile.mkdirs()

    JTestUtils.tempDirectory(parentFile.toPath, null)
  }

  /**
   * Create a random log directory in the format <string>-<int> used for Kafka partition logs.
   * It is the responsibility of the caller to set up a shutdown hook for deletion of the directory.
   */
  def randomPartitionLogDir(parentDir: File): File = {
    val attempts = 1000
    val f = Iterator.continually(new File(parentDir, "kafka-" + random.nextInt(1000000)))
                                  .take(attempts).find(_.mkdir())
                                  .getOrElse(sys.error(s"Failed to create directory after $attempts attempts"))
    f.deleteOnExit()
    f
  }


  /**
   * Create a temporary file
   */
  def tempFile(): File = JTestUtils.tempFile()

  /**
   * Create a temporary file and return an open file channel for this file
   */
  def tempChannel(): FileChannel = new RandomAccessFile(tempFile(), "rw").getChannel()

  /**
   * Create a kafka server instance with appropriate test settings
   * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
   *
   * @param config The configuration of the server
   */
  def createServer(config: KafkaConfig, time: Time = Time.SYSTEM): KafkaServer = {
    val server = new KafkaServer(config, time)
    server.startup()
    server
  }

  def boundPort(server: KafkaServer, securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int =
    server.boundPort(ListenerName.forSecurityProtocol(securityProtocol))

  def createBroker(id: Int, host: String, port: Int, securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Broker =
    new Broker(id, host, port, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol)

  /**
   * Create a test config for the provided parameters.
   *
   * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
   */
  def createBrokerConfigs(numConfigs: Int,
    zkConnect: String,
    enableControlledShutdown: Boolean = true,
    enableDeleteTopic: Boolean = false,
    interBrokerSecurityProtocol: Option[SecurityProtocol] = None,
    trustStoreFile: Option[File] = None,
    saslProperties: Option[Properties] = None,
    enablePlaintext: Boolean = true,
    enableSsl: Boolean = false,
    enableSaslPlaintext: Boolean = false,
    enableSaslSsl: Boolean = false,
    rackInfo: Map[Int, String] = Map()): Seq[Properties] = {
    (0 until numConfigs).map { node =>
      createBrokerConfig(node, zkConnect, enableControlledShutdown, enableDeleteTopic, RandomPort,
        interBrokerSecurityProtocol, trustStoreFile, saslProperties, enablePlaintext = enablePlaintext, enableSsl = enableSsl,
        enableSaslPlaintext = enableSaslPlaintext, enableSaslSsl = enableSaslSsl, rack = rackInfo.get(node))
    }
  }

  def getBrokerListStrFromServers(servers: Seq[KafkaServer], protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): String = {
    servers.map { s =>
      val listener = s.config.advertisedListeners.find(_.securityProtocol == protocol).getOrElse(
        sys.error(s"Could not find listener with security protocol $protocol"))
      formatAddress(listener.host, boundPort(s, protocol))
    }.mkString(",")
  }

  def bootstrapServers(servers: Seq[KafkaServer], listenerName: ListenerName): String = {
    servers.map { s =>
      val listener = s.config.advertisedListeners.find(_.listenerName == listenerName).getOrElse(
        sys.error(s"Could not find listener with name ${listenerName.value}"))
      formatAddress(listener.host, s.boundPort(listenerName))
    }.mkString(",")
  }

  /**
    * Shutdown `servers` and delete their log directories.
    */
  def shutdownServers(servers: Seq[KafkaServer]) {
    servers.par.foreach { s =>
      s.shutdown()
      CoreUtils.delete(s.config.logDirs)
    }
  }

  /**
    * Create a test config for the provided parameters.
    *
    * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
    */
  def createBrokerConfig(nodeId: Int, zkConnect: String,
    enableControlledShutdown: Boolean = true,
    enableDeleteTopic: Boolean = false,
    port: Int = RandomPort,
    interBrokerSecurityProtocol: Option[SecurityProtocol] = None,
    trustStoreFile: Option[File] = None,
    saslProperties: Option[Properties] = None,
    enablePlaintext: Boolean = true,
    enableSaslPlaintext: Boolean = false, saslPlaintextPort: Int = RandomPort,
    enableSsl: Boolean = false, sslPort: Int = RandomPort,
    enableSaslSsl: Boolean = false, saslSslPort: Int = RandomPort, rack: Option[String] = None)
  : Properties = {

    def shouldEnable(protocol: SecurityProtocol) = interBrokerSecurityProtocol.fold(false)(_ == protocol)

    val protocolAndPorts = ArrayBuffer[(SecurityProtocol, Int)]()
    if (enablePlaintext || shouldEnable(SecurityProtocol.PLAINTEXT))
      protocolAndPorts += SecurityProtocol.PLAINTEXT -> port
    if (enableSsl || shouldEnable(SecurityProtocol.SSL))
      protocolAndPorts += SecurityProtocol.SSL -> sslPort
    if (enableSaslPlaintext || shouldEnable(SecurityProtocol.SASL_PLAINTEXT))
      protocolAndPorts += SecurityProtocol.SASL_PLAINTEXT -> saslPlaintextPort
    if (enableSaslSsl || shouldEnable(SecurityProtocol.SASL_SSL))
      protocolAndPorts += SecurityProtocol.SASL_SSL -> saslSslPort

    val listeners = protocolAndPorts.map { case (protocol, port) =>
      s"${protocol.name}://localhost:$port"
    }.mkString(",")

    val props = new Properties
    if (nodeId >= 0) props.put(KafkaConfig.BrokerIdProp, nodeId.toString)
    props.put(KafkaConfig.ListenersProp, listeners)
    props.put(KafkaConfig.LogDirProp, TestUtils.tempDir().getAbsolutePath)
    props.put(KafkaConfig.ZkConnectProp, zkConnect)
    props.put(KafkaConfig.ZkConnectionTimeoutMsProp, "10000")
    props.put(KafkaConfig.ReplicaSocketTimeoutMsProp, "1500")
    props.put(KafkaConfig.ControllerSocketTimeoutMsProp, "1500")
    props.put(KafkaConfig.ControlledShutdownEnableProp, enableControlledShutdown.toString)
    props.put(KafkaConfig.DeleteTopicEnableProp, enableDeleteTopic.toString)
    props.put(KafkaConfig.LogDeleteDelayMsProp, "1000")
    props.put(KafkaConfig.ControlledShutdownRetryBackoffMsProp, "100")
    props.put(KafkaConfig.LogCleanerDedupeBufferSizeProp, "2097152")
    props.put(KafkaConfig.LogMessageTimestampDifferenceMaxMsProp, Long.MaxValue.toString)
    props.put(KafkaConfig.OffsetsTopicReplicationFactorProp, "1")
    if (!props.containsKey(KafkaConfig.OffsetsTopicPartitionsProp))
      props.put(KafkaConfig.OffsetsTopicPartitionsProp, "5")
    if (!props.containsKey(KafkaConfig.GroupInitialRebalanceDelayMsProp))
      props.put(KafkaConfig.GroupInitialRebalanceDelayMsProp, "0")
    rack.foreach(props.put(KafkaConfig.RackProp, _))

    if (protocolAndPorts.exists { case (protocol, _) => usesSslTransportLayer(protocol) })
      props.putAll(sslConfigs(Mode.SERVER, false, trustStoreFile, s"server$nodeId"))

    if (protocolAndPorts.exists { case (protocol, _) => usesSaslAuthentication(protocol) })
      props.putAll(JaasTestUtils.saslConfigs(saslProperties))

    interBrokerSecurityProtocol.foreach { protocol =>
      props.put(KafkaConfig.InterBrokerSecurityProtocolProp, protocol.name)
    }

    props
  }

  /**
   * Create a topic in zookeeper.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(zkUtils: ZkUtils,
                  topic: String,
                  numPartitions: Int = 1,
                  replicationFactor: Int = 1,
                  servers: Seq[KafkaServer],
                  topicConfig: Properties = new Properties): scala.collection.immutable.Map[Int, Int] = {
    // create topic
    AdminUtils.createTopic(zkUtils, topic, numPartitions, replicationFactor, topicConfig)
    // wait until the update metadata request for new topic reaches all servers
    (0 until numPartitions).map { i =>
      TestUtils.waitUntilMetadataIsPropagated(servers, topic, i)
      i -> TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, i)
    }.toMap
  }

  /**
   * Create a topic in zookeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(zkUtils: ZkUtils, topic: String, partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
                  servers: Seq[KafkaServer]): scala.collection.immutable.Map[Int, Int] = {
    // create topic
    AdminUtils.createOrUpdateTopicPartitionAssignmentPathInZK(zkUtils, topic, partitionReplicaAssignment)
    // wait until the update metadata request for new topic reaches all servers
    partitionReplicaAssignment.keySet.map { case i =>
      TestUtils.waitUntilMetadataIsPropagated(servers, topic, i)
      i -> TestUtils.waitUntilLeaderIsElectedOrChanged(zkUtils, topic, i)
    }.toMap
  }

  /**
    * Create the consumer offsets/group metadata topic and wait until the leader is elected and metadata is propagated
    * to all brokers.
    */
  def createOffsetsTopic(zkUtils: ZkUtils, servers: Seq[KafkaServer]): Unit = {
    val server = servers.head
    createTopic(zkUtils, Topic.GROUP_METADATA_TOPIC_NAME,
      server.config.getInt(KafkaConfig.OffsetsTopicPartitionsProp),
      server.config.getShort(KafkaConfig.OffsetsTopicReplicationFactorProp).toInt,
      servers,
      server.groupCoordinator.offsetsTopicConfigs)
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
   * Fail a test case explicitly. Return Nothing so that we are not constrained by the return type.
   */
  def fail(msg: String): Nothing = throw new AssertionError(msg)

  /**
   * Wrap a single record log buffer.
   */
  def singletonRecords(value: Array[Byte],
                       key: Array[Byte] = null,
                       codec: CompressionType = CompressionType.NONE,
                       timestamp: Long = RecordBatch.NO_TIMESTAMP,
                       magicValue: Byte = RecordBatch.CURRENT_MAGIC_VALUE): MemoryRecords = {
    records(Seq(new SimpleRecord(timestamp, key, value)), magicValue = magicValue, codec = codec)
  }

  def recordsWithValues(magicValue: Byte,
                        codec: CompressionType,
                        values: Array[Byte]*): MemoryRecords = {
    records(values.map(value => new SimpleRecord(value)), magicValue, codec)
  }

  def records(records: Iterable[SimpleRecord],
              magicValue: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
              codec: CompressionType = CompressionType.NONE,
              producerId: Long = RecordBatch.NO_PRODUCER_ID,
              producerEpoch: Short = RecordBatch.NO_PRODUCER_EPOCH,
              sequence: Int = RecordBatch.NO_SEQUENCE,
              baseOffset: Long = 0L): MemoryRecords = {
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, baseOffset,
      System.currentTimeMillis, producerId, producerEpoch, sequence)
    records.foreach(builder.append)
    builder.build()
  }

  /**
   * Generate an array of random bytes
   *
   * @param numBytes The size of the array
   */
  def randomBytes(numBytes: Int): Array[Byte] = JTestUtils.randomBytes(numBytes)

  /**
   * Generate a random string of letters and digits of the given length
   *
   * @param len The length of the string
   * @return The random string
   */
  def randomString(len: Int): String = JTestUtils.randomString(len)

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
      var length1 = length
      while (expected.hasNext) {
        expected.next
        length1 += 1
      }
      assertFalse("Iterators have uneven length-- first has more: "+length1 + " > " + length, true)
    }

    // check if the actual iterator was longer
    if (actual.hasNext) {
      var length2 = length
      while (actual.hasNext) {
        actual.next
        length2 += 1
      }
      assertFalse("Iterators have uneven length-- second has more: "+length2 + " > " + length, true)
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
   * Create a hexadecimal string for the given bytes
   */
  def hexString(bytes: Array[Byte]): String = hexString(ByteBuffer.wrap(bytes))

  /**
   * Create a hexadecimal string for the given bytes
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
  @deprecated("This method has been deprecated and it will be removed in a future release.", "0.10.0.0")
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
    new Producer[K, V](new kafka.producer.ProducerConfig(props))
  }

  private def securityConfigs(mode: Mode,
                              securityProtocol: SecurityProtocol,
                              trustStoreFile: Option[File],
                              certAlias: String,
                              saslProperties: Option[Properties]): Properties = {
    val props = new Properties
    if (usesSslTransportLayer(securityProtocol))
      props.putAll(sslConfigs(mode, securityProtocol == SecurityProtocol.SSL, trustStoreFile, certAlias))

    if (usesSaslAuthentication(securityProtocol))
      props.putAll(JaasTestUtils.saslConfigs(saslProperties))
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
    props
  }

  def producerSecurityConfigs(securityProtocol: SecurityProtocol, trustStoreFile: Option[File], saslProperties: Option[Properties]): Properties =
    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "producer", saslProperties)

  /**
   * Create a (new) producer with a few pre-configured properties.
   */
  def createNewProducer[K, V](brokerList: String,
                        acks: Int = -1,
                        maxBlockMs: Long = 60 * 1000L,
                        bufferSize: Long = 1024L * 1024L,
                        retries: Int = 0,
                        lingerMs: Long = 0,
                        requestTimeoutMs: Long = 10 * 1024L,
                        securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
                        trustStoreFile: Option[File] = None,
                        saslProperties: Option[Properties] = None,
                        keySerializer: Serializer[K] = new ByteArraySerializer,
                        valueSerializer: Serializer[V] = new ByteArraySerializer,
                        props: Option[Properties] = None): KafkaProducer[K, V] = {

    val producerProps = props.getOrElse(new Properties)
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, acks.toString)
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs.toString)
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
    producerProps.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs.toString)

    /* Only use these if not already set */
    val defaultProps = Map(
      ProducerConfig.RETRY_BACKOFF_MS_CONFIG -> "100",
      ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "200",
      ProducerConfig.LINGER_MS_CONFIG -> lingerMs.toString
    )

    defaultProps.foreach { case (key, value) =>
      if (!producerProps.containsKey(key)) producerProps.put(key, value)
    }

    /*
     * It uses CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to determine whether
     * securityConfigs has been invoked already. For example, we need to
     * invoke it before this call in IntegrationTestHarness, otherwise the
     * SSL client auth fails.
     */
    if (!producerProps.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG))
      producerProps.putAll(producerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties))

    new KafkaProducer[K, V](producerProps, keySerializer, valueSerializer)
  }

  def usesSslTransportLayer(securityProtocol: SecurityProtocol): Boolean = securityProtocol match {
    case SecurityProtocol.SSL | SecurityProtocol.SASL_SSL => true
    case _ => false
  }

  def usesSaslAuthentication(securityProtocol: SecurityProtocol): Boolean = securityProtocol match {
    case SecurityProtocol.SASL_PLAINTEXT | SecurityProtocol.SASL_SSL => true
    case _ => false
  }

  def consumerSecurityConfigs(securityProtocol: SecurityProtocol, trustStoreFile: Option[File], saslProperties: Option[Properties]): Properties =
    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "consumer", saslProperties)

  def adminClientSecurityConfigs(securityProtocol: SecurityProtocol, trustStoreFile: Option[File], saslProperties: Option[Properties]): Properties =
    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "admin-client", saslProperties)

  /**
   * Create a new consumer with a few pre-configured properties.
   */
  def createNewConsumer(brokerList: String,
                        groupId: String = "group",
                        autoOffsetReset: String = "earliest",
                        partitionFetchSize: Long = 4096L,
                        partitionAssignmentStrategy: String = classOf[RangeAssignor].getName,
                        sessionTimeout: Int = 30000,
                        securityProtocol: SecurityProtocol,
                        trustStoreFile: Option[File] = None,
                        saslProperties: Option[Properties] = None,
                        props: Option[Properties] = None) : KafkaConsumer[Array[Byte],Array[Byte]] = {
    import org.apache.kafka.clients.consumer.ConsumerConfig

    val consumerProps = props.getOrElse(new Properties())
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    consumerProps.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, partitionFetchSize.toString)

    val defaultProps = Map(
      ConsumerConfig.RETRY_BACKOFF_MS_CONFIG -> "100",
      ConsumerConfig.RECONNECT_BACKOFF_MS_CONFIG -> "200",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.ByteArrayDeserializer",
      ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG -> partitionAssignmentStrategy,
      ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG -> sessionTimeout.toString,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId)

    defaultProps.foreach { case (key, value) =>
      if (!consumerProps.containsKey(key)) consumerProps.put(key, value)
    }

    /*
     * It uses CommonClientConfigs.SECURITY_PROTOCOL_CONFIG to determine whether
     * securityConfigs has been invoked already. For example, we need to
     * invoke it before this call in IntegrationTestHarness, otherwise the
     * SSL client auth fails.
     */
    if(!consumerProps.containsKey(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG))
      consumerProps.putAll(consumerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties))

    new KafkaConsumer[Array[Byte],Array[Byte]](consumerProps)
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

    props
  }

  @deprecated("This method has been deprecated and will be removed in a future release", "0.11.0.0")
  def getSyncProducerConfig(port: Int): Properties = {
    val props = new Properties()
    props.put("host", "localhost")
    props.put("port", port.toString)
    props.put("request.timeout.ms", "10000")
    props.put("request.required.acks", "1")
    props.put("serializer.class", classOf[StringEncoder].getName)
    props
  }


  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def updateConsumerOffset(config : ConsumerConfig, path : String, offset : Long) = {
    val zkUtils = ZkUtils(config.zkConnect, config.zkSessionTimeoutMs, config.zkConnectionTimeoutMs, false)
    zkUtils.updatePersistentPath(path, offset.toString)
    zkUtils.close()

  }

  def getMessageIterator(iter: Iterator[MessageAndOffset]): Iterator[Message] = {
    new IteratorTemplate[Message] {
      override def makeNext(): Message = {
        if (iter.hasNext)
          iter.next.message
        else
          allDone()
      }
    }
  }

  def createBrokersInZk(zkUtils: ZkUtils, ids: Seq[Int]): Seq[Broker] =
    createBrokersInZk(ids.map(kafka.admin.BrokerMetadata(_, None)), zkUtils)

  def createBrokersInZk(brokerMetadatas: Seq[kafka.admin.BrokerMetadata], zkUtils: ZkUtils): Seq[Broker] = {
    val brokers = brokerMetadatas.map { b =>
      val protocol = SecurityProtocol.PLAINTEXT
      val listenerName = ListenerName.forSecurityProtocol(protocol)
      Broker(b.id, Seq(EndPoint("localhost", 6667, listenerName, protocol)), b.rack)
    }
    brokers.foreach(b => zkUtils.registerBrokerInZk(b.id, "localhost", 6667, b.endPoints, jmxPort = -1,
      rack = b.rack, ApiVersion.latestVersion))
    brokers
  }

  def deleteBrokersInZk(zkUtils: ZkUtils, ids: Seq[Int]): Seq[Broker] = {
    val brokers = ids.map(createBroker(_, "localhost", 6667, SecurityProtocol.PLAINTEXT))
    brokers.foreach(b => zkUtils.deletePath(ZkUtils.BrokerIdsPath + "/" + b))
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
  @deprecated("This method has been deprecated and it will be removed in a future release", "0.10.0.0")
  def produceRequest(topic: String,
                     partition: Int,
                     message: ByteBufferMessageSet,
                     acks: Int,
                     timeout: Int,
                     correlationId: Int = 0,
                     clientId: String): ProducerRequest = {
    produceRequestWithAcks(Seq(topic), Seq(partition), message, acks, timeout, correlationId, clientId)
  }

  @deprecated("This method has been deprecated and it will be removed in a future release", "0.10.0.0")
  def produceRequestWithAcks(topics: Seq[String],
                             partitions: Seq[Int],
                             message: ByteBufferMessageSet,
                             acks: Int,
                             timeout: Int,
                             correlationId: Int = 0,
                             clientId: String): ProducerRequest = {
    val data = topics.flatMap(topic =>
      partitions.map(partition => (TopicAndPartition(topic,  partition), message))
    )
    new ProducerRequest(correlationId, clientId, acks.toShort, timeout, collection.mutable.Map(data:_*))
  }

  def makeLeaderForPartition(zkUtils: ZkUtils,
                             topic: String,
                             leaderPerPartitionMap: scala.collection.immutable.Map[Int, Int],
                             controllerEpoch: Int) {
    leaderPerPartitionMap.foreach { case (partition, leader) =>
      try {
        val newLeaderAndIsr = zkUtils.getLeaderAndIsrForPartition(topic, partition)
          .map(_.newLeader(leader))
          .getOrElse(LeaderAndIsr(leader, List(leader)))

        zkUtils.updatePersistentPath(
          getTopicPartitionLeaderAndIsrPath(topic, partition),
          zkUtils.leaderAndIsrZkData(newLeaderAndIsr, controllerEpoch)
        )
      } catch {
        case oe: Throwable => error(s"Error while electing leader for partition [$topic,$partition]", oe)
      }
    }
  }

  /**
   *  If neither oldLeaderOpt nor newLeaderOpt is defined, wait until the leader of a partition is elected.
   *  If oldLeaderOpt is defined, it waits until the new leader is different from the old leader.
   *  If newLeaderOpt is defined, it waits until the new leader becomes the expected new leader.
   *
   * @return The new leader (note that negative values are used to indicate conditions like NoLeader and
   *         LeaderDuringDelete).
   * @throws AssertionError if the expected condition is not true within the timeout.
   */
  def waitUntilLeaderIsElectedOrChanged(zkUtils: ZkUtils, topic: String, partition: Int, timeoutMs: Long = 30000L,
                                        oldLeaderOpt: Option[Int] = None, newLeaderOpt: Option[Int] = None): Int = {
    require(!(oldLeaderOpt.isDefined && newLeaderOpt.isDefined), "Can't define both the old and the new leader")
    val startTime = System.currentTimeMillis()
    val topicPartition = new TopicPartition(topic, partition)

    trace(s"Waiting for leader to be elected or changed for partition $topicPartition, old leader is $oldLeaderOpt, " +
      s"new leader is $newLeaderOpt")

    var leader: Option[Int] = None
    var electedOrChangedLeader: Option[Int] = None
    while (electedOrChangedLeader.isEmpty && System.currentTimeMillis() < startTime + timeoutMs) {
      // check if leader is elected
      leader = zkUtils.getLeaderForPartition(topic, partition)
      leader match {
        case Some(l) => (newLeaderOpt, oldLeaderOpt) match {
          case (Some(newLeader), _) if newLeader == l =>
            trace(s"Expected new leader $l is elected for partition $topicPartition")
            electedOrChangedLeader = leader
          case (_, Some(oldLeader)) if oldLeader != l =>
            trace(s"Leader for partition $topicPartition is changed from $oldLeader to $l")
            electedOrChangedLeader = leader
          case (None, None) =>
            trace(s"Leader $l is elected for partition $topicPartition")
            electedOrChangedLeader = leader
          case _ =>
            trace(s"Current leader for partition $topicPartition is $l")
        }
        case None =>
          trace(s"Leader for partition $topicPartition is not elected yet")
      }
      Thread.sleep(math.min(timeoutMs, 100L))
    }
    electedOrChangedLeader.getOrElse {
      val errorMessage = (newLeaderOpt, oldLeaderOpt) match {
        case (Some(newLeader), _) =>
          s"Timing out after $timeoutMs ms since expected new leader $newLeader was not elected for partition $topicPartition, leader is $leader"
        case (_, Some(oldLeader)) =>
          s"Timing out after $timeoutMs ms since a new leader that is different from $oldLeader was not elected for partition $topicPartition, " +
            s"leader is $leader"
        case _ =>
          s"Timing out after $timeoutMs ms since a leader was not elected for partition $topicPartition"
      }
      fail(errorMessage)
    }
  }

  /**
   * Execute the given block. If it throws an assert error, retry. Repeat
   * until no error is thrown or the time limit elapses
   */
  def retry(maxWaitMs: Long)(block: => Unit) {
    var wait = 1L
    val startTime = System.currentTimeMillis()
    while(true) {
      try {
        block
        return
      } catch {
        case e: AssertionError =>
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
  def waitUntilTrue(condition: () => Boolean, msg: => String,
                    waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS, pause: Long = 100L): Unit = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return
      if (System.currentTimeMillis() > startTime + waitTime)
        fail(msg)
      Thread.sleep(waitTime.min(pause))
    }
    // should never hit here
    throw new RuntimeException("unexpected error")
  }

  /**
    * Invoke `compute` until `predicate` is true or `waitTime` elapses.
    *
    * Return the last `compute` result and a boolean indicating whether `predicate` succeeded for that value.
    *
    * This method is useful in cases where `waitUntilTrue` makes it awkward to provide good error messages.
    */
  def computeUntilTrue[T](compute: => T, waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS, pause: Long = 100L)(
                    predicate: T => Boolean): (T, Boolean) = {
    val startTime = System.currentTimeMillis()
    while (true) {
      val result = compute
      if (predicate(result))
        return result -> true
      if (System.currentTimeMillis() > startTime + waitTime)
        return result -> false
      Thread.sleep(waitTime.min(pause))
    }
    // should never hit here
    throw new RuntimeException("unexpected error")
  }

  def isLeaderLocalOnBroker(topic: String, partitionId: Int, server: KafkaServer): Boolean = {
    server.replicaManager.getPartition(new TopicPartition(topic, partitionId)).exists(_.leaderReplicaIfLocal.isDefined)
  }

  def createRequestByteBuffer(request: RequestOrResponse): ByteBuffer = {
    val byteBuffer = ByteBuffer.allocate(request.sizeInBytes + 2)
    byteBuffer.putShort(request.requestId.get)
    request.writeTo(byteBuffer)
    byteBuffer.rewind()
    byteBuffer
  }

  /**
    * Wait until all brokers know about each other.
    *
    * @param servers The Kafka broker servers.
    * @param timeout The amount of time waiting on this condition before assert to fail
    */
  def waitUntilBrokerMetadataIsPropagated(servers: Seq[KafkaServer],
                                          timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
    val expectedBrokerIds = servers.map(_.config.brokerId).toSet
    TestUtils.waitUntilTrue(() => servers.forall(server =>
      expectedBrokerIds == server.apis.metadataCache.getAliveBrokers.map(_.id).toSet
    ), "Timed out waiting for broker metadata to propagate to all servers", timeout)
  }

  /**
   * Wait until a valid leader is propagated to the metadata cache in each broker.
   * It assumes that the leader propagated to each broker is the same.
   *
   * @param servers The list of servers that the metadata should reach to
   * @param topic The topic name
   * @param partition The partition Id
   * @param timeout The amount of time waiting on this condition before assert to fail
   * @return The leader of the partition.
   */
  def waitUntilMetadataIsPropagated(servers: Seq[KafkaServer], topic: String, partition: Int,
                                    timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
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

  def waitUntilControllerElected(zkUtils: ZkUtils, timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
    var controllerIdTry: Try[Int] = null
    TestUtils.waitUntilTrue(() => {
      controllerIdTry = Try { zkUtils.getController() }
      controllerIdTry.isSuccess
    }, s"Controller not elected after $timeout ms", waitTime = timeout)
    controllerIdTry.get
  }

  def waitUntilLeaderIsKnown(servers: Seq[KafkaServer], topic: String, partition: Int,
                             timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
    val tp = new TopicPartition(topic, partition)
    TestUtils.waitUntilTrue(() =>
      servers.exists { server =>
        server.replicaManager.getPartition(tp).exists(_.leaderReplicaIfLocal.isDefined)
      }, s"Partition $tp leaders not made yet after $timeout ms", waitTime = timeout
    )
  }

  def writeNonsenseToFile(fileName: File, position: Long, size: Int) {
    val file = new RandomAccessFile(fileName, "rw")
    file.seek(position)
    for (_ <- 0 until size)
      file.writeByte(random.nextInt(255))
    file.close()
  }

  def appendNonsenseToFile(fileName: File, size: Int) {
    val file = new FileOutputStream(fileName, true)
    for (_ <- 0 until size)
      file.write(random.nextInt(255))
    file.close()
  }

  def checkForPhantomInSyncReplicas(zkUtils: ZkUtils, topic: String, partitionToBeReassigned: Int, assignedReplicas: Seq[Int]) {
    val inSyncReplicas = zkUtils.getInSyncReplicasForPartition(topic, partitionToBeReassigned)
    // in sync replicas should not have any replica that is not in the new assigned replicas
    val phantomInSyncReplicas = inSyncReplicas.toSet -- assignedReplicas.toSet
    assertTrue("All in sync replicas %s must be in the assigned replica list %s".format(inSyncReplicas, assignedReplicas),
      phantomInSyncReplicas.isEmpty)
  }

  def ensureNoUnderReplicatedPartitions(zkUtils: ZkUtils, topic: String, partitionToBeReassigned: Int, assignedReplicas: Seq[Int],
                                                servers: Seq[KafkaServer]) {
    TestUtils.waitUntilTrue(() => {
        val inSyncReplicas = zkUtils.getInSyncReplicasForPartition(topic, partitionToBeReassigned)
        inSyncReplicas.size == assignedReplicas.size
      },
      "Reassigned partition [%s,%d] is under replicated".format(topic, partitionToBeReassigned))
    var leader: Option[Int] = None
    TestUtils.waitUntilTrue(() => {
        leader = zkUtils.getLeaderForPartition(topic, partitionToBeReassigned)
        leader.isDefined
      },
      "Reassigned partition [%s,%d] is unavailable".format(topic, partitionToBeReassigned))
    TestUtils.waitUntilTrue(() => {
        val leaderBroker = servers.filter(s => s.config.brokerId == leader.get).head
        leaderBroker.replicaManager.underReplicatedPartitionCount == 0
      },
      "Reassigned partition [%s,%d] is under-replicated as reported by the leader %d".format(topic, partitionToBeReassigned, leader.get))
  }

  def checkIfReassignPartitionPathExists(zkUtils: ZkUtils): Boolean = {
    zkUtils.pathExists(ZkUtils.ReassignPartitionsPath)
  }

  def verifyNonDaemonThreadsStatus(threadNamePrefix: String) {
    val threadCount = Thread.getAllStackTraces.keySet.asScala.count { t =>
      !t.isDaemon && t.isAlive && t.getName.startsWith(threadNamePrefix)
    }
    assertEquals(0, threadCount)
  }

  /**
   * Create new LogManager instance with default configuration for testing
   */
  def createLogManager(logDirs: Array[File] = Array.empty[File],
                       defaultConfig: LogConfig = LogConfig(),
                       cleanerConfig: CleanerConfig = CleanerConfig(enableCleaner = false),
                       time: MockTime = new MockTime()): LogManager = {
    new LogManager(logDirs = logDirs,
                   topicConfigs = Map(),
                   defaultConfig = defaultConfig,
                   cleanerConfig = cleanerConfig,
                   ioThreads = 4,
                   flushCheckMs = 1000L,
                   flushRecoveryOffsetCheckpointMs = 10000L,
                   flushStartOffsetCheckpointMs = 10000L,
                   retentionCheckMs = 1000L,
                   maxPidExpirationMs = 60 * 60 * 1000,
                   scheduler = time.scheduler,
                   time = time,
                   brokerState = BrokerState(),
                   brokerTopicStats = new BrokerTopicStats)
  }

  @deprecated("This method has been deprecated and it will be removed in a future release.", "0.10.0.0")
  def sendMessages(servers: Seq[KafkaServer],
                   topic: String,
                   numMessages: Int,
                   partition: Int = -1,
                   compression: CompressionCodec = NoCompressionCodec): List[String] = {
    val header = "test-%d".format(partition)
    val props = new Properties()
    props.put("compression.codec", compression.codec.toString)
    val ms = 0.until(numMessages).map(x => header + "-" + x)

    // Specific Partition
    if (partition >= 0) {
      val producer: Producer[Int, String] =
        createProducer(TestUtils.getBrokerListStrFromServers(servers),
          encoder = classOf[StringEncoder].getName,
          keyEncoder = classOf[IntEncoder].getName,
          partitioner = classOf[FixedValuePartitioner].getName,
          producerProps = props)

      producer.send(ms.map(m => new KeyedMessage[Int, String](topic, partition, m)): _*)
      debug("Sent %d messages for partition [%s,%d]".format(ms.size, topic, partition))
      producer.close()
      ms.toList
    } else {
      // Use topic as the key to determine partition
      val producer: Producer[String, String] = createProducer(
        TestUtils.getBrokerListStrFromServers(servers),
        encoder = classOf[StringEncoder].getName,
        keyEncoder = classOf[StringEncoder].getName,
        partitioner = classOf[DefaultPartitioner].getName,
        producerProps = props)
      producer.send(ms.map(m => new KeyedMessage[String, String](topic, topic, m)): _*)
      producer.close()
      debug("Sent %d messages for topic [%s]".format(ms.size, topic))
      ms.toList
    }
  }

  def produceMessages(servers: Seq[KafkaServer],
                      topic: String,
                      numMessages: Int,
                      acks: Int = -1,
                      valueBytes: Int = -1): Seq[Array[Byte]] = {

    val producer = createNewProducer(
      TestUtils.getBrokerListStrFromServers(servers),
      retries = 5,
      requestTimeoutMs = 2000,
      acks = acks
    )

    val values = (0 until numMessages).map(x => valueBytes match {
      case -1 => s"test-$x".getBytes
      case _ => new Array[Byte](valueBytes)
    })

    val futures = values.map { value =>
      producer.send(new ProducerRecord(topic, value))
    }
    futures.foreach(_.get)
    producer.close()

    debug(s"Sent ${values.size} messages for topic [$topic]")

    values
  }

  def produceMessage(servers: Seq[KafkaServer], topic: String, message: String) {
    val producer = createNewProducer(
      TestUtils.getBrokerListStrFromServers(servers),
      retries = 5,
      requestTimeoutMs = 2000
    )
    producer.send(new ProducerRecord(topic, topic.getBytes, message.getBytes)).get
    producer.close()
  }

  /**
   * Consume all messages (or a specific number of messages)
   *
   * @param topicMessageStreams the Topic Message Streams
   * @param nMessagesPerThread an optional field to specify the exact number of messages to be returned.
   *                           ConsumerTimeoutException will be thrown if there are no messages to be consumed.
   *                           If not specified, then all available messages will be consumed, and no exception is thrown.
   * @return the list of messages consumed.
   */
  @deprecated("This method has been deprecated and will be removed in a future release.", "0.11.0.0")
  def getMessages(topicMessageStreams: Map[String, List[KafkaStream[String, String]]],
                     nMessagesPerThread: Int = -1): List[String] = {

    var messages: List[String] = Nil
    val shouldGetAllMessages = nMessagesPerThread < 0
    for (messageStreams <- topicMessageStreams.values) {
      for (messageStream <- messageStreams) {
        val iterator = messageStream.iterator()
        try {
          var i = 0
          while ((shouldGetAllMessages && iterator.hasNext()) || (i < nMessagesPerThread)) {
            assertTrue(iterator.hasNext)
            val message = iterator.next().message // will throw a timeout exception if the message isn't there
            messages ::= message
            debug("received message: " + message)
            i += 1
          }
        } catch {
          case e: ConsumerTimeoutException =>
            if (shouldGetAllMessages) {
              // swallow the exception
              debug("consumer timed out after receiving " + messages.length + " message(s).")
            } else {
              throw e
            }
        }
      }
    }

    messages.reverse
  }

  def verifyTopicDeletion(zkUtils: ZkUtils, topic: String, numPartitions: Int, servers: Seq[KafkaServer]) {
    val topicPartitions = (0 until numPartitions).map(new TopicPartition(topic, _))
    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    TestUtils.waitUntilTrue(() => !zkUtils.isTopicMarkedForDeletion(topic),
      "Admin path /admin/delete_topic/%s path not deleted even after a replica is restarted".format(topic))
    TestUtils.waitUntilTrue(() => !zkUtils.pathExists(getTopicPath(topic)),
      "Topic path /brokers/topics/%s not deleted after /admin/delete_topic/%s path is deleted".format(topic, topic))
    // ensure that the topic-partition has been deleted from all brokers' replica managers
    TestUtils.waitUntilTrue(() =>
      servers.forall(server => topicPartitions.forall(tp => server.replicaManager.getPartition(tp).isEmpty)),
      "Replica manager's should have deleted all of this topic's partitions")
    // ensure that logs from all replicas are deleted if delete topic is marked successful in zookeeper
    assertTrue("Replica logs not deleted after delete topic is complete",
      servers.forall(server => topicPartitions.forall(tp => server.getLogManager().getLog(tp).isEmpty)))
    // ensure that topic is removed from all cleaner offsets
    TestUtils.waitUntilTrue(() => servers.forall(server => topicPartitions.forall { tp =>
      val checkpoints = server.getLogManager().logDirs.map { logDir =>
        new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint")).read()
      }
      checkpoints.forall(checkpointsPerLogDir => !checkpointsPerLogDir.contains(tp))
    }), "Cleaner offset for deleted partition should have been removed")
    import scala.collection.JavaConverters._
    TestUtils.waitUntilTrue(() => servers.forall(server =>
      server.config.logDirs.forall { logDir =>
        topicPartitions.forall { tp =>
          !new File(logDir, tp.topic + "-" + tp.partition).exists()
        }
      }
    ), "Failed to soft-delete the data to a delete directory")
    TestUtils.waitUntilTrue(() => servers.forall(server =>
      server.config.logDirs.forall { logDir =>
        topicPartitions.forall { tp =>
          !java.util.Arrays.asList(new File(logDir).list()).asScala.exists { partitionDirectoryName =>
            partitionDirectoryName.startsWith(tp.topic + "-" + tp.partition) &&
              partitionDirectoryName.endsWith(Log.DeleteDirSuffix)
          }
        }
      }
    ), "Failed to hard-delete the delete directory")
  }

  /**
   * Translate the given buffer into a string
   *
   * @param buffer The buffer to translate
   * @param encoding The encoding to use in translating bytes to characters
   */
  def readString(buffer: ByteBuffer, encoding: String = Charset.defaultCharset.toString): String = {
    val bytes = new Array[Byte](buffer.remaining)
    buffer.get(bytes)
    new String(bytes, encoding)
  }

  def copyOf(props: Properties): Properties = {
    val copy = new Properties()
    copy.putAll(props)
    copy
  }

  def sslConfigs(mode: Mode, clientCert: Boolean, trustStoreFile: Option[File], certAlias: String): Properties = {
    val trustStore = trustStoreFile.getOrElse {
      throw new Exception("SSL enabled but no trustStoreFile provided")
    }

    val sslConfigs = TestSslUtils.createSslConfig(clientCert, true, mode, trustStore, certAlias)

    val sslProps = new Properties()
    sslConfigs.asScala.foreach { case (k, v) => sslProps.put(k, v) }
    sslProps
  }

  // a X509TrustManager to trust self-signed certs for unit tests.
  def trustAllCerts: X509TrustManager = {
    val trustManager = new X509TrustManager() {
      override def getAcceptedIssuers: Array[X509Certificate] = {
        null
      }
      override def checkClientTrusted(certs: Array[X509Certificate], authType: String) {
      }
      override def checkServerTrusted(certs: Array[X509Certificate], authType: String) {
      }
    }
    trustManager
  }

  def waitAndVerifyAcls(expected: Set[Acl], authorizer: Authorizer, resource: Resource) = {
    TestUtils.waitUntilTrue(() => authorizer.getAcls(resource) == expected,
      s"expected acls $expected but got ${authorizer.getAcls(resource)}", waitTime = JTestUtils.DEFAULT_MAX_WAIT_MS)
  }

  /**
   * Verifies that this ACL is the secure one.
   */
  def isAclSecure(acl: ACL, sensitive: Boolean): Boolean = {
    debug(s"ACL $acl")
    acl.getPerms match {
      case Perms.READ => !sensitive && acl.getId.getScheme == "world"
      case Perms.ALL => acl.getId.getScheme == "sasl"
      case _ => false
    }
  }

  /**
   * Verifies that the ACL corresponds to the unsecure one that
   * provides ALL access to everyone (world).
   */
  def isAclUnsecure(acl: ACL): Boolean = {
    debug(s"ACL $acl")
    acl.getPerms match {
      case Perms.ALL => acl.getId.getScheme == "world"
      case _ => false
    }
  }

  private def secureZkPaths(zkUtils: ZkUtils): Seq[String] = {
    def subPaths(path: String): Seq[String] = {
      if (zkUtils.pathExists(path))
        path +: zkUtils.getChildren(path).map(c => path + "/" + c).flatMap(subPaths)
      else
        Seq.empty
    }
    val topLevelPaths = ZkUtils.SecureZkRootPaths ++ ZkUtils.SensitiveZkRootPaths
    topLevelPaths.flatMap(subPaths)
  }

  /**
   * Verifies that all secure paths in ZK are created with the expected ACL.
   */
  def verifySecureZkAcls(zkUtils: ZkUtils, usersWithAccess: Int) {
    secureZkPaths(zkUtils).foreach(path => {
      if (zkUtils.pathExists(path)) {
        val sensitive = ZkUtils.sensitivePath(path)
        // usersWithAccess have ALL access to path. For paths that are
        // not sensitive, world has READ access.
        val aclCount = if (sensitive) usersWithAccess else usersWithAccess + 1
        val acls = zkUtils.zkConnection.getAcl(path).getKey
        assertEquals(s"Invalid ACLs for $path $acls", aclCount, acls.size)
        acls.asScala.foreach(acl => isAclSecure(acl, sensitive))
      }
    })
  }

  /**
   * Verifies that secure paths in ZK have no access control. This is
   * the case when zookeeper.set.acl=false and no ACLs have been configured.
   */
  def verifyUnsecureZkAcls(zkUtils: ZkUtils) {
    secureZkPaths(zkUtils).foreach(path => {
      if (zkUtils.pathExists(path)) {
        val acls = zkUtils.zkConnection.getAcl(path).getKey
        assertEquals(s"Invalid ACLs for $path $acls", 1, acls.size)
        acls.asScala.foreach(isAclUnsecure)
      }
    })
  }

  /**
    * To use this you pass in a sequence of functions that are your arrange/act/assert test on the SUT.
    * They all run at the same time in the assertConcurrent method; the chances of triggering a multithreading code error,
    * and thereby failing some assertion are greatly increased.
    */
  def assertConcurrent(message: String, functions: Seq[() => Any], timeoutMs: Int) {

    def failWithTimeout() {
      fail(s"$message. Timed out, the concurrent functions took more than $timeoutMs milliseconds")
    }

    val numThreads = functions.size
    val threadPool = Executors.newFixedThreadPool(numThreads)
    val exceptions = ArrayBuffer[Throwable]()
    try {
      val runnables = functions.map { function =>
        new Callable[Unit] {
          override def call(): Unit = function()
        }
      }.asJava
      val futures = threadPool.invokeAll(runnables, timeoutMs, TimeUnit.MILLISECONDS).asScala
      futures.foreach { future =>
        if (future.isCancelled)
          failWithTimeout()
        else
          try future.get()
          catch { case e: Exception =>
            exceptions += e
          }
      }
    } catch {
      case _: InterruptedException => failWithTimeout()
      case e: Throwable => exceptions += e
    } finally {
      threadPool.shutdownNow()
    }
    assertTrue(s"$message failed with exception(s) $exceptions", exceptions.isEmpty)

  }

  def consumeTopicRecords[K, V](servers: Seq[KafkaServer], topic: String, numMessages: Int,
                                waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val consumer = createNewConsumer(TestUtils.getBrokerListStrFromServers(servers),
      securityProtocol = SecurityProtocol.PLAINTEXT)
    try {
      consumer.subscribe(Collections.singleton(topic))
      consumeRecords(consumer, numMessages, waitTime)
    } finally consumer.close()
  }

  def consumeRecords[K, V](consumer: KafkaConsumer[K, V], numMessages: Int,
                           waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[K, V]] = {
    val records = new ArrayBuffer[ConsumerRecord[K, V]]()
    waitUntilTrue(() => {
      records ++= consumer.poll(50).asScala
      records.size >= numMessages
    }, s"Consumed ${records.size} records until timeout instead of the expected $numMessages records", waitTime)
    assertEquals("Consumed more records than expected", numMessages, records.size)
    records
  }

  def createTransactionalProducer(transactionalId: String, servers: Seq[KafkaServer]) = {
    val props = new Properties()
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1")
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers), retries = Integer.MAX_VALUE, acks = -1, props = Some(props))
  }

  // Seeds the given topic with records with keys and values in the range [0..numRecords)
  def seedTopicWithNumberedRecords(topic: String, numRecords: Int, servers: Seq[KafkaServer]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    val producer = TestUtils.createNewProducer(TestUtils.getBrokerListStrFromServers(servers),
      retries = Integer.MAX_VALUE, acks = -1, props = Some(props))
    try {
      for (i <- 0 until numRecords) {
        producer.send(new ProducerRecord[Array[Byte], Array[Byte]](topic, asBytes(i.toString), asBytes(i.toString)))
      }
      producer.flush()
    } finally {
      producer.close()
    }
  }

  private def asString(bytes: Array[Byte]) = new String(bytes, StandardCharsets.UTF_8)

  private def asBytes(string: String) = string.getBytes(StandardCharsets.UTF_8)

  // Verifies that the record was intended to be committed by checking the the headers for an expected transaction status
  // If true, this will return the value as a string. It is expected that the record in question should have been created
  // by the `producerRecordWithExpectedTransactionStatus` method.
  def assertCommittedAndGetValue(record: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
    record.headers.headers(transactionStatusKey).headOption match {
      case Some(header) =>
        assertEquals(s"Got ${asString(header.value)} but expected the value to indicate " +
          s"committed status.", asString(committedValue), asString(header.value))
      case None =>
        fail("expected the record header to include an expected transaction status, but received nothing.")
    }
    recordValueAsString(record)
  }

  def recordValueAsString(record: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
    asString(record.value)
  }

  def producerRecordWithExpectedTransactionStatus(topic: String, key: Array[Byte], value: Array[Byte],
                                                  willBeCommitted: Boolean) : ProducerRecord[Array[Byte], Array[Byte]] = {
    val header = new Header {override def key() = transactionStatusKey
      override def value() = if (willBeCommitted)
        committedValue
      else
        abortedValue
    }
    new ProducerRecord[Array[Byte], Array[Byte]](topic, null, key, value, List(header))
  }

  def producerRecordWithExpectedTransactionStatus(topic: String, key: String, value: String,
                                                  willBeCommitted: Boolean) : ProducerRecord[Array[Byte], Array[Byte]] = {
    producerRecordWithExpectedTransactionStatus(topic, asBytes(key), asBytes(value), willBeCommitted)
  }

  // Collect the current positions for all partition in the consumers current assignment.
  def consumerPositions(consumer: KafkaConsumer[Array[Byte], Array[Byte]]) : Map[TopicPartition, OffsetAndMetadata]  = {
    val offsetsToCommit = new mutable.HashMap[TopicPartition, OffsetAndMetadata]()
    consumer.assignment.foreach{ topicPartition =>
      offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
    }
    offsetsToCommit.toMap
  }

  def pollUntilAtLeastNumRecords(consumer: KafkaConsumer[Array[Byte], Array[Byte]], numRecords: Int): Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val records = new ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]()
    TestUtils.waitUntilTrue(() => {
      records ++= consumer.poll(50)
      records.size >= numRecords
    }, s"Consumed ${records.size} records until timeout, but expected $numRecords records.")
    records
  }

  def resetToCommittedPositions(consumer: KafkaConsumer[Array[Byte], Array[Byte]]) = {
    consumer.assignment.foreach { case(topicPartition) =>
      val offset = consumer.committed(topicPartition)
      if (offset != null)
        consumer.seek(topicPartition, offset.offset)
      else
        consumer.seekToBeginning(Collections.singletonList(topicPartition))
    }
  }

  /**
   * Capture the console output during the execution of the provided function.
   */
  def grabConsoleOutput(f: => Unit) : String = {
    val out = new ByteArrayOutputStream
    try scala.Console.withOut(out)(f)
    finally scala.Console.out.flush
    out.toString
  }

  /**
    * Recursively copy the contents of a source directory to a target directory.
    */
  def copyDir(source: Path, target: Path): Unit = {
    Files.walkFileTree(source, new SimpleFileVisitor[Path]() {
      override def preVisitDirectory(dir: Path, attrs: BasicFileAttributes): FileVisitResult = {
        val copyToDir = target.resolve(source.relativize(dir))
        try {
          Files.copy(dir, copyToDir)
        } catch {
          case e: FileAlreadyExistsException => if (!Files.isDirectory(copyToDir)) throw e
        }
        return FileVisitResult.CONTINUE
      }
      override def visitFile(file: Path, attrs: BasicFileAttributes): FileVisitResult = {
        Files.copy(file, target.resolve(source.relativize(file)))
        return FileVisitResult.CONTINUE
      }
    })
  }

}

class IntEncoder(props: VerifiableProperties = null) extends Encoder[Int] {
  override def toBytes(n: Int) = n.toString.getBytes
}

@deprecated("This class is deprecated and it will be removed in a future release.", "0.10.0.0")
class StaticPartitioner(props: VerifiableProperties = null) extends Partitioner {
  def partition(data: Any, numPartitions: Int): Int = {
    data.asInstanceOf[String].length % numPartitions
  }
}

@deprecated("This class has been deprecated and it will be removed in a future release.", "0.10.0.0")
class FixedValuePartitioner(props: VerifiableProperties = null) extends Partitioner {
  def partition(data: Any, numPartitions: Int): Int = data.asInstanceOf[Int]
}
