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
import java.nio.file.{Files, StandardOpenOption}
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.{Arrays, Collections, Properties}
import java.util.concurrent.{Callable, ExecutionException, Executors, TimeUnit}
import javax.net.ssl.X509TrustManager
import kafka.api._
import kafka.cluster.{Broker, EndPoint, IsrChangeListener}
import kafka.log._
import kafka.security.auth.{Acl, Resource, Authorizer => LegacyAuthorizer}
import kafka.server._
import kafka.server.checkpoints.OffsetCheckpointFile
import com.yammer.metrics.core.Meter
import kafka.controller.LeaderIsrAndControllerEpoch
import kafka.metrics.KafkaYammerMetrics
import kafka.server.metadata.{CachedConfigRepository, ConfigRepository, MetadataBroker}
import kafka.utils.Implicits._
import kafka.zk._
import org.apache.kafka.clients.CommonClientConfigs
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBinding, AclBindingFilter}
import org.apache.kafka.common.config.ConfigResource
import org.apache.kafka.common.errors.{KafkaStorageException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.network.{ListenerName, Mode}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.quota.{ClientQuotaAlteration, ClientQuotaEntity}
import org.apache.kafka.common.record._
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, ByteArraySerializer, Deserializer, IntegerSerializer, Serializer}
import org.apache.kafka.common.utils.{Time, Utils}
import org.apache.kafka.common.utils.Utils._
import org.apache.kafka.common.{KafkaFuture, Node, TopicPartition}
import org.apache.kafka.server.authorizer.{Authorizer => JAuthorizer}
import org.apache.kafka.test.{TestSslUtils, TestUtils => JTestUtils}
import org.apache.log4j.PropertyConfigurator
import org.apache.zookeeper.KeeperException.SessionExpiredException
import org.apache.zookeeper.ZooDefs._
import org.apache.zookeeper.data.ACL
import org.junit.jupiter.api.Assertions._

import scala.jdk.CollectionConverters._
import scala.collection.mutable.{ArrayBuffer, ListBuffer}
import scala.collection.{Map, Seq, mutable}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}

/**
 * Utility functions to help with testing
 */
object TestUtils extends Logging {

  val random = JTestUtils.RANDOM

  /* 0 gives a random port; you can then retrieve the assigned port from the Socket object. */
  val RandomPort = 0

  /* Incorrect broker port which can used by kafka clients in tests. This port should not be used
   by any other service and hence we use a reserved port. */
  val IncorrectBrokerPort = 225

  /** Port to use for unit tests that mock/don't require a real ZK server. */
  val MockZkPort = 1
  /** ZooKeeper connection string to use for unit tests that mock/don't require a real ZK server. */
  val MockZkConnect = "127.0.0.1:" + MockZkPort
  // CN in SSL certificates - this is used for endpoint validation when enabled
  val SslCertificateCn = "localhost"

  private val transactionStatusKey = "transactionStatus"
  private val committedValue : Array[Byte] = "committed".getBytes(StandardCharsets.UTF_8)
  private val abortedValue : Array[Byte] = "aborted".getBytes(StandardCharsets.UTF_8)

  sealed trait LogDirFailureType
  case object Roll extends LogDirFailureType
  case object Checkpoint extends LogDirFailureType

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
  def tempChannel(): FileChannel =
    FileChannel.open(tempFile().toPath, StandardOpenOption.READ, StandardOpenOption.WRITE)

  /**
   * Create a kafka server instance with appropriate test settings
   * USING THIS IS A SIGN YOU ARE NOT WRITING A REAL UNIT TEST
   *
   * @param config The configuration of the server
   */
  def createServer(config: KafkaConfig, time: Time = Time.SYSTEM): KafkaServer = {
    createServer(config, time, None)
  }

  def createServer(config: KafkaConfig, threadNamePrefix: Option[String]): KafkaServer = {
    createServer(config, Time.SYSTEM, threadNamePrefix)
  }

  def createServer(config: KafkaConfig, time: Time, threadNamePrefix: Option[String]): KafkaServer = {
    createServer(config, time, threadNamePrefix, enableForwarding = false)
  }

  def createServer(config: KafkaConfig, time: Time, threadNamePrefix: Option[String], enableForwarding: Boolean): KafkaServer = {
    val server = new KafkaServer(config, time, threadNamePrefix, enableForwarding)
    server.startup()
    server
  }

  def boundPort(server: KafkaServer, securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int =
    server.boundPort(ListenerName.forSecurityProtocol(securityProtocol))

  def createBroker(id: Int, host: String, port: Int, securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): MetadataBroker = {
    MetadataBroker(id, null, Map(securityProtocol.name -> new Node(id, host, port)), false)
  }

  def createMetadataBroker(id: Int,
                           host: String = "localhost",
                           port: Int = 9092,
                           securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
                           rack: Option[String] = None,
                           fenced: Boolean = false): MetadataBroker = {
    MetadataBroker(id, rack.getOrElse(null),
      Map(securityProtocol.name -> new Node(id, host, port, rack.getOrElse(null))), fenced)
  }

  def createBrokerAndEpoch(id: Int, host: String, port: Int, securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
                           epoch: Long = 0): (Broker, Long) = {
    (new Broker(id, host, port, ListenerName.forSecurityProtocol(securityProtocol), securityProtocol), epoch)
  }

  /**
   * Create a test config for the provided parameters.
   *
   * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
   */
  def createBrokerConfigs(numConfigs: Int,
    zkConnect: String,
    enableControlledShutdown: Boolean = true,
    enableDeleteTopic: Boolean = true,
    interBrokerSecurityProtocol: Option[SecurityProtocol] = None,
    trustStoreFile: Option[File] = None,
    saslProperties: Option[Properties] = None,
    enablePlaintext: Boolean = true,
    enableSsl: Boolean = false,
    enableSaslPlaintext: Boolean = false,
    enableSaslSsl: Boolean = false,
    rackInfo: Map[Int, String] = Map(),
    logDirCount: Int = 1,
    enableToken: Boolean = false,
    numPartitions: Int = 1,
    defaultReplicationFactor: Short = 1): Seq[Properties] = {
    (0 until numConfigs).map { node =>
      createBrokerConfig(node, zkConnect, enableControlledShutdown, enableDeleteTopic, RandomPort,
        interBrokerSecurityProtocol, trustStoreFile, saslProperties, enablePlaintext = enablePlaintext, enableSsl = enableSsl,
        enableSaslPlaintext = enableSaslPlaintext, enableSaslSsl = enableSaslSsl, rack = rackInfo.get(node), logDirCount = logDirCount, enableToken = enableToken,
        numPartitions = numPartitions, defaultReplicationFactor = defaultReplicationFactor)
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
  def shutdownServers(servers: Seq[KafkaServer]): Unit = {
    import ExecutionContext.Implicits._
    val future = Future.traverse(servers) { s =>
      Future {
        s.shutdown()
        CoreUtils.delete(s.config.logDirs)
      }
    }
    Await.result(future, FiniteDuration(5, TimeUnit.MINUTES))
  }

  /**
    * Create a test config for the provided parameters.
    *
    * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
    */
  def createBrokerConfig(nodeId: Int,
                         zkConnect: String,
                         enableControlledShutdown: Boolean = true,
                         enableDeleteTopic: Boolean = true,
                         port: Int = RandomPort,
                         interBrokerSecurityProtocol: Option[SecurityProtocol] = None,
                         trustStoreFile: Option[File] = None,
                         saslProperties: Option[Properties] = None,
                         enablePlaintext: Boolean = true,
                         enableSaslPlaintext: Boolean = false,
                         saslPlaintextPort: Int = RandomPort,
                         enableSsl: Boolean = false,
                         sslPort: Int = RandomPort,
                         enableSaslSsl: Boolean = false,
                         saslSslPort: Int = RandomPort,
                         rack: Option[String] = None,
                         logDirCount: Int = 1,
                         enableToken: Boolean = false,
                         numPartitions: Int = 1,
                         defaultReplicationFactor: Short = 1): Properties = {
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
    if (logDirCount > 1) {
      val logDirs = (1 to logDirCount).toList.map(i =>
        // We would like to allow user to specify both relative path and absolute path as log directory for backward-compatibility reason
        // We can verify this by using a mixture of relative path and absolute path as log directories in the test
        if (i % 2 == 0) tempDir().getAbsolutePath else tempRelativeDir("data")
      ).mkString(",")
      props.put(KafkaConfig.LogDirsProp, logDirs)
    } else {
      props.put(KafkaConfig.LogDirProp, tempDir().getAbsolutePath)
    }
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
      props ++= sslConfigs(Mode.SERVER, false, trustStoreFile, s"server$nodeId")

    if (protocolAndPorts.exists { case (protocol, _) => usesSaslAuthentication(protocol) })
      props ++= JaasTestUtils.saslConfigs(saslProperties)

    interBrokerSecurityProtocol.foreach { protocol =>
      props.put(KafkaConfig.InterBrokerSecurityProtocolProp, protocol.name)
    }

    if (enableToken)
      props.put(KafkaConfig.DelegationTokenSecretKeyProp, "secretkey")

    props.put(KafkaConfig.NumPartitionsProp, numPartitions.toString)
    props.put(KafkaConfig.DefaultReplicationFactorProp, defaultReplicationFactor.toString)

    props
  }

  /**
   * Create a topic in ZooKeeper.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(zkClient: KafkaZkClient,
                  topic: String,
                  numPartitions: Int = 1,
                  replicationFactor: Int = 1,
                  servers: Seq[KafkaServer],
                  topicConfig: Properties = new Properties): scala.collection.immutable.Map[Int, Int] = {
    val adminZkClient = new AdminZkClient(zkClient)
    // create topic
    waitUntilTrue( () => {
      var hasSessionExpirationException = false
      try {
        adminZkClient.createTopic(topic, numPartitions, replicationFactor, topicConfig)
      } catch {
        case _: SessionExpiredException => hasSessionExpirationException = true
        case e: Throwable => throw e // let other exceptions propagate
      }
      !hasSessionExpirationException},
      s"Can't create topic $topic")

    // wait until we've propagated all partitions metadata to all servers
    val allPartitionsMetadata = waitForAllPartitionsMetadata(servers, topic, numPartitions)

    (0 until numPartitions).map { i =>
      i -> allPartitionsMetadata.get(new TopicPartition(topic, i)).map(_.leader()).getOrElse(
        throw new IllegalStateException(s"Cannot get the partition leader for topic: $topic, partition: $i in server metadata cache"))
    }.toMap
  }

  /**
   * Create a topic in ZooKeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(zkClient: KafkaZkClient,
                  topic: String,
                  partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
                  servers: Seq[KafkaServer]): scala.collection.immutable.Map[Int, Int] = {
    createTopic(zkClient, topic, partitionReplicaAssignment, servers, new Properties())
  }

  /**
   * Create a topic in ZooKeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic(zkClient: KafkaZkClient,
                  topic: String,
                  partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
                  servers: Seq[KafkaServer],
                  topicConfig: Properties): scala.collection.immutable.Map[Int, Int] = {
    val adminZkClient = new AdminZkClient(zkClient)
    // create topic
    waitUntilTrue( () => {
      var hasSessionExpirationException = false
      try {
        adminZkClient.createTopicWithAssignment(topic, topicConfig, partitionReplicaAssignment)
      } catch {
        case _: SessionExpiredException => hasSessionExpirationException = true
        case e: Throwable => throw e // let other exceptions propagate
      }
      !hasSessionExpirationException},
      s"Can't create topic $topic")

    // wait until we've propagated all partitions metadata to all servers
    val allPartitionsMetadata = waitForAllPartitionsMetadata(servers, topic, partitionReplicaAssignment.size)

    partitionReplicaAssignment.keySet.map { i =>
      i -> allPartitionsMetadata.get(new TopicPartition(topic, i)).map(_.leader()).getOrElse(
        throw new IllegalStateException(s"Cannot get the partition leader for topic: $topic, partition: $i in server metadata cache"))
    }.toMap
  }

  /**
    * Create the consumer offsets/group metadata topic and wait until the leader is elected and metadata is propagated
    * to all brokers.
    */
  def createOffsetsTopic(zkClient: KafkaZkClient, servers: Seq[KafkaServer]): Unit = {
    val server = servers.head
    createTopic(zkClient, Topic.GROUP_METADATA_TOPIC_NAME,
      server.config.getInt(KafkaConfig.OffsetsTopicPartitionsProp),
      server.config.getShort(KafkaConfig.OffsetsTopicReplicationFactorProp).toInt,
      servers,
      server.groupCoordinator.offsetsTopicConfigs)
  }

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
              baseOffset: Long = 0L,
              partitionLeaderEpoch: Int = RecordBatch.NO_PARTITION_LEADER_EPOCH): MemoryRecords = {
    val buf = ByteBuffer.allocate(DefaultRecordBatch.sizeInBytes(records.asJava))
    val builder = MemoryRecords.builder(buf, magicValue, codec, TimestampType.CREATE_TIME, baseOffset,
      System.currentTimeMillis, producerId, producerEpoch, sequence, false, partitionLeaderEpoch)
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
  def checkEquals(b1: ByteBuffer, b2: ByteBuffer): Unit = {
    assertEquals(b1.limit() - b1.position(), b2.limit() - b2.position(), "Buffers should have equal length")
    for(i <- 0 until b1.limit() - b1.position())
      assertEquals(b1.get(b1.position() + i), b2.get(b1.position() + i), "byte " + i + " byte not equal.")
  }

  /**
   *  Throw an exception if an iterable has different length than expected
   *
   */
  def checkLength[T](s1: Iterator[T], expectedLength:Int): Unit = {
    var n = 0
    while (s1.hasNext) {
      n += 1
      s1.next()
    }
    assertEquals(expectedLength, n)
  }

  /**
   * Throw an exception if the two iterators are of differing lengths or contain
   * different messages on their Nth element
   */
  def checkEquals[T](s1: java.util.Iterator[T], s2: java.util.Iterator[T]): Unit = {
    while(s1.hasNext && s2.hasNext)
      assertEquals(s1.next, s2.next)
    assertFalse(s1.hasNext, "Iterators have uneven length--first has more")
    assertFalse(s2.hasNext, "Iterators have uneven length--second has more")
  }

  def stackedIterator[T](s: Iterator[T]*): Iterator[T] = {
    new Iterator[T] {
      var cur: Iterator[T] = null
      val topIterator = s.iterator

      def hasNext: Boolean = {
        while (true) {
          if (cur == null) {
            if (topIterator.hasNext)
              cur = topIterator.next()
            else
              return false
          }
          if (cur.hasNext)
            return true
          cur = null
        }
        // should never reach here
        throw new RuntimeException("should not reach here")
      }

      def next() : T = cur.next()
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
    for(i <- 0 until buffer.limit())
      builder.append(String.format("%x", Integer.valueOf(buffer.get(buffer.position() + i))))
    builder.toString
  }

  /**
   * Returns security configuration options for broker or clients
   *
   * @param mode Client or server mode
   * @param securityProtocol Security protocol which indicates if SASL or SSL or both configs are included
   * @param trustStoreFile Trust store file must be provided for SSL and SASL_SSL
   * @param certAlias Alias of certificate in SSL key store
   * @param certCn CN for certificate
   * @param saslProperties SASL configs if security protocol is SASL_SSL or SASL_PLAINTEXT
   * @param tlsProtocol TLS version
   * @param needsClientCert If not empty, a flag which indicates if client certificates are required. By default
   *                        client certificates are generated only if securityProtocol is SSL (not for SASL_SSL).
   */
  def securityConfigs(mode: Mode,
                      securityProtocol: SecurityProtocol,
                      trustStoreFile: Option[File],
                      certAlias: String,
                      certCn: String,
                      saslProperties: Option[Properties],
                      tlsProtocol: String = TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS,
                      needsClientCert: Option[Boolean] = None): Properties = {
    val props = new Properties
    if (usesSslTransportLayer(securityProtocol)) {
      val addClientCert = needsClientCert.getOrElse(securityProtocol == SecurityProtocol.SSL)
      props ++= sslConfigs(mode, addClientCert, trustStoreFile, certAlias, certCn, tlsProtocol)
    }

    if (usesSaslAuthentication(securityProtocol))
      props ++= JaasTestUtils.saslConfigs(saslProperties)
    props.put(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, securityProtocol.name)
    props
  }

  def producerSecurityConfigs(securityProtocol: SecurityProtocol,
                              trustStoreFile: Option[File],
                              saslProperties: Option[Properties]): Properties =
    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "producer", SslCertificateCn, saslProperties)

  /**
   * Create a (new) producer with a few pre-configured properties.
   */
  def createProducer[K, V](brokerList: String,
                           acks: Int = -1,
                           maxBlockMs: Long = 60 * 1000L,
                           bufferSize: Long = 1024L * 1024L,
                           retries: Int = Int.MaxValue,
                           deliveryTimeoutMs: Int = 30 * 1000,
                           lingerMs: Int = 0,
                           batchSize: Int = 16384,
                           compressionType: String = "none",
                           requestTimeoutMs: Int = 20 * 1000,
                           securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
                           trustStoreFile: Option[File] = None,
                           saslProperties: Option[Properties] = None,
                           keySerializer: Serializer[K] = new ByteArraySerializer,
                           valueSerializer: Serializer[V] = new ByteArraySerializer,
                           enableIdempotence: Boolean = false): KafkaProducer[K, V] = {
    val producerProps = new Properties
    producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    producerProps.put(ProducerConfig.ACKS_CONFIG, acks.toString)
    producerProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs.toString)
    producerProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, bufferSize.toString)
    producerProps.put(ProducerConfig.RETRIES_CONFIG, retries.toString)
    producerProps.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs.toString)
    producerProps.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs.toString)
    producerProps.put(ProducerConfig.LINGER_MS_CONFIG, lingerMs.toString)
    producerProps.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)
    producerProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, compressionType)
    producerProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, enableIdempotence.toString)
    producerProps ++= producerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties)
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
    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "consumer", SslCertificateCn, saslProperties)

  def adminClientSecurityConfigs(securityProtocol: SecurityProtocol, trustStoreFile: Option[File], saslProperties: Option[Properties]): Properties =
    securityConfigs(Mode.CLIENT, securityProtocol, trustStoreFile, "admin-client", SslCertificateCn, saslProperties)

  /**
   * Create a consumer with a few pre-configured properties.
   */
  def createConsumer[K, V](brokerList: String,
                           groupId: String = "group",
                           autoOffsetReset: String = "earliest",
                           enableAutoCommit: Boolean = true,
                           readCommitted: Boolean = false,
                           maxPollRecords: Int = 500,
                           securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
                           trustStoreFile: Option[File] = None,
                           saslProperties: Option[Properties] = None,
                           keyDeserializer: Deserializer[K] = new ByteArrayDeserializer,
                           valueDeserializer: Deserializer[V] = new ByteArrayDeserializer): KafkaConsumer[K, V] = {
    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, if (readCommitted) "read_committed" else "read_uncommitted")
    consumerProps ++= consumerSecurityConfigs(securityProtocol, trustStoreFile, saslProperties)
    new KafkaConsumer[K, V](consumerProps, keyDeserializer, valueDeserializer)
  }

  def createBrokersInZk(zkClient: KafkaZkClient, ids: Seq[Int]): Seq[Broker] =
    createBrokersInZk(ids.map(kafka.admin.BrokerMetadata(_, None)), zkClient)

  def createBrokersInZk(brokerMetadatas: Seq[kafka.admin.BrokerMetadata], zkClient: KafkaZkClient): Seq[Broker] = {
    zkClient.makeSurePersistentPathExists(BrokerIdsZNode.path)
    val brokers = brokerMetadatas.map { b =>
      val protocol = SecurityProtocol.PLAINTEXT
      val listenerName = ListenerName.forSecurityProtocol(protocol)
      Broker(b.id, Seq(EndPoint("localhost", 6667, listenerName, protocol)), b.rack)
    }
    brokers.foreach(b => zkClient.registerBroker(BrokerInfo(Broker(b.id, b.endPoints, rack = b.rack),
      ApiVersion.latestVersion, jmxPort = -1)))
    brokers
  }

  def deleteBrokersInZk(zkClient: KafkaZkClient, ids: Seq[Int]): Seq[MetadataBroker] = {
    val brokers = ids.map(createBroker(_, "localhost", 6667, SecurityProtocol.PLAINTEXT))
    ids.foreach(b => zkClient.deletePath(BrokerIdsZNode.path + "/" + b))
    brokers
  }

  def getMsgStrings(n: Int): Seq[String] = {
    val buffer = new ListBuffer[String]
    for (i <- 0 until  n)
      buffer += ("msg" + i)
    buffer
  }

  def makeLeaderForPartition(zkClient: KafkaZkClient,
                             topic: String,
                             leaderPerPartitionMap: scala.collection.immutable.Map[Int, Int],
                             controllerEpoch: Int): Unit = {
    val newLeaderIsrAndControllerEpochs = leaderPerPartitionMap.map { case (partition, leader) =>
      val topicPartition = new TopicPartition(topic, partition)
      val newLeaderAndIsr = zkClient.getTopicPartitionState(topicPartition)
        .map(_.leaderAndIsr.newLeader(leader))
        .getOrElse(LeaderAndIsr(leader, List(leader)))
      topicPartition -> LeaderIsrAndControllerEpoch(newLeaderAndIsr, controllerEpoch)
    }
    zkClient.setTopicPartitionStatesRaw(newLeaderIsrAndControllerEpochs, ZkVersion.MatchAnyVersion)
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
  def waitUntilLeaderIsElectedOrChanged(zkClient: KafkaZkClient, topic: String, partition: Int, timeoutMs: Long = 30000L,
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
      leader = zkClient.getLeaderForPartition(topicPartition)
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
      throw new AssertionError(errorMessage)
    }
  }

  /**
   * Execute the given block. If it throws an assert error, retry. Repeat
   * until no error is thrown or the time limit elapses
   */
  def retry(maxWaitMs: Long)(block: => Unit): Unit = {
    var wait = 1L
    val startTime = System.currentTimeMillis()
    while(true) {
      try {
        block
        return
      } catch {
        case e: AssertionError =>
          val elapsed = System.currentTimeMillis - startTime
          if (elapsed > maxWaitMs) {
            throw e
          } else {
            info("Attempt failed, sleeping for " + wait + ", and then retrying.")
            Thread.sleep(wait)
            wait += math.min(wait, 1000)
          }
      }
    }
  }

  def pollUntilTrue(consumer: Consumer[_, _],
                    action: () => Boolean,
                    msg: => String,
                    waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
    waitUntilTrue(() => {
      consumer.poll(Duration.ofMillis(100))
      action()
    }, msg = msg, pause = 0L, waitTimeMs = waitTimeMs)
  }

  def pollRecordsUntilTrue[K, V](consumer: Consumer[K, V],
                                 action: ConsumerRecords[K, V] => Boolean,
                                 msg: => String,
                                 waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
    waitUntilTrue(() => {
      val records = consumer.poll(Duration.ofMillis(100))
      action(records)
    }, msg = msg, pause = 0L, waitTimeMs = waitTimeMs)
  }

  def subscribeAndWaitForRecords(topic: String,
                                 consumer: KafkaConsumer[Array[Byte], Array[Byte]],
                                 waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
    consumer.subscribe(Collections.singletonList(topic))
    pollRecordsUntilTrue(
      consumer,
      (records: ConsumerRecords[Array[Byte], Array[Byte]]) => !records.isEmpty,
      "Expected records",
      waitTimeMs)
  }

  /**
   * Wait for the presence of an optional value.
   *
   * @param func The function defining the optional value
   * @param msg Error message in the case that the value never appears
   * @param waitTimeMs Maximum time to wait
   * @return The unwrapped value returned by the function
   */
  def awaitValue[T](func: () => Option[T], msg: => String, waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): T = {
    var value: Option[T] = None
    waitUntilTrue(() => {
      value = func()
      value.isDefined
    }, msg, waitTimeMs)
    value.get
  }

  /**
    * Wait until the given condition is true or throw an exception if the given wait time elapses.
    *
    * @param condition condition to check
    * @param msg error message
    * @param waitTimeMs maximum time to wait and retest the condition before failing the test
    * @param pause delay between condition checks
    */
  def waitUntilTrue(condition: () => Boolean, msg: => String,
                    waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS, pause: Long = 100L): Unit = {
    val startTime = System.currentTimeMillis()
    while (true) {
      if (condition())
        return
      if (System.currentTimeMillis() > startTime + waitTimeMs)
        fail(msg)
      Thread.sleep(waitTimeMs.min(pause))
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
    server.replicaManager.onlinePartition(new TopicPartition(topic, partitionId)).exists(_.leaderLogIfLocal.isDefined)
  }

  def findLeaderEpoch(brokerId: Int,
                      topicPartition: TopicPartition,
                      servers: Iterable[KafkaServer]): Int = {
    val leaderServer = servers.find(_.config.brokerId == brokerId)
    val leaderPartition = leaderServer.flatMap(_.replicaManager.onlinePartition(topicPartition))
      .getOrElse(throw new AssertionError(s"Failed to find expected replica on broker $brokerId"))
    leaderPartition.getLeaderEpoch
  }

  def findFollowerId(topicPartition: TopicPartition,
                     servers: Iterable[KafkaServer]): Int = {
    val followerOpt = servers.find { server =>
      server.replicaManager.onlinePartition(topicPartition) match {
        case Some(partition) => !partition.leaderReplicaIdOpt.contains(server.config.brokerId)
        case None => false
      }
    }
    followerOpt
      .map(_.config.brokerId)
      .getOrElse(throw new AssertionError(s"Unable to locate follower for $topicPartition"))
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
    waitUntilTrue(() => servers.forall(server =>
      expectedBrokerIds == server.dataPlaneRequestProcessor.metadataCache.getAliveBrokers.map(_.id).toSet
    ), "Timed out waiting for broker metadata to propagate to all servers", timeout)
  }

  /**
   * Wait until the expected number of partitions is in the metadata cache in each broker.
   *
   * @param servers The list of servers that the metadata should reach to
   * @param topic The topic name
   * @param expectedNumPartitions The expected number of partitions
   * @return all partitions metadata
   */
  def waitForAllPartitionsMetadata(servers: Seq[KafkaServer],
                                   topic: String, expectedNumPartitions: Int): Map[TopicPartition, UpdateMetadataPartitionState] = {
    waitUntilTrue(
      () => servers.forall { server =>
        server.metadataCache.numPartitions(topic) match {
          case Some(numPartitions) => numPartitions == expectedNumPartitions
          case _ => false
        }
      },
      s"Topic [$topic] metadata not propagated after 60000 ms", waitTimeMs = 60000L)

    // since the metadata is propagated, we should get the same metadata from each server
    (0 until expectedNumPartitions).map { i =>
      new TopicPartition(topic, i) -> servers.head.metadataCache.getPartitionInfo(topic, i).getOrElse(
          throw new IllegalStateException(s"Cannot get topic: $topic, partition: $i in server metadata cache"))
    }.toMap
  }

  /**
   * Wait until a valid leader is propagated to the metadata cache in each broker.
   * It assumes that the leader propagated to each broker is the same.
   *
   * @param servers The list of servers that the metadata should reach to
   * @param topic The topic name
   * @param partition The partition Id
   * @param timeout The amount of time waiting on this condition before assert to fail
   * @return The metadata of the partition.
   */
  def waitForPartitionMetadata(servers: Seq[KafkaServer], topic: String, partition: Int,
                               timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): UpdateMetadataPartitionState = {
    waitUntilTrue(
      () => servers.forall { server =>
        server.metadataCache.getPartitionInfo(topic, partition) match {
          case Some(partitionState) => Request.isValidBrokerId(partitionState.leader)
          case _ => false
        }
      },
      "Partition [%s,%d] metadata not propagated after %d ms".format(topic, partition, timeout),
      waitTimeMs = timeout)

    servers.head.metadataCache.getPartitionInfo(topic, partition).getOrElse(
      throw new IllegalStateException(s"Cannot get topic: $topic, partition: $partition in server metadata cache"))
  }

  def waitUntilControllerElected(zkClient: KafkaZkClient, timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
    val (controllerId, _) = computeUntilTrue(zkClient.getControllerId, waitTime = timeout)(_.isDefined)
    controllerId.getOrElse(throw new AssertionError(s"Controller not elected after $timeout ms"))
  }

  def awaitLeaderChange(servers: Seq[KafkaServer],
                        tp: TopicPartition,
                        oldLeader: Int,
                        timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
    def newLeaderExists: Option[Int] = {
      servers.find { server =>
        server.config.brokerId != oldLeader &&
          server.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
      }.map(_.config.brokerId)
    }

    waitUntilTrue(() => newLeaderExists.isDefined,
      s"Did not observe leader change for partition $tp after $timeout ms", waitTimeMs = timeout)

    newLeaderExists.get
  }

  def waitUntilLeaderIsKnown(servers: Seq[KafkaServer],
                             tp: TopicPartition,
                             timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
    def leaderIfExists: Option[Int] = {
      servers.find { server =>
        server.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
      }.map(_.config.brokerId)
    }

    waitUntilTrue(() => leaderIfExists.isDefined,
      s"Partition $tp leaders not made yet after $timeout ms", waitTimeMs = timeout)

    leaderIfExists.get
  }

  def writeNonsenseToFile(fileName: File, position: Long, size: Int): Unit = {
    val file = new RandomAccessFile(fileName, "rw")
    file.seek(position)
    for (_ <- 0 until size)
      file.writeByte(random.nextInt(255))
    file.close()
  }

  def appendNonsenseToFile(file: File, size: Int): Unit = {
    val outputStream = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND)
    try {
      for (_ <- 0 until size)
        outputStream.write(random.nextInt(255))
    } finally outputStream.close()
  }

  def checkForPhantomInSyncReplicas(zkClient: KafkaZkClient, topic: String, partitionToBeReassigned: Int, assignedReplicas: Seq[Int]): Unit = {
    val inSyncReplicas = zkClient.getInSyncReplicasForPartition(new TopicPartition(topic, partitionToBeReassigned))
    // in sync replicas should not have any replica that is not in the new assigned replicas
    val phantomInSyncReplicas = inSyncReplicas.get.toSet -- assignedReplicas.toSet
    assertTrue(phantomInSyncReplicas.isEmpty,
      "All in sync replicas %s must be in the assigned replica list %s".format(inSyncReplicas, assignedReplicas))
  }

  def ensureNoUnderReplicatedPartitions(zkClient: KafkaZkClient, topic: String, partitionToBeReassigned: Int, assignedReplicas: Seq[Int],
                                                servers: Seq[KafkaServer]): Unit = {
    val topicPartition = new TopicPartition(topic, partitionToBeReassigned)
    waitUntilTrue(() => {
        val inSyncReplicas = zkClient.getInSyncReplicasForPartition(topicPartition)
        inSyncReplicas.get.size == assignedReplicas.size
      },
      "Reassigned partition [%s,%d] is under replicated".format(topic, partitionToBeReassigned))
    var leader: Option[Int] = None
    waitUntilTrue(() => {
        leader = zkClient.getLeaderForPartition(topicPartition)
        leader.isDefined
      },
      "Reassigned partition [%s,%d] is unavailable".format(topic, partitionToBeReassigned))
    waitUntilTrue(() => {
        val leaderBroker = servers.filter(s => s.config.brokerId == leader.get).head
        leaderBroker.replicaManager.underReplicatedPartitionCount == 0
      },
      "Reassigned partition [%s,%d] is under-replicated as reported by the leader %d".format(topic, partitionToBeReassigned, leader.get))
  }

  // Note: Call this method in the test itself, rather than the @AfterEach method.
  // Because of the assert, if assertNoNonDaemonThreads fails, nothing after would be executed.
  def assertNoNonDaemonThreads(threadNamePrefix: String): Unit = {
    val threadCount = Thread.getAllStackTraces.keySet.asScala.count { t =>
      !t.isDaemon && t.isAlive && t.getName.startsWith(threadNamePrefix)
    }
    assertEquals(0, threadCount)
  }

  def allThreadStackTraces(): String = {
    Thread.getAllStackTraces.asScala.map { case (thread, stackTrace) =>
      thread.getName + "\n\t" + stackTrace.toList.map(_.toString).mkString("\n\t")
    }.mkString("\n")
  }

  /**
   * Create new LogManager instance with default configuration for testing
   */
  def createLogManager(logDirs: Seq[File] = Seq.empty[File],
                       defaultConfig: LogConfig = LogConfig(),
                       configRepository: ConfigRepository = new CachedConfigRepository,
                       cleanerConfig: CleanerConfig = CleanerConfig(enableCleaner = false),
                       time: MockTime = new MockTime()): LogManager = {
    new LogManager(logDirs = logDirs.map(_.getAbsoluteFile),
                   initialOfflineDirs = Array.empty[File],
                   configRepository = configRepository,
                   initialDefaultConfig = defaultConfig,
                   cleanerConfig = cleanerConfig,
                   recoveryThreadsPerDataDir = 4,
                   flushCheckMs = 1000L,
                   flushRecoveryOffsetCheckpointMs = 10000L,
                   flushStartOffsetCheckpointMs = 10000L,
                   retentionCheckMs = 1000L,
                   maxPidExpirationMs = 60 * 60 * 1000,
                   scheduler = time.scheduler,
                   time = time,
                   brokerTopicStats = new BrokerTopicStats,
                   logDirFailureChannel = new LogDirFailureChannel(logDirs.size),
                   keepPartitionMetadataFile = true)
  }

  class MockAlterIsrManager extends AlterIsrManager {
    val isrUpdates: mutable.Queue[AlterIsrItem] = new mutable.Queue[AlterIsrItem]()
    val inFlight: AtomicBoolean = new AtomicBoolean(false)

    override def submit(alterIsrItem: AlterIsrItem): Boolean = {
      if (inFlight.compareAndSet(false, true)) {
        isrUpdates += alterIsrItem
        true
      } else {
        false
      }
    }

    override def clearPending(topicPartition: TopicPartition): Unit = {
      inFlight.set(false);
    }

    def completeIsrUpdate(newZkVersion: Int): Unit = {
      if (inFlight.compareAndSet(true, false)) {
        val item = isrUpdates.head
        item.callback.apply(Right(item.leaderAndIsr.withZkVersion(newZkVersion)))
      } else {
        fail("Expected an in-flight ISR update, but there was none")
      }
    }

    def failIsrUpdate(error: Errors): Unit = {
      if (inFlight.compareAndSet(true, false)) {
        val item = isrUpdates.dequeue()
        item.callback.apply(Left(error))
      } else {
        fail("Expected an in-flight ISR update, but there was none")
      }
    }
  }

  def createAlterIsrManager(): MockAlterIsrManager = {
    new MockAlterIsrManager()
  }

  class MockIsrChangeListener extends IsrChangeListener {
    val expands: AtomicInteger = new AtomicInteger(0)
    val shrinks: AtomicInteger = new AtomicInteger(0)
    val failures: AtomicInteger = new AtomicInteger(0)

    override def markExpand(): Unit = expands.incrementAndGet()

    override def markShrink(): Unit = shrinks.incrementAndGet()

    override def markFailed(): Unit = failures.incrementAndGet()

    def reset(): Unit = {
      expands.set(0)
      shrinks.set(0)
      failures.set(0)
    }
  }

  def createIsrChangeListener(): MockIsrChangeListener = {
    new MockIsrChangeListener()
  }

  def createConfigRepository(topic: String, props: Properties): CachedConfigRepository = {
    val configRepository = new CachedConfigRepository()
    props.entrySet().forEach(e => configRepository.setTopicConfig(topic, e.getKey.toString, e.getValue.toString))
    configRepository
  }

  def produceMessages(servers: Seq[KafkaServer],
                      records: Seq[ProducerRecord[Array[Byte], Array[Byte]]],
                      acks: Int = -1): Unit = {
    val producer = createProducer(TestUtils.getBrokerListStrFromServers(servers), acks = acks)
    try {
      val futures = records.map(producer.send)
      futures.foreach(_.get)
    } finally {
      producer.close()
    }

    val topics = records.map(_.topic).distinct
    debug(s"Sent ${records.size} messages for topics ${topics.mkString(",")}")
  }

  def generateAndProduceMessages(servers: Seq[KafkaServer],
                                 topic: String,
                                 numMessages: Int,
                                 acks: Int = -1): Seq[String] = {
    val values = (0 until numMessages).map(x =>  s"test-$x")
    val intSerializer = new IntegerSerializer()
    val records = values.zipWithIndex.map { case (v, i) =>
      new ProducerRecord(topic, intSerializer.serialize(topic, i), v.getBytes)
    }
    produceMessages(servers, records, acks)
    values
  }

  def produceMessage(servers: Seq[KafkaServer], topic: String, message: String,
                     deliveryTimeoutMs: Int = 30 * 1000, requestTimeoutMs: Int = 20 * 1000): Unit = {
    val producer = createProducer(TestUtils.getBrokerListStrFromServers(servers),
      deliveryTimeoutMs = deliveryTimeoutMs, requestTimeoutMs = requestTimeoutMs)
    try {
      producer.send(new ProducerRecord(topic, topic.getBytes, message.getBytes)).get
    } finally {
      producer.close()
    }
  }

  def verifyTopicDeletion(zkClient: KafkaZkClient, topic: String, numPartitions: Int, servers: Seq[KafkaServer]): Unit = {
    val topicPartitions = (0 until numPartitions).map(new TopicPartition(topic, _))
    // wait until admin path for delete topic is deleted, signaling completion of topic deletion
    waitUntilTrue(() => !zkClient.isTopicMarkedForDeletion(topic),
      "Admin path /admin/delete_topics/%s path not deleted even after a replica is restarted".format(topic))
    waitUntilTrue(() => !zkClient.topicExists(topic),
      "Topic path /brokers/topics/%s not deleted after /admin/delete_topics/%s path is deleted".format(topic, topic))
    // ensure that the topic-partition has been deleted from all brokers' replica managers
    waitUntilTrue(() =>
      servers.forall(server => topicPartitions.forall(tp => server.replicaManager.onlinePartition(tp).isEmpty)),
      "Replica manager's should have deleted all of this topic's partitions")
    // ensure that logs from all replicas are deleted if delete topic is marked successful in ZooKeeper
    assertTrue(servers.forall(server => topicPartitions.forall(tp => server.getLogManager.getLog(tp).isEmpty)),
      "Replica logs not deleted after delete topic is complete")
    // ensure that topic is removed from all cleaner offsets
    waitUntilTrue(() => servers.forall(server => topicPartitions.forall { tp =>
      val checkpoints = server.getLogManager.liveLogDirs.map { logDir =>
        new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint")).read()
      }
      checkpoints.forall(checkpointsPerLogDir => !checkpointsPerLogDir.contains(tp))
    }), "Cleaner offset for deleted partition should have been removed")
    waitUntilTrue(() => servers.forall(server =>
      server.config.logDirs.forall { logDir =>
        topicPartitions.forall { tp =>
          !new File(logDir, tp.topic + "-" + tp.partition).exists()
        }
      }
    ), "Failed to soft-delete the data to a delete directory")
    waitUntilTrue(() => servers.forall(server =>
      server.config.logDirs.forall { logDir =>
        topicPartitions.forall { tp =>
          !Arrays.asList(new File(logDir).list()).asScala.exists { partitionDirectoryName =>
            partitionDirectoryName.startsWith(tp.topic + "-" + tp.partition) &&
              partitionDirectoryName.endsWith(Log.DeleteDirSuffix)
          }
        }
      }
    ), "Failed to hard-delete the delete directory")
  }


  def causeLogDirFailure(failureType: LogDirFailureType, leaderServer: KafkaServer, partition: TopicPartition): Unit = {
    // Make log directory of the partition on the leader broker inaccessible by replacing it with a file
    val localLog = leaderServer.replicaManager.localLogOrException(partition)
    val logDir = localLog.dir.getParentFile
    CoreUtils.swallow(Utils.delete(logDir), this)
    logDir.createNewFile()
    assertTrue(logDir.isFile)

    if (failureType == Roll) {
      assertThrows(classOf[KafkaStorageException], () => leaderServer.replicaManager.getLog(partition).get.roll())
    } else if (failureType == Checkpoint) {
      leaderServer.replicaManager.checkpointHighWatermarks()
    }

    // Wait for ReplicaHighWatermarkCheckpoint to happen so that the log directory of the topic will be offline
    waitUntilTrue(() => !leaderServer.logManager.isLogDirOnline(logDir.getAbsolutePath), "Expected log directory offline", 3000L)
    assertTrue(leaderServer.replicaManager.localLog(partition).isEmpty)
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
    copy ++= props
    copy
  }

  def sslConfigs(mode: Mode, clientCert: Boolean, trustStoreFile: Option[File], certAlias: String,
                 certCn: String = SslCertificateCn,
                 tlsProtocol: String = TestSslUtils.DEFAULT_TLS_PROTOCOL_FOR_TESTS): Properties = {
    val trustStore = trustStoreFile.getOrElse {
      throw new Exception("SSL enabled but no trustStoreFile provided")
    }

    val sslConfigs = new TestSslUtils.SslConfigsBuilder(mode)
      .useClientCert(clientCert)
      .createNewTrustStore(trustStore)
      .certAlias(certAlias)
      .cn(certCn)
      .tlsProtocol(tlsProtocol)
      .build()

    val sslProps = new Properties()
    sslConfigs.forEach { (k, v) => sslProps.put(k, v) }
    sslProps
  }

  // a X509TrustManager to trust self-signed certs for unit tests.
  def trustAllCerts: X509TrustManager = {
    val trustManager = new X509TrustManager() {
      override def getAcceptedIssuers: Array[X509Certificate] = {
        null
      }
      override def checkClientTrusted(certs: Array[X509Certificate], authType: String): Unit = {
      }
      override def checkServerTrusted(certs: Array[X509Certificate], authType: String): Unit = {
      }
    }
    trustManager
  }

  def waitAndVerifyAcls(expected: Set[AccessControlEntry],
                        authorizer: JAuthorizer,
                        resource: ResourcePattern,
                        accessControlEntryFilter: AccessControlEntryFilter = AccessControlEntryFilter.ANY): Unit = {
    val newLine = scala.util.Properties.lineSeparator

    val filter = new AclBindingFilter(resource.toFilter, accessControlEntryFilter)
    waitUntilTrue(() => authorizer.acls(filter).asScala.map(_.entry).toSet == expected,
      s"expected acls:${expected.mkString(newLine + "\t", newLine + "\t", newLine)}" +
        s"but got:${authorizer.acls(filter).asScala.map(_.entry).mkString(newLine + "\t", newLine + "\t", newLine)}")
  }

  @deprecated("Use org.apache.kafka.server.authorizer.Authorizer", "Since 2.5")
  def waitAndVerifyAcls(expected: Set[Acl], authorizer: LegacyAuthorizer, resource: Resource): Unit = {
    val newLine = scala.util.Properties.lineSeparator

    waitUntilTrue(() => authorizer.getAcls(resource) == expected,
      s"expected acls:${expected.mkString(newLine + "\t", newLine + "\t", newLine)}" +
        s"but got:${authorizer.getAcls(resource).mkString(newLine + "\t", newLine + "\t", newLine)}")
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

  private def secureZkPaths(zkClient: KafkaZkClient): Seq[String] = {
    def subPaths(path: String): Seq[String] = {
      if (zkClient.pathExists(path))
        path +: zkClient.getChildren(path).map(c => path + "/" + c).flatMap(subPaths)
      else
        Seq.empty
    }
    val topLevelPaths = ZkData.SecureRootPaths ++ ZkData.SensitiveRootPaths
    topLevelPaths.flatMap(subPaths)
  }

  /**
   * Verifies that all secure paths in ZK are created with the expected ACL.
   */
  def verifySecureZkAcls(zkClient: KafkaZkClient, usersWithAccess: Int): Unit = {
    secureZkPaths(zkClient).foreach(path => {
      if (zkClient.pathExists(path)) {
        val sensitive = ZkData.sensitivePath(path)
        // usersWithAccess have ALL access to path. For paths that are
        // not sensitive, world has READ access.
        val aclCount = if (sensitive) usersWithAccess else usersWithAccess + 1
        val acls = zkClient.getAcl(path)
        assertEquals(aclCount, acls.size, s"Invalid ACLs for $path $acls")
        acls.foreach(acl => isAclSecure(acl, sensitive))
      }
    })
  }

  /**
   * Verifies that secure paths in ZK have no access control. This is
   * the case when zookeeper.set.acl=false and no ACLs have been configured.
   */
  def verifyUnsecureZkAcls(zkClient: KafkaZkClient): Unit = {
    secureZkPaths(zkClient).foreach(path => {
      if (zkClient.pathExists(path)) {
        val acls = zkClient.getAcl(path)
        assertEquals(1, acls.size, s"Invalid ACLs for $path $acls")
        acls.foreach(isAclUnsecure)
      }
    })
  }

  /**
    * To use this you pass in a sequence of functions that are your arrange/act/assert test on the SUT.
    * They all run at the same time in the assertConcurrent method; the chances of triggering a multithreading code error,
    * and thereby failing some assertion are greatly increased.
    */
  def assertConcurrent(message: String, functions: Seq[() => Any], timeoutMs: Int): Unit = {

    def failWithTimeout(): Unit = {
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
    assertTrue(exceptions.isEmpty, s"$message failed with exception(s) $exceptions")
  }

  def consumeTopicRecords[K, V](servers: Seq[KafkaServer],
                                topic: String,
                                numMessages: Int,
                                groupId: String = "group",
                                securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
                                trustStoreFile: Option[File] = None,
                                waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val consumer = createConsumer(TestUtils.getBrokerListStrFromServers(servers, securityProtocol),
      groupId = groupId,
      securityProtocol = securityProtocol,
      trustStoreFile = trustStoreFile)
    try {
      consumer.subscribe(Collections.singleton(topic))
      consumeRecords(consumer, numMessages, waitTime)
    } finally consumer.close()
  }

  def pollUntilAtLeastNumRecords[K, V](consumer: Consumer[K, V],
                                       numRecords: Int,
                                       waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[K, V]] = {
    val records = new ArrayBuffer[ConsumerRecord[K, V]]()
    def pollAction(polledRecords: ConsumerRecords[K, V]): Boolean = {
      records ++= polledRecords.asScala
      records.size >= numRecords
    }
    pollRecordsUntilTrue(consumer, pollAction,
      waitTimeMs = waitTimeMs,
      msg = s"Consumed ${records.size} records before timeout instead of the expected $numRecords records")
    records
  }

  def consumeRecords[K, V](consumer: Consumer[K, V],
                           numRecords: Int,
                           waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[K, V]] = {
    val records = pollUntilAtLeastNumRecords(consumer, numRecords, waitTimeMs)
    assertEquals(numRecords, records.size, "Consumed more records than expected")
    records
  }

  /**
    * Will consume all the records for the given consumer for the specified duration. If you want to drain all the
    * remaining messages in the partitions the consumer is subscribed to, the duration should be set high enough so
    * that the consumer has enough time to poll everything. This would be based on the number of expected messages left
    * in the topic, and should not be too large (ie. more than a second) in our tests.
    *
    * @return All the records consumed by the consumer within the specified duration.
    */
  def consumeRecordsFor[K, V](consumer: KafkaConsumer[K, V], duration: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[K, V]] = {
    val startTime = System.currentTimeMillis()
    val records = new ArrayBuffer[ConsumerRecord[K, V]]()
    waitUntilTrue(() => {
      records ++= consumer.poll(Duration.ofMillis(50)).asScala
      System.currentTimeMillis() - startTime > duration
    }, s"The timeout $duration was greater than the maximum wait time.")
    records
  }

  def createTransactionalProducer(transactionalId: String,
                                  servers: Seq[KafkaServer],
                                  batchSize: Int = 16384,
                                  transactionTimeoutMs: Long = 60000,
                                  maxBlockMs: Long = 60000,
                                  deliveryTimeoutMs: Int = 120000,
                                  requestTimeoutMs: Int = 30000,
                                  maxInFlight: Int = 5): KafkaProducer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerListStrFromServers(servers))
    props.put(ProducerConfig.ACKS_CONFIG, "all")
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, batchSize.toString)
    props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, transactionalId)
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, transactionTimeoutMs.toString)
    props.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, maxBlockMs.toString)
    props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, deliveryTimeoutMs.toString)
    props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs.toString)
    props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, maxInFlight.toString)
    new KafkaProducer[Array[Byte], Array[Byte]](props, new ByteArraySerializer, new ByteArraySerializer)
  }

  // Seeds the given topic with records with keys and values in the range [0..numRecords)
  def seedTopicWithNumberedRecords(topic: String, numRecords: Int, servers: Seq[KafkaServer]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerListStrFromServers(servers))
    val producer = new KafkaProducer[Array[Byte], Array[Byte]](props, new ByteArraySerializer, new ByteArraySerializer)
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

  // Verifies that the record was intended to be committed by checking the headers for an expected transaction status
  // If true, this will return the value as a string. It is expected that the record in question should have been created
  // by the `producerRecordWithExpectedTransactionStatus` method.
  def assertCommittedAndGetValue(record: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
    record.headers.headers(transactionStatusKey).asScala.headOption match {
      case Some(header) =>
        assertEquals(asString(committedValue), asString(header.value), s"Got ${asString(header.value)} but expected the value to indicate " +
          s"committed status.")
      case None =>
        fail("expected the record header to include an expected transaction status, but received nothing.")
    }
    recordValueAsString(record)
  }

  def recordValueAsString(record: ConsumerRecord[Array[Byte], Array[Byte]]) : String = {
    asString(record.value)
  }

  def producerRecordWithExpectedTransactionStatus(topic: String, partition: Integer, key: Array[Byte], value: Array[Byte], willBeCommitted: Boolean): ProducerRecord[Array[Byte], Array[Byte]] = {
    val header = new Header {override def key() = transactionStatusKey
      override def value() = if (willBeCommitted)
        committedValue
      else
        abortedValue
    }
    new ProducerRecord[Array[Byte], Array[Byte]](topic, partition, key, value, Collections.singleton(header))
  }

  def producerRecordWithExpectedTransactionStatus(topic: String, partition: Integer, key: String, value: String, willBeCommitted: Boolean): ProducerRecord[Array[Byte], Array[Byte]] = {
    producerRecordWithExpectedTransactionStatus(topic, partition, asBytes(key), asBytes(value), willBeCommitted)
  }

  // Collect the current positions for all partition in the consumers current assignment.
  def consumerPositions(consumer: KafkaConsumer[Array[Byte], Array[Byte]]) : Map[TopicPartition, OffsetAndMetadata]  = {
    val offsetsToCommit = new mutable.HashMap[TopicPartition, OffsetAndMetadata]()
    consumer.assignment.forEach { topicPartition =>
      offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
    }
    offsetsToCommit.toMap
  }

  def resetToCommittedPositions(consumer: KafkaConsumer[Array[Byte], Array[Byte]]): Unit = {
    val committed = consumer.committed(consumer.assignment).asScala.filter(_._2 != null).map { case (k, v) => k -> v.offset }

    consumer.assignment.forEach { topicPartition =>
      if (committed.contains(topicPartition))
        consumer.seek(topicPartition, committed(topicPartition))
      else
        consumer.seekToBeginning(Collections.singletonList(topicPartition))
    }
  }

  def incrementalAlterConfigs(servers: Seq[KafkaServer], adminClient: Admin, props: Properties,
                              perBrokerConfig: Boolean, opType: OpType = OpType.SET): AlterConfigsResult  = {
    val configEntries = props.asScala.map { case (k, v) => new AlterConfigOp(new ConfigEntry(k, v), opType) }.toList.asJavaCollection
    val configs = if (perBrokerConfig) {
      servers.map { server =>
        val resource = new ConfigResource(ConfigResource.Type.BROKER, server.config.brokerId.toString)
        (resource, configEntries)
      }.toMap.asJava
    } else {
      Map(new ConfigResource(ConfigResource.Type.BROKER, "") -> configEntries).asJava
    }
    adminClient.incrementalAlterConfigs(configs)
  }

  def alterClientQuotas(adminClient: Admin, request: Map[ClientQuotaEntity, Map[String, Option[Double]]]): AlterClientQuotasResult = {
    val entries = request.map { case (entity, alter) =>
      val ops = alter.map { case (key, value) =>
        new ClientQuotaAlteration.Op(key, value.map(Double.box).getOrElse(null))
      }.asJavaCollection
      new ClientQuotaAlteration(entity, ops)
    }.asJavaCollection
    adminClient.alterClientQuotas(entries)
  }

  def assertLeader(client: Admin, topicPartition: TopicPartition, expectedLeader: Int): Unit = {
    waitForLeaderToBecome(client, topicPartition, Some(expectedLeader))
  }

  def assertNoLeader(client: Admin, topicPartition: TopicPartition): Unit = {
    waitForLeaderToBecome(client, topicPartition, None)
  }

  def waitForLeaderToBecome(client: Admin, topicPartition: TopicPartition, leader: Option[Int]): Unit = {
    val topic = topicPartition.topic
    val partition = topicPartition.partition

    waitUntilTrue(() => {
      try {
        val topicResult = client.describeTopics(Arrays.asList(topic)).all.get.get(topic)
        val partitionResult = topicResult.partitions.get(partition)
        Option(partitionResult.leader).map(_.id) == leader
      } catch {
        case e: ExecutionException if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false
      }
    }, "Timed out waiting for leader metadata")
  }

  def waitForBrokersOutOfIsr(client: Admin, partition: Set[TopicPartition], brokerIds: Set[Int]): Unit = {
    waitUntilTrue(
      () => {
        val description = client.describeTopics(partition.map(_.topic).asJava).all.get.asScala
        val isr = description
          .values
          .flatMap(_.partitions.asScala.flatMap(_.isr.asScala))
          .map(_.id)
          .toSet

        brokerIds.intersect(isr).isEmpty
      },
      s"Expected brokers $brokerIds to no longer in the ISR for $partition"
    )
  }

  def waitForBrokersInIsr(client: Admin, partition: TopicPartition, brokerIds: Set[Int]): Unit = {
    waitUntilTrue(
      () => {
        val description = client.describeTopics(Set(partition.topic).asJava).all.get.asScala
        val isr = description
          .values
          .flatMap(_.partitions.asScala.flatMap(_.isr.asScala))
          .map(_.id)
          .toSet

        brokerIds.subsetOf(isr)
      },
      s"Expected brokers $brokerIds to be in the ISR for $partition"
    )
  }

  def waitForReplicasAssigned(client: Admin, partition: TopicPartition, brokerIds: Seq[Int]): Unit = {
    waitUntilTrue(
      () => {
        val description = client.describeTopics(Set(partition.topic).asJava).all.get.asScala
        val replicas = description
          .values
          .flatMap(_.partitions.asScala.flatMap(_.replicas.asScala))
          .map(_.id)
          .toSeq

        brokerIds == replicas
      },
      s"Expected brokers $brokerIds to be the replicas for $partition"
    )
  }

  /**
   * Capture the console output during the execution of the provided function.
   */
  def grabConsoleOutput(f: => Unit) : String = {
    val out = new ByteArrayOutputStream
    try scala.Console.withOut(out)(f)
    finally scala.Console.out.flush()
    out.toString
  }

  /**
   * Capture the console error during the execution of the provided function.
   */
  def grabConsoleError(f: => Unit) : String = {
    val err = new ByteArrayOutputStream
    try scala.Console.withErr(err)(f)
    finally scala.Console.err.flush()
    err.toString
  }

  /**
   * Capture both the console output and console error during the execution of the provided function.
   */
  def grabConsoleOutputAndError(f: => Unit) : (String, String) = {
    val out = new ByteArrayOutputStream
    val err = new ByteArrayOutputStream
    try scala.Console.withOut(out)(scala.Console.withErr(err)(f))
    finally {
      scala.Console.out.flush()
      scala.Console.err.flush()
    }
    (out.toString, err.toString)
  }

  def assertFutureExceptionTypeEquals(future: KafkaFuture[_], clazz: Class[_ <: Throwable],
                                      expectedErrorMessage: Option[String] = None): Unit = {
    val cause = assertThrows(classOf[ExecutionException], () => future.get()).getCause
    assertTrue(clazz.isInstance(cause), "Expected an exception of type " + clazz.getName + "; got type " +
      cause.getClass.getName)
    expectedErrorMessage.foreach(message => assertTrue(cause.getMessage.contains(message), s"Received error message : ${cause.getMessage}" +
      s" does not contain expected error message : $message"))
  }

  def totalMetricValue(server: KafkaServer, metricName: String): Long = {
    val allMetrics = server.metrics.metrics
    val total = allMetrics.values().asScala.filter(_.metricName().name() == metricName)
      .foldLeft(0.0)((total, metric) => total + metric.metricValue.asInstanceOf[Double])
    total.toLong
  }

  def meterCount(metricName: String): Long = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (k, _) => k.getMBeanName.endsWith(metricName) }
      .values
      .headOption
      .getOrElse(fail(s"Unable to find metric $metricName"))
      .asInstanceOf[Meter]
      .count
  }

  def clearYammerMetrics(): Unit = {
    for (metricName <- KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala)
      KafkaYammerMetrics.defaultRegistry.removeMetric(metricName)
  }

  def stringifyTopicPartitions(partitions: Set[TopicPartition]): String = {
    Json.encodeAsString(Map("partitions" ->
      partitions.map(tp => Map("topic" -> tp.topic, "partition" -> tp.partition).asJava).asJava).asJava)
  }

  def resource[R <: AutoCloseable, A](resource: R)(func: R => A): A = {
    try {
      func(resource)
    } finally {
      resource.close()
    }
  }

  /**
   * Set broker replication quotas and enable throttling for a set of partitions. This
   * will override any previous replication quotas, but will leave the throttling status
   * of other partitions unaffected.
   */
  def setReplicationThrottleForPartitions(admin: Admin,
                                          brokerIds: Seq[Int],
                                          partitions: Set[TopicPartition],
                                          throttleBytes: Int): Unit = {
    throttleAllBrokersReplication(admin, brokerIds, throttleBytes)
    assignThrottledPartitionReplicas(admin, partitions.map(_ -> brokerIds).toMap)
  }

  /**
   * Remove a set of throttled partitions and reset the overall replication quota.
   */
  def removeReplicationThrottleForPartitions(admin: Admin,
                                             brokerIds: Seq[Int],
                                             partitions: Set[TopicPartition]): Unit = {
    removePartitionReplicaThrottles(admin, partitions)
    resetBrokersThrottle(admin, brokerIds)
  }

  /**
    * Throttles all replication across the cluster.
    * @param adminClient is the adminClient to use for making connection with the cluster
    * @param brokerIds all broker ids in the cluster
    * @param throttleBytes is the target throttle
    */
  def throttleAllBrokersReplication(adminClient: Admin, brokerIds: Seq[Int], throttleBytes: Int): Unit = {
    val throttleConfigs = Seq(
      new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.LeaderReplicationThrottledRateProp, throttleBytes.toString), AlterConfigOp.OpType.SET),
      new AlterConfigOp(new ConfigEntry(DynamicConfig.Broker.FollowerReplicationThrottledRateProp, throttleBytes.toString), AlterConfigOp.OpType.SET)
    ).asJavaCollection

    adminClient.incrementalAlterConfigs(
      brokerIds.map { brokerId =>
        new ConfigResource(ConfigResource.Type.BROKER, brokerId.toString) -> throttleConfigs
      }.toMap.asJava
    ).all().get()
  }

  def resetBrokersThrottle(adminClient: Admin, brokerIds: Seq[Int]): Unit =
    throttleAllBrokersReplication(adminClient, brokerIds, Int.MaxValue)

  def assignThrottledPartitionReplicas(adminClient: Admin, allReplicasByPartition: Map[TopicPartition, Seq[Int]]): Unit = {
    val throttles = allReplicasByPartition.groupBy(_._1.topic()).map {
      case (topic, replicasByPartition) =>
        new ConfigResource(ConfigResource.Type.TOPIC, topic) -> Seq(
          new AlterConfigOp(new ConfigEntry(LogConfig.LeaderReplicationThrottledReplicasProp, formatReplicaThrottles(replicasByPartition)), AlterConfigOp.OpType.SET),
          new AlterConfigOp(new ConfigEntry(LogConfig.FollowerReplicationThrottledReplicasProp, formatReplicaThrottles(replicasByPartition)), AlterConfigOp.OpType.SET)
        ).asJavaCollection
    }
    adminClient.incrementalAlterConfigs(throttles.asJava).all().get()
  }

  def removePartitionReplicaThrottles(adminClient: Admin, partitions: Set[TopicPartition]): Unit = {
    val throttles = partitions.map {
      tp =>
        new ConfigResource(ConfigResource.Type.TOPIC, tp.topic()) -> Seq(
          new AlterConfigOp(new ConfigEntry(LogConfig.LeaderReplicationThrottledReplicasProp, ""), AlterConfigOp.OpType.DELETE),
          new AlterConfigOp(new ConfigEntry(LogConfig.FollowerReplicationThrottledReplicasProp, ""), AlterConfigOp.OpType.DELETE)
        ).asJavaCollection
    }.toMap
    adminClient.incrementalAlterConfigs(throttles.asJava).all().get()
  }

  def formatReplicaThrottles(moves: Map[TopicPartition, Seq[Int]]): String =
    moves.flatMap { case (tp, assignment) =>
      assignment.map(replicaId => s"${tp.partition}:$replicaId")
    }.mkString(",")

  def waitForAllReassignmentsToComplete(adminClient: Admin, pause: Long = 100L): Unit = {
    waitUntilTrue(() => adminClient.listPartitionReassignments().reassignments().get().isEmpty,
      s"There still are ongoing reassignments", pause = pause)
  }

  def addAndVerifyAcls(server: KafkaServer, acls: Set[AccessControlEntry], resource: ResourcePattern): Unit = {
    val authorizer = server.dataPlaneRequestProcessor.authorizer.get
    val aclBindings = acls.map { acl => new AclBinding(resource, acl) }
    authorizer.createAcls(null, aclBindings.toList.asJava).asScala
      .map(_.toCompletableFuture.get)
      .foreach { result =>
        result.exception.ifPresent { e => throw e }
      }
    val aclFilter = new AclBindingFilter(resource.toFilter, AccessControlEntryFilter.ANY)
    waitAndVerifyAcls(
      authorizer.acls(aclFilter).asScala.map(_.entry).toSet ++ acls,
      authorizer, resource)
  }

  /**
   * Convenience function for tests which mutate log levels which resets the logging configuration after the test.
   */
  def withLogReset[T](fn: => T): T = {
    try {
      fn
    } finally {
      resetLogging
    }
  }

  /**
    * Resets the logging configuration after the test.
    */
  def resetLogging[T] = {
    org.apache.log4j.LogManager.resetConfiguration()
    val stream = this.getClass.getResourceAsStream("/log4j.properties")
    try {
      PropertyConfigurator.configure(stream)
    } finally {
      stream.close()
    }
  }
}
