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

import com.yammer.metrics.core.{Histogram, Meter}
import kafka.log._
import kafka.network.RequestChannel
import kafka.security.JaasTestUtils
import kafka.server._
import kafka.server.metadata.{ConfigRepository, MockConfigRepository}
import kafka.utils.Implicits._
import kafka.zk._
import org.apache.kafka.clients.admin.AlterConfigOp.OpType
import org.apache.kafka.clients.admin._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common._
import org.apache.kafka.common.acl.{AccessControlEntry, AccessControlEntryFilter, AclBindingFilter}
import org.apache.kafka.common.compress.Compression
import org.apache.kafka.common.config.{ConfigException, ConfigResource}
import org.apache.kafka.common.errors.{OperationNotAttemptedException, TopicExistsException, UnknownTopicOrPartitionException}
import org.apache.kafka.common.header.Header
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.memory.MemoryPool
import org.apache.kafka.common.message.UpdateMetadataRequestData.UpdateMetadataPartitionState
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network.{ClientInformation, ConnectionMode, ListenerName}
import org.apache.kafka.common.protocol.{ApiKeys, Errors}
import org.apache.kafka.common.record._
import org.apache.kafka.common.requests._
import org.apache.kafka.common.resource.ResourcePattern
import org.apache.kafka.common.security.auth.{KafkaPrincipal, KafkaPrincipalSerde, SecurityProtocol}
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Utils.formatAddress
import org.apache.kafka.coordinator.group.GroupCoordinatorConfig
import org.apache.kafka.coordinator.transaction.TransactionLogConfig
import org.apache.kafka.metadata.LeaderAndIsr
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.network.metrics.RequestChannelMetrics
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.authorizer.{AuthorizableRequestContext, Authorizer => JAuthorizer}
import org.apache.kafka.server.common.{ControllerRequestCompletionHandler, MetadataVersion}
import org.apache.kafka.server.config.{DelegationTokenManagerConfigs, KRaftConfigs, ReplicationConfigs, ServerConfigs, ServerLogConfigs, ZkConfigs}
import org.apache.kafka.server.metrics.KafkaYammerMetrics
import org.apache.kafka.server.util.MockTime
import org.apache.kafka.storage.internals.checkpoint.OffsetCheckpointFile
import org.apache.kafka.storage.internals.log.{CleanerConfig, LogConfig, LogDirFailureChannel, ProducerStateManagerConfig}
import org.apache.kafka.storage.log.metrics.BrokerTopicStats
import org.apache.kafka.test.{TestUtils => JTestUtils}
import org.apache.zookeeper.KeeperException.SessionExpiredException
import org.junit.jupiter.api.Assertions._
import org.mockito.ArgumentMatchers.{any, anyBoolean}
import org.mockito.Mockito

import java.io._
import java.net.InetAddress
import java.nio._
import java.nio.charset.{Charset, StandardCharsets}
import java.nio.file.{Files, StandardOpenOption}
import java.time.Duration
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.{Collections, Optional, Properties}
import scala.collection.mutable.ArrayBuffer
import scala.collection.{Map, Seq, mutable}
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOption
import scala.jdk.javaapi.OptionConverters
import scala.util.{Failure, Success, Try}

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

  val MockKraftPort = 1

  val MockKraftConnect = "127.0.0.1:" + MockKraftPort
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

  /**
   * Create a temporary relative directory
   */
  def tempRelativeDir(root: String): File = JTestUtils.tempRelativeDir(root)

  /**
   * Create a random log directory in the format <string>-<int> used for Kafka partition logs.
   * It is the responsibility of the caller to set up a shutdown hook for deletion of the directory.
   */
  def randomPartitionLogDir(parentDir: File): File = JTestUtils.randomPartitionLogDir(parentDir)

  /**
   * Create a temporary file
   */
  def tempFile(): File = JTestUtils.tempFile()

  /**
   * Create a temporary file with particular suffix and prefix
   */
  def tempFile(prefix: String, suffix: String): File = JTestUtils.tempFile(prefix, suffix)

  def tempPropertiesFile(properties: Map[String, String]): File = {
    val content = properties.map{case (k, v) => k + "=" + v}.mkString(System.lineSeparator())
    JTestUtils.tempFile(content)
  }

  /**
   * Create a test config for the provided parameters.
   *
   * Note that if `interBrokerSecurityProtocol` is defined, the listener for the `SecurityProtocol` will be enabled.
   */
  def createBrokerConfigs(
    numConfigs: Int,
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
    defaultReplicationFactor: Short = 1,
    startingIdNumber: Int = 0,
    enableFetchFromFollower: Boolean = false): Seq[Properties] = {
    val endingIdNumber = startingIdNumber + numConfigs - 1
    (startingIdNumber to endingIdNumber).map { node =>
      createBrokerConfig(node, zkConnect, enableControlledShutdown, enableDeleteTopic, RandomPort,
        interBrokerSecurityProtocol, trustStoreFile, saslProperties, enablePlaintext = enablePlaintext, enableSsl = enableSsl,
        enableSaslPlaintext = enableSaslPlaintext, enableSaslSsl = enableSaslSsl, rack = rackInfo.get(node), logDirCount = logDirCount, enableToken = enableToken,
        numPartitions = numPartitions, defaultReplicationFactor = defaultReplicationFactor, enableFetchFromFollower = enableFetchFromFollower)
    }
  }

  def plaintextBootstrapServers[B <: KafkaBroker](
    brokers: Seq[B]
  ): String = {
    bootstrapServers(brokers, ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT))
  }

  def bootstrapServers[B <: KafkaBroker](
    brokers: Seq[B],
    listenerName: ListenerName
  ): String = {
    brokers.map { s =>
      val listener = s.config.effectiveAdvertisedBrokerListeners.find(_.listenerName == listenerName).getOrElse(
        sys.error(s"Could not find listener with name ${listenerName.value}"))
      formatAddress(listener.host, s.boundPort(listenerName))
    }.mkString(",")
  }

  /**
    * Shutdown `servers` and delete their log directories.
    */
  def shutdownServers[B <: KafkaBroker](brokers: Seq[B], deleteLogDirs: Boolean = true): Unit = {
    import ExecutionContext.Implicits._
    val future = Future.traverse(brokers) { s =>
      Future {
        s.shutdown()
        if (deleteLogDirs) CoreUtils.delete(s.config.logDirs)
      }
    }
    Await.result(future, FiniteDuration(5, TimeUnit.MINUTES))
  }

  def createDummyBrokerConfig(): Properties = {
    createBrokerConfig(0, "")
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
                         defaultReplicationFactor: Short = 1,
                         enableFetchFromFollower: Boolean = false): Properties = {
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
    props.put(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
    props.put(ServerConfigs.UNSTABLE_API_VERSIONS_ENABLE_CONFIG, "true")
    if (zkConnect == null) {
      props.setProperty(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG, TimeUnit.MINUTES.toMillis(10).toString)
      props.put(KRaftConfigs.NODE_ID_CONFIG, nodeId.toString)
      props.put(ServerConfigs.BROKER_ID_CONFIG, nodeId.toString)
      props.put(SocketServerConfigs.ADVERTISED_LISTENERS_CONFIG, listeners)
      props.put(SocketServerConfigs.LISTENERS_CONFIG, listeners)
      props.put(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, "CONTROLLER")
      props.put(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, protocolAndPorts.
        map(p => "%s:%s".format(p._1, p._1)).mkString(",") + ",CONTROLLER:PLAINTEXT")
    } else {
      if (nodeId >= 0) props.put(ServerConfigs.BROKER_ID_CONFIG, nodeId.toString)
      props.put(SocketServerConfigs.LISTENERS_CONFIG, listeners)
    }
    if (logDirCount > 1) {
      val logDirs = (1 to logDirCount).toList.map(i =>
        // We would like to allow user to specify both relative path and absolute path as log directory for backward-compatibility reason
        // We can verify this by using a mixture of relative path and absolute path as log directories in the test
        if (i % 2 == 0) tempDir().getAbsolutePath else tempRelativeDir("data")
      ).mkString(",")
      props.put(ServerLogConfigs.LOG_DIRS_CONFIG, logDirs)
    } else {
      props.put(ServerLogConfigs.LOG_DIR_CONFIG, tempDir().getAbsolutePath)
    }
    if (zkConnect == null) {
      props.put(KRaftConfigs.PROCESS_ROLES_CONFIG, "broker")
      // Note: this is just a placeholder value for controller.quorum.voters. JUnit
      // tests use random port assignment, so the controller ports are not known ahead of
      // time. Therefore, we ignore controller.quorum.voters and use
      // controllerQuorumVotersFuture instead.
      props.put(QuorumConfig.QUORUM_VOTERS_CONFIG, "1000@localhost:0")
    } else {
      props.put(ZkConfigs.ZK_CONNECT_CONFIG, zkConnect)
      props.put(ZkConfigs.ZK_CONNECTION_TIMEOUT_MS_CONFIG, "10000")
    }
    props.put(ReplicationConfigs.REPLICA_SOCKET_TIMEOUT_MS_CONFIG, "1500")
    props.put(ReplicationConfigs.CONTROLLER_SOCKET_TIMEOUT_MS_CONFIG, "1500")
    props.put(ServerConfigs.CONTROLLED_SHUTDOWN_ENABLE_CONFIG, enableControlledShutdown.toString)
    props.put(ServerConfigs.DELETE_TOPIC_ENABLE_CONFIG, enableDeleteTopic.toString)
    props.put(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "1000")
    props.put(ServerConfigs.CONTROLLED_SHUTDOWN_RETRY_BACKOFF_MS_CONFIG, "100")
    props.put(CleanerConfig.LOG_CLEANER_DEDUPE_BUFFER_SIZE_PROP, "2097152")
    props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG, "1")
    props.put(ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_CONFIG, "100")
    if (!props.containsKey(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG))
      props.put(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG, "5")
    if (!props.containsKey(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG))
      props.put(GroupCoordinatorConfig.GROUP_INITIAL_REBALANCE_DELAY_MS_CONFIG, "0")
    rack.foreach(props.put(ServerConfigs.BROKER_RACK_CONFIG, _))
    // Reduce number of threads per broker
    props.put(SocketServerConfigs.NUM_NETWORK_THREADS_CONFIG, "2")
    props.put(ServerConfigs.BACKGROUND_THREADS_CONFIG, "2")

    if (protocolAndPorts.exists { case (protocol, _) => JaasTestUtils.usesSslTransportLayer(protocol) })
      props ++= JaasTestUtils.sslConfigs(ConnectionMode.SERVER, false, OptionConverters.toJava(trustStoreFile), s"server$nodeId")

    if (protocolAndPorts.exists { case (protocol, _) => JaasTestUtils.usesSaslAuthentication(protocol) })
      props ++= JaasTestUtils.saslConfigs(saslProperties.toJava)

    interBrokerSecurityProtocol.foreach { protocol =>
      props.put(ReplicationConfigs.INTER_BROKER_SECURITY_PROTOCOL_CONFIG, protocol.name)
    }

    if (enableToken)
      props.put(DelegationTokenManagerConfigs.DELEGATION_TOKEN_SECRET_KEY_CONFIG, "secretkey")

    props.put(ServerLogConfigs.NUM_PARTITIONS_CONFIG, numPartitions.toString)
    props.put(ReplicationConfigs.DEFAULT_REPLICATION_FACTOR_CONFIG, defaultReplicationFactor.toString)

    if (enableFetchFromFollower) {
      props.put(ServerConfigs.BROKER_RACK_CONFIG, nodeId.toString)
      props.put(ReplicationConfigs.REPLICA_SELECTOR_CLASS_CONFIG, "org.apache.kafka.common.replica.RackAwareReplicaSelector")
    }
    props
  }

  def setIbpVersion(config: Properties, version: MetadataVersion): Unit = {
    config.setProperty(ReplicationConfigs.INTER_BROKER_PROTOCOL_VERSION_CONFIG, version.version)
  }

  def createAdminClient[B <: KafkaBroker](
    brokers: Seq[B],
    listenerName: ListenerName,
    adminConfig: Properties = new Properties
  ): Admin = {
    val adminClientProperties = new Properties()
    adminClientProperties.putAll(adminConfig)
    if (!adminClientProperties.containsKey(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG) && !adminClientProperties.containsKey(AdminClientConfig.BOOTSTRAP_CONTROLLERS_CONFIG)) {
      adminClientProperties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers(brokers, listenerName))
    }
    Admin.create(adminClientProperties)
  }

  def createTopicWithAdminRaw[B <: KafkaBroker](
    admin: Admin,
    topic: String,
    numPartitions: Int = 1,
    replicationFactor: Int = 1,
    replicaAssignment: collection.Map[Int, Seq[Int]] = Map.empty,
    topicConfig: Properties = new Properties,
  ): Uuid = {
    val configsMap = new util.HashMap[String, String]()
    topicConfig.forEach((k, v) => configsMap.put(k.toString, v.toString))

    val result = if (replicaAssignment.isEmpty) {
      admin.createTopics(Collections.singletonList(new NewTopic(
        topic, numPartitions, replicationFactor.toShort).configs(configsMap)))
    } else {
      val assignment = new util.HashMap[Integer, util.List[Integer]]()
      replicaAssignment.foreachEntry { case (k, v) =>
        val replicas = new util.ArrayList[Integer]
        v.foreach(r => replicas.add(r.asInstanceOf[Integer]))
        assignment.put(k.asInstanceOf[Integer], replicas)
      }
      admin.createTopics(Collections.singletonList(new NewTopic(
        topic, assignment).configs(configsMap)))
    }

    result.topicId(topic).get()
  }

  def createTopicWithAdmin[B <: KafkaBroker](
    admin: Admin,
    topic: String,
    brokers: Seq[B],
    controllers: Seq[ControllerServer],
    numPartitions: Int = 1,
    replicationFactor: Int = 1,
    replicaAssignment: collection.Map[Int, Seq[Int]] = Map.empty,
    topicConfig: Properties = new Properties,
  ): scala.collection.immutable.Map[Int, Int] = {
    val effectiveNumPartitions = if (replicaAssignment.isEmpty) {
      numPartitions
    } else {
      replicaAssignment.size
    }

    def isTopicExistsAndHasSameNumPartitionsAndReplicationFactor(cause: Throwable): Boolean = {
      cause != null &&
        cause.isInstanceOf[TopicExistsException] &&
        // wait until all partitions metadata are propagated before verifying partitions number and replication factor
        waitForAllPartitionsMetadata(brokers, topic, effectiveNumPartitions).nonEmpty &&
        topicHasSameNumPartitionsAndReplicationFactor(admin, topic, effectiveNumPartitions, replicationFactor)
    }

    try {
      createTopicWithAdminRaw(
        admin,
        topic,
        numPartitions,
        replicationFactor,
        replicaAssignment,
        topicConfig
      )
    } catch {
      case e: ExecutionException =>
        if (!isTopicExistsAndHasSameNumPartitionsAndReplicationFactor(e.getCause)) {
          throw e
        }
    }

    // wait until we've propagated all partitions metadata to all brokers
    val allPartitionsMetadata = waitForAllPartitionsMetadata(brokers, topic, effectiveNumPartitions)
    controllers.foreach(controller => ensureConsistentKRaftMetadata(brokers, controller))

    (0 until effectiveNumPartitions).map { i =>
      i -> allPartitionsMetadata.get(new TopicPartition(topic, i)).map(_.leader()).getOrElse(
        throw new IllegalStateException(s"Cannot get the partition leader for topic: $topic, partition: $i in server metadata cache"))
    }.toMap
  }

  def describeTopic(
    admin: Admin,
    topic: String
  ): TopicDescription = {
    val describedTopics = admin.describeTopics(
      Collections.singleton(topic)
    ).allTopicNames().get()
    describedTopics.get(topic)
  }

  def topicHasSameNumPartitionsAndReplicationFactor(adminClient: Admin,
                                                    topic: String,
                                                    numPartitions: Int,
                                                    replicationFactor: Int): Boolean = {
    val description = describeTopic(adminClient, topic)
    description != null &&
      description.partitions().size() == numPartitions &&
      description.partitions().iterator().next().replicas().size() == replicationFactor
  }

  def createOffsetsTopicWithAdmin[B <: KafkaBroker](
    admin: Admin,
    brokers: Seq[B],
    controllers: Seq[ControllerServer]
  ): Map[Int, Int] = {
    val broker = brokers.head
    createTopicWithAdmin(
      admin = admin,
      topic = Topic.GROUP_METADATA_TOPIC_NAME,
      numPartitions = broker.config.getInt(GroupCoordinatorConfig.OFFSETS_TOPIC_PARTITIONS_CONFIG),
      replicationFactor = broker.config.getShort(GroupCoordinatorConfig.OFFSETS_TOPIC_REPLICATION_FACTOR_CONFIG).toInt,
      brokers = brokers,
      controllers = controllers,
      topicConfig = broker.groupCoordinator.groupMetadataTopicConfigs,
    )
  }

  def createTransactionStateTopicWithAdmin[B <: KafkaBroker](
    admin: Admin,
    brokers: Seq[B],
    controllers: Seq[ControllerServer]
  ): Map[Int, Int] = {
    val broker = brokers.head
    createTopicWithAdmin(
      admin = admin,
      topic = Topic.TRANSACTION_STATE_TOPIC_NAME,
      numPartitions = broker.config.getInt(TransactionLogConfig.TRANSACTIONS_TOPIC_PARTITIONS_CONFIG),
      replicationFactor = broker.config.getShort(TransactionLogConfig.TRANSACTIONS_TOPIC_REPLICATION_FACTOR_CONFIG).toInt,
      brokers = brokers,
      controllers = controllers,
      topicConfig = new Properties(),
    )
  }

  def deleteTopicWithAdmin[B <: KafkaBroker](
    admin: Admin,
    topic: String,
    brokers: Seq[B],
    controllers: Seq[ControllerServer]
  ): Unit = {
    try {
      admin.deleteTopics(Collections.singletonList(topic)).all().get()
    } catch {
      case e: ExecutionException if e.getCause != null &&
        e.getCause.isInstanceOf[UnknownTopicOrPartitionException] =>
        // ignore
    }
    waitForAllPartitionsMetadata(brokers, topic, 0)
    controllers.foreach(controller => ensureConsistentKRaftMetadata(brokers, controller))
  }

  /**
   * Create a topic in ZooKeeper using a customized replica assignment.
   * Wait until the leader is elected and the metadata is propagated to all brokers.
   * Return the leader for each partition.
   */
  def createTopic[B <: KafkaBroker](zkClient: KafkaZkClient,
                  topic: String,
                  partitionReplicaAssignment: collection.Map[Int, Seq[Int]],
                  servers: Seq[B]): scala.collection.immutable.Map[Int, Int] = {
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
                  servers: Seq[KafkaBroker],
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
   * Wrap a single record log buffer.
   */
  def singletonRecords(value: Array[Byte],
                       key: Array[Byte] = null,
                       codec: Compression = Compression.NONE,
                       timestamp: Long = RecordBatch.NO_TIMESTAMP,
                       magicValue: Byte = RecordBatch.CURRENT_MAGIC_VALUE): MemoryRecords = {
    records(Seq(new SimpleRecord(timestamp, key, value)), magicValue = magicValue, codec = codec)
  }

  def records(records: Iterable[SimpleRecord],
              magicValue: Byte = RecordBatch.CURRENT_MAGIC_VALUE,
              codec: Compression = Compression.NONE,
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
    producerProps ++= JaasTestUtils.producerSecurityConfigs(securityProtocol, OptionConverters.toJava(trustStoreFile), OptionConverters.toJava(saslProperties))
    new KafkaProducer[K, V](producerProps, keySerializer, valueSerializer)
  }

  /**
   * Create a consumer with a few pre-configured properties.
   */
  def createConsumer[K, V](brokerList: String,
                           groupProtocol: GroupProtocol,
                           groupId: String = "group",
                           autoOffsetReset: String = "earliest",
                           enableAutoCommit: Boolean = true,
                           readCommitted: Boolean = false,
                           maxPollRecords: Int = 500,
                           securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
                           trustStoreFile: Option[File] = None,
                           saslProperties: Option[Properties] = None,
                           keyDeserializer: Deserializer[K] = new ByteArrayDeserializer,
                           valueDeserializer: Deserializer[V] = new ByteArrayDeserializer): Consumer[K, V] = {
    val consumerProps = new Properties
    consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList)
    consumerProps.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, groupProtocol.toString)
    consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, autoOffsetReset)
    consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit.toString)
    consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords.toString)
    consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, if (readCommitted) "read_committed" else "read_uncommitted")
    consumerProps ++= JaasTestUtils.consumerSecurityConfigs(securityProtocol, OptionConverters.toJava(trustStoreFile), OptionConverters.toJava(saslProperties))
    new KafkaConsumer[K, V](consumerProps, keyDeserializer, valueDeserializer)
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
  def waitUntilLeaderIsElectedOrChangedWithAdmin(
    admin: Admin,
    topic: String,
    partition: Int,
    timeoutMs: Long = 30000L,
    oldLeaderOpt: Option[Int] = None,
    newLeaderOpt: Option[Int] = None
  ): Int = {
    def getPartitionLeader(topic: String, partition: Int): Option[Int] = {
      admin.describeTopics(Collections.singletonList(topic)).allTopicNames().get().get(topic).partitions().asScala.
        find(_.partition() == partition).
        flatMap { p =>
          if (p.leader().id() == Node.noNode().id()) {
            None
          } else {
            Some(p.leader().id())
          }
        }
    }
    doWaitUntilLeaderIsElectedOrChanged(getPartitionLeader, topic, partition, timeoutMs, oldLeaderOpt, newLeaderOpt)
  }

  private def doWaitUntilLeaderIsElectedOrChanged(
    getPartitionLeader: (String, Int) => Option[Int],
    topic: String,
    partition: Int,
    timeoutMs: Long,
    oldLeaderOpt: Option[Int],
    newLeaderOpt: Option[Int]
  ): Int = {
    require(!(oldLeaderOpt.isDefined && newLeaderOpt.isDefined), "Can't define both the old and the new leader")
    val startTime = System.currentTimeMillis()
    val topicPartition = new TopicPartition(topic, partition)

    trace(s"Waiting for leader to be elected or changed for partition $topicPartition, old leader is $oldLeaderOpt, " +
      s"new leader is $newLeaderOpt")

    var leader: Option[Int] = None
    var electedOrChangedLeader: Option[Int] = None
    while (electedOrChangedLeader.isEmpty && System.currentTimeMillis() < startTime + timeoutMs) {
      // check if leader is elected
      leader = getPartitionLeader(topic, partition)
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
    while (true) {
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
                                 waitTimeMs: Long = JTestUtils.DEFAULT_MAX_WAIT_MS,
                                 pollTimeoutMs: Long = 100): Unit = {
    waitUntilTrue(() => {
      val records = consumer.poll(Duration.ofMillis(pollTimeoutMs))
      action(records)
    }, msg = msg, pause = 0L, waitTimeMs = waitTimeMs)
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

  /**
   * Invoke `assertions` until no AssertionErrors are thrown or `waitTime` elapses.
   *
   * This method is useful in cases where there may be some expected delay in a particular test condition that is
   * otherwise difficult to poll for. `computeUntilTrue` and `waitUntilTrue` should be preferred in cases where we can
   * easily wait on a condition before evaluating the assertions.
   */
  def tryUntilNoAssertionError[T](waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS, pause: Long = 100L)(assertions: => T): T = {
    val (either, _) = TestUtils.computeUntilTrue({
      try {
        val res = assertions
        Left(res)
      } catch {
        case ae: AssertionError => Right(ae)
      }
    }, waitTime = waitTime, pause = pause)(_.isLeft)

    either match {
      case Left(res) => res
      case Right(err) => throw err
    }
  }

  def findLeaderEpoch(brokerId: Int,
                      topicPartition: TopicPartition,
                      brokers: Iterable[KafkaBroker]): Int = {
    val leaderBroker = brokers.find(_.config.brokerId == brokerId)
    val leaderPartition = leaderBroker.flatMap(_.replicaManager.onlinePartition(topicPartition))
      .getOrElse(throw new AssertionError(s"Failed to find expected replica on broker $brokerId"))
    leaderPartition.getLeaderEpoch
  }

  def findFollowerId(topicPartition: TopicPartition,
                     brokers: Iterable[KafkaBroker]): Int = {
    val followerOpt = brokers.find { server =>
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
    * @param brokers The Kafka brokers.
    * @param timeout The amount of time waiting on this condition before assert to fail
    */
  def waitUntilBrokerMetadataIsPropagated[B <: KafkaBroker](
      brokers: Seq[B],
      timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Unit = {
    val expectedBrokerIds = brokers.map(_.config.brokerId).toSet
    waitUntilTrue(() => brokers.forall(server =>
      expectedBrokerIds == server.dataPlaneRequestProcessor.metadataCache.getAliveBrokers().map(_.id).toSet
    ), "Timed out waiting for broker metadata to propagate to all servers", timeout)
  }

  /**
   * Wait until the expected number of partitions is in the metadata cache in each broker.
   *
   * @param brokers The list of brokers that the metadata should reach to
   * @param topic The topic name
   * @param expectedNumPartitions The expected number of partitions
   * @return all partitions metadata
   */
  def waitForAllPartitionsMetadata[B <: KafkaBroker](
      brokers: Seq[B],
      topic: String,
      expectedNumPartitions: Int): Map[TopicPartition, UpdateMetadataPartitionState] = {
    waitUntilTrue(
      () => brokers.forall { broker =>
        if (expectedNumPartitions == 0) {
          broker.metadataCache.numPartitions(topic) == None
        } else {
          broker.metadataCache.numPartitions(topic) == Some(expectedNumPartitions)
        }
      },
      s"Topic [$topic] metadata not propagated after 60000 ms", waitTimeMs = 60000L)

    // since the metadata is propagated, we should get the same metadata from each server
    (0 until expectedNumPartitions).map { i =>
      new TopicPartition(topic, i) -> brokers.head.metadataCache.getPartitionInfo(topic, i).getOrElse(
          throw new IllegalStateException(s"Cannot get topic: $topic, partition: $i in server metadata cache"))
    }.toMap
  }

  /**
   * Wait until a valid leader is propagated to the metadata cache in each broker.
   * It assumes that the leader propagated to each broker is the same.
   *
   * @param brokers The list of brokers that the metadata should reach to
   * @param topic The topic name
   * @param partition The partition Id
   * @param timeout The amount of time waiting on this condition before assert to fail
   * @return The metadata of the partition.
   */
  def waitForPartitionMetadata[B <: KafkaBroker](
      brokers: Seq[B], topic: String, partition: Int,
      timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): UpdateMetadataPartitionState = {
    waitUntilTrue(
      () => brokers.forall { broker =>
        broker.metadataCache.getPartitionInfo(topic, partition) match {
          case Some(partitionState) => FetchRequest.isValidBrokerId(partitionState.leader)
          case _ => false
        }
      },
      "Partition [%s,%d] metadata not propagated after %d ms".format(topic, partition, timeout),
      waitTimeMs = timeout)

    brokers.head.metadataCache.getPartitionInfo(topic, partition).getOrElse(
      throw new IllegalStateException(s"Cannot get topic: $topic, partition: $partition in server metadata cache"))
  }

  /**
   * Wait until the kraft broker metadata have caught up to the controller, before calling this, we should make sure
   * the related metadata message has already been committed to the controller metadata log.
   */
  def ensureConsistentKRaftMetadata(
      brokers: Seq[KafkaBroker],
      controllerServer: ControllerServer,
      msg: String = "Timeout waiting for controller metadata propagating to brokers"
  ): Unit = {
    val controllerOffset = controllerServer.raftManager.replicatedLog.endOffset().offset - 1
    TestUtils.waitUntilTrue(
      () => {
        brokers.forall { broker =>
          val loader = broker.asInstanceOf[BrokerServer].sharedServer.loader
          loader == null || loader.lastAppliedOffset() >= controllerOffset
        }
      }, msg)
  }

  def awaitLeaderChange[B <: KafkaBroker](
      brokers: Seq[B],
      tp: TopicPartition,
      oldLeaderOpt: Option[Int] = None,
      expectedLeaderOpt: Option[Int] = None,
      timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
    def newLeaderExists: Option[Int] = {
      if (expectedLeaderOpt.isDefined) {
        debug(s"Checking leader that has changed to ${expectedLeaderOpt.get}")
        brokers.find { broker =>
          broker.config.brokerId == expectedLeaderOpt.get &&
            broker.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
        }.map(_.config.brokerId)

      } else if (oldLeaderOpt.isDefined) {
          debug(s"Checking leader that has changed from ${oldLeaderOpt}")
          brokers.find { broker =>
            broker.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
            broker.config.brokerId != oldLeaderOpt.get &&
              broker.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
          }.map(_.config.brokerId)

      } else {
        debug(s"Checking the elected leader")
        brokers.find { broker =>
            broker.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
        }.map(_.config.brokerId)
      }
    }

    waitUntilTrue(() => newLeaderExists.isDefined,
      s"Did not observe leader change for partition $tp after $timeout ms", waitTimeMs = timeout)

    newLeaderExists.get
  }

  def waitUntilLeaderIsKnown[B <: KafkaBroker](
      brokers: Seq[B],
      tp: TopicPartition,
      timeout: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Int = {
    def leaderIfExists: Option[Int] = {
      brokers.find { broker =>
        broker.replicaManager.onlinePartition(tp).exists(_.leaderLogIfLocal.isDefined)
      }.map(_.config.brokerId)
    }

    waitUntilTrue(() => leaderIfExists.isDefined,
      s"Partition $tp leaders not made yet after $timeout ms", waitTimeMs = timeout)

    leaderIfExists.get
  }

  def appendNonsenseToFile(file: File, size: Int): Unit = {
    val outputStream = Files.newOutputStream(file.toPath(), StandardOpenOption.APPEND)
    try {
      for (_ <- 0 until size)
        outputStream.write(random.nextInt(255))
    } finally outputStream.close()
  }

  // Note: Call this method in the test itself, rather than the @AfterEach method.
  // Because of the assert, if assertNoNonDaemonThreads fails, nothing after would be executed.
  def assertNoNonDaemonThreads(threadNamePrefix: String): Unit = {
    val nonDaemonThreads = Thread.getAllStackTraces.keySet.asScala.filter { t =>
      !t.isDaemon && t.isAlive && t.getName.startsWith(threadNamePrefix)
    }
    val threadCount = nonDaemonThreads.size
    assertEquals(0, threadCount, s"Found unexpected $threadCount NonDaemon threads=${nonDaemonThreads.map(t => t.getName).mkString(", ")}")
  }

  /**
   * Create new LogManager instance with default configuration for testing
   */
  def createLogManager(logDirs: Seq[File] = Seq.empty[File],
                       defaultConfig: LogConfig = new LogConfig(new Properties),
                       configRepository: ConfigRepository = new MockConfigRepository,
                       cleanerConfig: CleanerConfig = new CleanerConfig(false),
                       time: MockTime = new MockTime(),
                       interBrokerProtocolVersion: MetadataVersion = MetadataVersion.latestTesting,
                       recoveryThreadsPerDataDir: Int = 4,
                       transactionVerificationEnabled: Boolean = false,
                       log: Option[UnifiedLog] = None,
                       remoteStorageSystemEnable: Boolean = false,
                       initialTaskDelayMs: Long = ServerLogConfigs.LOG_INITIAL_TASK_DELAY_MS_DEFAULT): LogManager = {
    val logManager = new LogManager(logDirs = logDirs.map(_.getAbsoluteFile),
                   initialOfflineDirs = Array.empty[File],
                   configRepository = configRepository,
                   initialDefaultConfig = defaultConfig,
                   cleanerConfig = cleanerConfig,
                   recoveryThreadsPerDataDir = recoveryThreadsPerDataDir,
                   flushCheckMs = 1000L,
                   flushRecoveryOffsetCheckpointMs = 10000L,
                   flushStartOffsetCheckpointMs = 10000L,
                   retentionCheckMs = 1000L,
                   maxTransactionTimeoutMs = 5 * 60 * 1000,
                   producerStateManagerConfig = new ProducerStateManagerConfig(TransactionLogConfig.PRODUCER_ID_EXPIRATION_MS_DEFAULT, transactionVerificationEnabled),
                   producerIdExpirationCheckIntervalMs = TransactionLogConfig.PRODUCER_ID_EXPIRATION_CHECK_INTERVAL_MS_DEFAULT,
                   scheduler = time.scheduler,
                   time = time,
                   brokerTopicStats = new BrokerTopicStats,
                   logDirFailureChannel = new LogDirFailureChannel(logDirs.size),
                   keepPartitionMetadataFile = true,
                   interBrokerProtocolVersion = interBrokerProtocolVersion,
                   remoteStorageSystemEnable = remoteStorageSystemEnable,
                   initialTaskDelayMs = initialTaskDelayMs)

    if (log.isDefined) {
      val spyLogManager = Mockito.spy(logManager)
      Mockito.doReturn(log.get, Nil: _*).when(spyLogManager).getOrCreateLog(any(classOf[TopicPartition]), anyBoolean(), anyBoolean(), any(classOf[Option[Uuid]]), any(classOf[Option[Uuid]]))
      spyLogManager
    } else
      logManager
  }

  class MockAlterPartitionManager extends AlterPartitionManager {
    val isrUpdates: mutable.Queue[AlterPartitionItem] = new mutable.Queue[AlterPartitionItem]()
    val inFlight: AtomicBoolean = new AtomicBoolean(false)


    override def submit(
      topicPartition: TopicIdPartition,
      leaderAndIsr: LeaderAndIsr,
      controllerEpoch: Int
    ): CompletableFuture[LeaderAndIsr]= {
      val future = new CompletableFuture[LeaderAndIsr]()
      if (inFlight.compareAndSet(false, true)) {
        isrUpdates += AlterPartitionItem(
          topicPartition,
          leaderAndIsr,
          future,
          controllerEpoch
        )
      } else {
        future.completeExceptionally(new OperationNotAttemptedException(
          s"Failed to enqueue AlterIsr request for $topicPartition since there is already an inflight request"))
      }
      future
    }

    def completeIsrUpdate(newPartitionEpoch: Int): Unit = {
      if (inFlight.compareAndSet(true, false)) {
        val item = isrUpdates.dequeue()
        item.future.complete(item.leaderAndIsr.withPartitionEpoch(newPartitionEpoch))
      } else {
        fail("Expected an in-flight ISR update, but there was none")
      }
    }

    def failIsrUpdate(error: Errors): Unit = {
      if (inFlight.compareAndSet(true, false)) {
        val item = isrUpdates.dequeue()
        item.future.completeExceptionally(error.exception)
      } else {
        fail("Expected an in-flight ISR update, but there was none")
      }
    }
  }

  def createAlterIsrManager(): MockAlterPartitionManager = {
    new MockAlterPartitionManager()
  }

  def generateAndProduceMessages[B <: KafkaBroker](
      brokers: Seq[B],
      topic: String,
      numMessages: Int,
      acks: Int = -1): Seq[String] = {
    val values = (0 until numMessages).map(x =>  s"test-$x")
    val intSerializer = new IntegerSerializer()
    val records = values.zipWithIndex.map { case (v, i) =>
      new ProducerRecord(topic, intSerializer.serialize(topic, i), v.getBytes)
    }
    val producer = createProducer(plaintextBootstrapServers(brokers), acks = acks)
    try {
      val futures = records.map(producer.send)
      futures.foreach(_.get)
    } finally {
      producer.close()
    }

    val topics = records.map(_.topic).distinct
    debug(s"Sent ${records.size} messages for topics ${topics.mkString(",")}")
    values
  }

  def produceMessage[B <: KafkaBroker](
      brokers: Seq[B],
      topic: String,
      message: String,
      timestamp: java.lang.Long = null,
      deliveryTimeoutMs: Int = 30 * 1000,
      requestTimeoutMs: Int = 20 * 1000): Unit = {
    val producer = createProducer(plaintextBootstrapServers(brokers),
      deliveryTimeoutMs = deliveryTimeoutMs, requestTimeoutMs = requestTimeoutMs)
    try {
      producer.send(new ProducerRecord(topic, null, timestamp, topic.getBytes, message.getBytes)).get
    } finally {
      producer.close()
    }
  }

  def verifyTopicDeletion[B <: KafkaBroker](
      topic: String,
      numPartitions: Int,
      brokers: Seq[B]): Unit = {
    val topicPartitions = (0 until numPartitions).map(new TopicPartition(topic, _))
    // ensure that the topic-partition has been deleted from all brokers' replica managers
    waitUntilTrue(() =>
      brokers.forall(broker => topicPartitions.forall(tp => broker.replicaManager.onlinePartition(tp).isEmpty)),
      "Replica manager's should have deleted all of this topic's partitions")
    // ensure that logs from all replicas are deleted
    waitUntilTrue(() => brokers.forall(broker => topicPartitions.forall(tp => broker.logManager.getLog(tp).isEmpty)),
      "Replica logs not deleted after delete topic is complete")
    // ensure that topic is removed from all cleaner offsets
    waitUntilTrue(() => brokers.forall(broker => topicPartitions.forall { tp =>
      val checkpoints = broker.logManager.liveLogDirs.map { logDir =>
        new OffsetCheckpointFile(new File(logDir, "cleaner-offset-checkpoint"), null).read()
      }
      checkpoints.forall(checkpointsPerLogDir => !checkpointsPerLogDir.containsKey(tp))
    }), "Cleaner offset for deleted partition should have been removed")
    waitUntilTrue(() => brokers.forall(broker =>
      broker.config.logDirs.forall { logDir =>
        topicPartitions.forall { tp =>
          !new File(logDir, tp.topic + "-" + tp.partition).exists()
        }
      }
    ), "Failed to soft-delete the data to a delete directory")
    waitUntilTrue(() => brokers.forall(broker =>
      broker.config.logDirs.forall { logDir =>
        topicPartitions.forall { tp =>
          !util.Arrays.asList(new File(logDir).list()).asScala.exists { partitionDirectoryNames =>
            partitionDirectoryNames.exists { directoryName =>
              directoryName.startsWith(tp.topic + "-" + tp.partition) &&
                directoryName.endsWith(UnifiedLog.DeleteDirSuffix)
            }
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

  def waitAndVerifyAcls(expected: Set[AccessControlEntry],
                        authorizer: JAuthorizer,
                        resource: ResourcePattern,
                        accessControlEntryFilter: AccessControlEntryFilter = AccessControlEntryFilter.ANY): Unit = {
    val newLine = scala.util.Properties.lineSeparator

    val filter = new AclBindingFilter(resource.toFilter, accessControlEntryFilter)
    waitUntilTrue(() => authorizer.acls(filter).asScala.map(_.entry).toSet == expected,
      s"expected acls:${expected.mkString(newLine + "\t", newLine + "\t", newLine)}" +
        s"but got:${authorizer.acls(filter).asScala.map(_.entry).mkString(newLine + "\t", newLine + "\t", newLine)}",
        45000)
  }

  def consumeTopicRecords[K, V, B <: KafkaBroker](
      brokers: Seq[B],
      topic: String,
      numMessages: Int,
      groupProtocol: GroupProtocol,
      groupId: String = "group",
      securityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT,
      trustStoreFile: Option[File] = None,
      waitTime: Long = JTestUtils.DEFAULT_MAX_WAIT_MS): Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    val consumer = createConsumer(bootstrapServers(brokers, ListenerName.forSecurityProtocol(securityProtocol)),
      groupProtocol,
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

  def createTransactionalProducer[B <: KafkaBroker](
      transactionalId: String,
      brokers: Seq[B],
      batchSize: Int = 16384,
      transactionTimeoutMs: Long = 60000,
      maxBlockMs: Long = 60000,
      deliveryTimeoutMs: Int = 120000,
      requestTimeoutMs: Int = 30000,
      maxInFlight: Int = 5): KafkaProducer[Array[Byte], Array[Byte]] = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, plaintextBootstrapServers(brokers))
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
  def seedTopicWithNumberedRecords[B <: KafkaBroker](
      topic: String,
      numRecords: Int,
      brokers: Seq[B]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, plaintextBootstrapServers(brokers))
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
  def consumerPositions(consumer: Consumer[Array[Byte], Array[Byte]]) : Map[TopicPartition, OffsetAndMetadata]  = {
    val offsetsToCommit = new mutable.HashMap[TopicPartition, OffsetAndMetadata]()
    consumer.assignment.forEach { topicPartition =>
      offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
    }
    offsetsToCommit.toMap
  }

  def resetToCommittedPositions(consumer: Consumer[Array[Byte], Array[Byte]]): Unit = {
    val committed = consumer.committed(consumer.assignment).asScala.filter(_._2 != null).map { case (k, v) => k -> v.offset }

    consumer.assignment.forEach { topicPartition =>
      if (committed.contains(topicPartition))
        consumer.seek(topicPartition, committed(topicPartition))
      else
        consumer.seekToBeginning(Collections.singletonList(topicPartition))
    }
  }

  def incrementalAlterConfigs[B <: KafkaBroker](
      servers: Seq[B],
      adminClient: Admin,
      props: Properties,
      perBrokerConfig: Boolean,
      opType: OpType = OpType.SET): AlterConfigsResult  = {
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

  def assertLeader(client: Admin, topicPartition: TopicPartition, expectedLeader: Int): Unit = {
    waitForLeaderToBecome(client, topicPartition, Some(expectedLeader))
  }

  def assertNoLeader(client: Admin, topicPartition: TopicPartition): Unit = {
    waitForLeaderToBecome(client, topicPartition, None)
  }

  def waitForOnlineBroker(client: Admin, brokerId: Int): Unit = {
    waitUntilTrue(() => {
      val nodes = client.describeCluster().nodes().get()
      nodes.asScala.exists(_.id == brokerId)
    }, s"Timed out waiting for brokerId $brokerId to come online")
  }

  def waitForLeaderToBecome(
    client: Admin,
    topicPartition: TopicPartition,
    expectedLeaderOpt: Option[Int]
  ): Unit = {
    val topic = topicPartition.topic
    val partitionId = topicPartition.partition

    def currentLeader: Try[Option[Int]] = Try {
      val topicDescription = client.describeTopics(List(topic).asJava).allTopicNames.get.get(topic)
      topicDescription.partitions.asScala
        .find(_.partition == partitionId)
        .flatMap(partitionState => Option(partitionState.leader))
        .map(_.id)
    }

    val (lastLeaderCheck, isLeaderElected) = computeUntilTrue(currentLeader) {
      case Success(leaderOpt) => leaderOpt == expectedLeaderOpt
      case Failure(e: ExecutionException) if e.getCause.isInstanceOf[UnknownTopicOrPartitionException] => false
      case Failure(e) => throw e
    }

    assertTrue(isLeaderElected, s"Timed out waiting for leader to become $expectedLeaderOpt. " +
      s"Last metadata lookup returned leader = ${lastLeaderCheck.getOrElse("unknown")}")
  }

  def waitForBrokersOutOfIsr(client: Admin, partition: Set[TopicPartition], brokerIds: Set[Int]): Unit = {
    waitUntilTrue(
      () => {
        val description = client.describeTopics(partition.map(_.topic).asJava).allTopicNames.get.asScala
        val isr = description
          .values
          .flatMap(_.partitions.asScala.flatMap(_.isr.asScala))
          .map(_.id)
          .toSet

        brokerIds.intersect(isr).isEmpty
      },
      s"Expected brokers $brokerIds to no longer be in the ISR for $partition"
    )
  }

  def currentIsr(admin: Admin, partition: TopicPartition): Set[Int] = {
    val description = admin.describeTopics(Set(partition.topic).asJava)
      .allTopicNames
      .get
      .asScala

    description
      .values
      .flatMap(_.partitions.asScala.flatMap(_.isr.asScala))
      .map(_.id)
      .toSet
  }

  def waitForBrokersInIsr(client: Admin, partition: TopicPartition, brokerIds: Set[Int]): Unit = {
    waitUntilTrue(
      () => {
        val isr = currentIsr(client, partition)
        brokerIds.subsetOf(isr)
      },
      s"Expected brokers $brokerIds to be in the ISR for $partition"
    )
  }

  def assertBadConfigContainingMessage(props: Properties, expectedExceptionContainsText: String): Unit = {
    try {
      KafkaConfig.fromProps(props)
      fail("Expected illegal configuration but instead it was legal")
    } catch {
      case caught @ (_: ConfigException | _: IllegalArgumentException) =>
        assertTrue(
          caught.getMessage.contains(expectedExceptionContainsText),
          s""""${caught.getMessage}" doesn't contain "$expectedExceptionContainsText""""
        )
    }
  }

  def totalMetricValue(broker: KafkaBroker, metricName: String): Long = {
    totalMetricValue(broker.metrics, metricName)
  }

  def totalMetricValue(metrics: Metrics, metricName: String): Long = {
    val allMetrics = metrics.metrics
    val total = allMetrics.values().asScala.filter(_.metricName().name() == metricName)
      .foldLeft(0.0)((total, metric) => total + metric.metricValue.asInstanceOf[Double])
    total.toLong
  }

  def meterCount(metricName: String): Long = {
    meterCountOpt(metricName).getOrElse(fail(s"Unable to find metric $metricName"))
  }

  def meterCountOpt(metricName: String): Option[Long] = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (k, _) => k.getMBeanName.endsWith(metricName) }
      .values
      .headOption
      .map(_.asInstanceOf[Meter].count)
  }

  def metersCount(metricName: String): Long = {
    KafkaYammerMetrics.defaultRegistry.allMetrics.asScala
      .filter { case (k, _) => k.getMBeanName.endsWith(metricName) }
      .values.map {
        case histogram: Histogram => histogram.count()
        case meter: Meter => meter.count()
        case _ => 0
      }.sum
  }

  def clearYammerMetrics(): Unit = {
    for (metricName <- KafkaYammerMetrics.defaultRegistry.allMetrics.keySet.asScala)
      KafkaYammerMetrics.defaultRegistry.removeMetric(metricName)
  }

  /**
   * Find an Authorizer that we can call createAcls or deleteAcls on.
   */
  def pickAuthorizerForWrite[B <: KafkaBroker](
    brokers: Seq[B],
    controllers: Seq[ControllerServer],
  ): JAuthorizer = {
    if (controllers.isEmpty) {
      brokers.head.authorizer.get
    } else {
      var result: JAuthorizer = null
      TestUtils.retry(120000) {
        val active = controllers.filter(_.controller.isActive).head
        result = active.authorizer.get
      }
      result
    }
  }

  val anonymousAuthorizableContext = new AuthorizableRequestContext() {
    override def listenerName(): String = ""
    override def securityProtocol(): SecurityProtocol = SecurityProtocol.PLAINTEXT
    override def principal(): KafkaPrincipal = KafkaPrincipal.ANONYMOUS
    override def clientAddress(): InetAddress = null
    override def requestType(): Int = 0
    override def requestVersion(): Int = 0
    override def clientId(): String = ""
    override def correlationId(): Int = 0
  }

  def buildEnvelopeRequest(
    request: AbstractRequest,
    principalSerde: KafkaPrincipalSerde,
    requestChannelMetrics: RequestChannelMetrics,
    startTimeNanos: Long,
    dequeueTimeNanos: Long = -1,
    fromPrivilegedListener: Boolean = true
  ): RequestChannel.Request = {
    val clientId = "id"
    val listenerName = ListenerName.forSecurityProtocol(SecurityProtocol.PLAINTEXT)

    val requestHeader = new RequestHeader(request.apiKey, request.version, clientId, 0)
    val requestBuffer = request.serializeWithHeader(requestHeader)

    val envelopeHeader = new RequestHeader(ApiKeys.ENVELOPE, ApiKeys.ENVELOPE.latestVersion(), clientId, 0)
    val envelopeBuffer = new EnvelopeRequest.Builder(
      requestBuffer,
      principalSerde.serialize(KafkaPrincipal.ANONYMOUS),
      InetAddress.getLocalHost.getAddress
    ).build().serializeWithHeader(envelopeHeader)

    RequestHeader.parse(envelopeBuffer)

    val envelopeContext = new RequestContext(envelopeHeader, "1", InetAddress.getLocalHost, Optional.empty(),
      KafkaPrincipal.ANONYMOUS, listenerName, SecurityProtocol.PLAINTEXT, ClientInformation.EMPTY,
      fromPrivilegedListener, Optional.of(principalSerde))

    val envelopRequest = new RequestChannel.Request(
      processor = 1,
      context = envelopeContext,
      startTimeNanos = startTimeNanos,
      memoryPool = MemoryPool.NONE,
      buffer = envelopeBuffer,
      metrics = requestChannelMetrics,
      envelope = None
    )
    envelopRequest.requestDequeueTimeNanos = dequeueTimeNanos
    envelopRequest
  }

  class TestControllerRequestCompletionHandler(expectedResponse: Option[AbstractResponse] = None)
    extends ControllerRequestCompletionHandler {
    var actualResponse: Option[ClientResponse] = Option.empty
    val completed: AtomicBoolean = new AtomicBoolean(false)
    val timedOut: AtomicBoolean = new AtomicBoolean(false)

    override def onComplete(response: ClientResponse): Unit = {
      actualResponse = Some(response)
      expectedResponse.foreach { expected =>
        assertEquals(expected, response.responseBody())
      }
      completed.set(true)
    }

    override def onTimeout(): Unit = {
      timedOut.set(true)
    }
  }
}
