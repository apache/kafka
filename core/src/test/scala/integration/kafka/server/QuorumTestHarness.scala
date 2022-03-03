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

package kafka.server

import java.io.{ByteArrayOutputStream, File, PrintStream}
import java.net.InetSocketAddress
import java.util
import java.util.{Collections, Properties}
import java.util.concurrent.CompletableFuture

import javax.security.auth.login.Configuration
import kafka.raft.KafkaRaftManager
import kafka.tools.StorageTool
import kafka.utils.{CoreUtils, Logging, TestInfoUtils, TestUtils}
import kafka.zk.{AdminZkClient, EmbeddedZookeeper, KafkaZkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.{TopicPartition, Uuid}
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{Exit, Time}
import org.apache.kafka.metadata.MetadataRecordSerde
import org.apache.kafka.raft.RaftConfig.{AddressSpec, InetAddressSpec}
import org.apache.kafka.server.common.ApiMessageAndVersion
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach, Tag, TestInfo}

import scala.collection.{Seq, immutable}

trait QuorumImplementation {
  def createBroker(config: KafkaConfig,
                   time: Time,
                   startup: Boolean): KafkaBroker

  def shutdown(): Unit
}

class ZooKeeperQuorumImplementation(val zookeeper: EmbeddedZookeeper,
                                    val zkClient: KafkaZkClient,
                                    val adminZkClient: AdminZkClient,
                                    val log: Logging) extends QuorumImplementation {
  override def createBroker(config: KafkaConfig,
                            time: Time,
                            startup: Boolean): KafkaBroker = {
    val server = new KafkaServer(config, time, None, false)
    if (startup) server.startup()
    server
  }

  override def shutdown(): Unit = {
    CoreUtils.swallow(zkClient.close(), log)
    CoreUtils.swallow(zookeeper.shutdown(), log)
  }
}

class KRaftQuorumImplementation(val raftManager: KafkaRaftManager[ApiMessageAndVersion],
                                val controllerServer: ControllerServer,
                                val metadataDir: File,
                                val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]],
                                val clusterId: String,
                                val log: Logging) extends QuorumImplementation {
  override def createBroker(config: KafkaConfig,
                            time: Time,
                            startup: Boolean): KafkaBroker = {
    val broker = new BrokerServer(config = config,
      metaProps = new MetaProperties(clusterId, config.nodeId),
      raftManager = raftManager,
      time = time,
      metrics = new Metrics(),
      threadNamePrefix = Some("Broker%02d_".format(config.nodeId)),
      initialOfflineDirs = Seq(),
      controllerQuorumVotersFuture = controllerQuorumVotersFuture,
      supportedFeatures = Collections.emptyMap())
    if (startup) broker.startup()
    broker
  }

  override def shutdown(): Unit = {
    CoreUtils.swallow(raftManager.shutdown(), log)
    CoreUtils.swallow(controllerServer.shutdown(), log)
  }
}

@Tag("integration")
abstract class QuorumTestHarness extends Logging {
  val zkConnectionTimeout = 10000
  val zkSessionTimeout = 15000 // Allows us to avoid ZK session expiration due to GC up to 2/3 * 15000ms = 10 secs
  val zkMaxInFlightRequests = Int.MaxValue

  protected def zkAclsEnabled: Option[Boolean] = None

  /**
   * When in KRaft mode, the security protocol to use for the controller listener.
   * Can be overridden by subclasses.
   */
  protected val controllerListenerSecurityProtocol: SecurityProtocol = SecurityProtocol.PLAINTEXT

  protected def kraftControllerConfigs(): Seq[Properties] = {
    Seq(new Properties())
  }

  private var implementation: QuorumImplementation = null

  def isKRaftTest(): Boolean = implementation.isInstanceOf[KRaftQuorumImplementation]

  def checkIsZKTest(): Unit = {
    if (isKRaftTest()) {
      throw new RuntimeException("This function can't be accessed when running the test " +
        "in KRaft mode. ZooKeeper mode is required.")
    }
  }

  def checkIsKRaftTest(): Unit = {
    if (!isKRaftTest()) {
      throw new RuntimeException("This function can't be accessed when running the test " +
        "in ZooKeeper mode. KRaft mode is required.")
    }
  }

  private def asZk(): ZooKeeperQuorumImplementation = {
    checkIsZKTest()
    implementation.asInstanceOf[ZooKeeperQuorumImplementation]
  }

  private def asKRaft(): KRaftQuorumImplementation = {
    checkIsKRaftTest()
    implementation.asInstanceOf[KRaftQuorumImplementation]
  }

  def zookeeper: EmbeddedZookeeper = asZk().zookeeper

  def zkClient: KafkaZkClient = asZk().zkClient

  def zkClientOrNull: KafkaZkClient = if (isKRaftTest()) null else asZk().zkClient

  def adminZkClient: AdminZkClient = asZk().adminZkClient

  def zkPort: Int = asZk().zookeeper.port

  def zkConnect: String = s"127.0.0.1:$zkPort"

  def zkConnectOrNull: String = if (isKRaftTest()) null else zkConnect

  def controllerServer: ControllerServer = asKRaft().controllerServer

  def controllerServers: Seq[ControllerServer] = {
    if (isKRaftTest()) {
      Seq(asKRaft().controllerServer)
    } else {
      Seq()
    }
  }

  // Note: according to the junit documentation: "JUnit Jupiter does not guarantee the execution
  // order of multiple @BeforeEach methods that are declared within a single test class or test
  // interface." Therefore, if you have things you would like to do before each test case runs, it
  // is best to override this function rather than declaring a new @BeforeEach function.
  // That way you control the initialization order.
  @BeforeEach
  def setUp(testInfo: TestInfo): Unit = {
    Exit.setExitProcedure((code, message) => {
      try {
        throw new RuntimeException(s"exit(${code}, ${message}) called!")
      } catch {
        case e: Throwable => error("test error", e)
          throw e
      } finally {
        tearDown()
      }
    })
    Exit.setHaltProcedure((code, message) => {
      try {
        throw new RuntimeException(s"halt(${code}, ${message}) called!")
      } catch {
        case e: Throwable => error("test error", e)
          throw e
      } finally {
        tearDown()
      }
    })
    val name = if (testInfo.getTestMethod().isPresent()) {
      testInfo.getTestMethod().get().toString()
    } else {
      "[unspecified]"
    }
    if (TestInfoUtils.isKRaft(testInfo)) {
      info(s"Running KRAFT test ${name}")
      implementation = newKRaftQuorum(testInfo)
    } else {
      info(s"Running ZK test ${name}")
      implementation = newZooKeeperQuorum()
    }
  }

  def createBroker(config: KafkaConfig,
                   time: Time = Time.SYSTEM,
                   startup: Boolean = true): KafkaBroker = {
    implementation.createBroker(config, time, startup)
  }

  def shutdownZooKeeper(): Unit = asZk().shutdown()

  def shutdownKRaftController(): Unit = {
    // Note that the RaftManager instance is left running; it will be shut down in tearDown()
    val kRaftQuorumImplementation = asKRaft()
    CoreUtils.swallow(kRaftQuorumImplementation.controllerServer.shutdown(), kRaftQuorumImplementation.log)
  }

  private def formatDirectories(directories: immutable.Seq[String],
                                metaProperties: MetaProperties): Unit = {
    val stream = new ByteArrayOutputStream()
    var out: PrintStream = null
    try {
      out = new PrintStream(stream)
      if (StorageTool.formatCommand(out, directories, metaProperties, false) != 0) {
        throw new RuntimeException(stream.toString())
      }
      debug(s"Formatted storage directory(ies) ${directories}")
    } finally {
      if (out != null) out.close()
      stream.close()
    }
  }

  private def newKRaftQuorum(testInfo: TestInfo): KRaftQuorumImplementation = {
    val clusterId = Uuid.randomUuid().toString
    val metadataDir = TestUtils.tempDir()
    val metaProperties = new MetaProperties(clusterId, 0)
    formatDirectories(immutable.Seq(metadataDir.getAbsolutePath()), metaProperties)
    val controllerMetrics = new Metrics()
    val propsList = kraftControllerConfigs()
    if (propsList.size != 1) {
      throw new RuntimeException("Only one KRaft controller is supported for now.")
    }
    val props = propsList(0)
    props.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    props.setProperty(KafkaConfig.NodeIdProp, "1000")
    props.setProperty(KafkaConfig.MetadataLogDirProp, metadataDir.getAbsolutePath())
    val proto = controllerListenerSecurityProtocol.toString()
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"CONTROLLER:${proto}")
    props.setProperty(KafkaConfig.ListenersProp, s"CONTROLLER://localhost:0")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    props.setProperty(KafkaConfig.QuorumVotersProp, "1000@localhost:0")
    val config = new KafkaConfig(props)
    val threadNamePrefix = "Controller_" + testInfo.getDisplayName
    val controllerQuorumVotersFuture = new CompletableFuture[util.Map[Integer, AddressSpec]]
    val raftManager = new KafkaRaftManager(
      metaProperties = metaProperties,
      config = config,
      recordSerde = MetadataRecordSerde.INSTANCE,
      topicPartition = new TopicPartition(KafkaRaftServer.MetadataTopic, 0),
      topicId = KafkaRaftServer.MetadataTopicId,
      time = Time.SYSTEM,
      metrics = controllerMetrics,
      threadNamePrefixOpt = Option(threadNamePrefix),
      controllerQuorumVotersFuture = controllerQuorumVotersFuture)
    var controllerServer: ControllerServer = null
    try {
      controllerServer = new ControllerServer(
        metaProperties = metaProperties,
        config = config,
        raftManager = raftManager,
        time = Time.SYSTEM,
        metrics = controllerMetrics,
        threadNamePrefix = Option(threadNamePrefix),
        controllerQuorumVotersFuture = controllerQuorumVotersFuture,
        configSchema = KafkaRaftServer.configSchema,
      )
      controllerServer.socketServerFirstBoundPortFuture.whenComplete((port, e) => {
        if (e != null) {
          error("Error completing controller socket server future", e)
          controllerQuorumVotersFuture.completeExceptionally(e)
        } else {
          controllerQuorumVotersFuture.complete(Collections.singletonMap(1000,
            new InetAddressSpec(new InetSocketAddress("localhost", port))))
        }
      })
      controllerServer.startup()
      raftManager.startup()
    } catch {
      case e: Throwable =>
        CoreUtils.swallow(raftManager.shutdown(), this)
        if (controllerServer != null) CoreUtils.swallow(controllerServer.shutdown(), this)
        throw e
    }
    new KRaftQuorumImplementation(raftManager,
      controllerServer,
      metadataDir,
      controllerQuorumVotersFuture,
      clusterId,
      this)
  }

  private def newZooKeeperQuorum(): ZooKeeperQuorumImplementation = {
    val zookeeper = new EmbeddedZookeeper()
    var zkClient: KafkaZkClient = null
    var adminZkClient: AdminZkClient = null
    try {
      zkClient = KafkaZkClient(s"127.0.0.1:${zookeeper.port}",
        zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled),
        zkSessionTimeout,
        zkConnectionTimeout,
        zkMaxInFlightRequests,
        Time.SYSTEM,
        name = "ZooKeeperTestHarness",
        new ZKClientConfig)
      adminZkClient = new AdminZkClient(zkClient)
    } catch {
      case t: Throwable =>
        CoreUtils.swallow(zookeeper.shutdown(), this)
        if (zkClient != null) CoreUtils.swallow(zkClient.close(), this)
        throw t
    }
    new ZooKeeperQuorumImplementation(zookeeper,
      zkClient,
      adminZkClient,
      this)
  }

  @AfterEach
  def tearDown(): Unit = {
    Exit.resetExitProcedure()
    Exit.resetHaltProcedure()
    if (implementation != null) {
      implementation.shutdown()
    }
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    Configuration.setConfiguration(null)
  }

  // Trigger session expiry by reusing the session id in another client
  def createZooKeeperClientToTriggerSessionExpiry(zooKeeper: ZooKeeper): ZooKeeper = {
    val dummyWatcher = new Watcher {
      override def process(event: WatchedEvent): Unit = {}
    }
    val anotherZkClient = new ZooKeeper(zkConnect, 1000, dummyWatcher,
      zooKeeper.getSessionId,
      zooKeeper.getSessionPasswd)
    assertNull(anotherZkClient.exists("/nonexistent", false)) // Make sure new client works
    anotherZkClient
  }
}

object QuorumTestHarness {
  val ZkClientEventThreadSuffix = "-EventThread"

  /**
   * Verify that a previous test that doesn't use QuorumTestHarness hasn't left behind an unexpected thread.
   * This assumes that brokers, ZooKeeper clients, producers and consumers are not created in another @BeforeClass,
   * which is true for core tests where this harness is used.
   */
  @BeforeAll
  def setUpClass(): Unit = {
    TestUtils.verifyNoUnexpectedThreads("@BeforeAll")
  }

  /**
   * Verify that tests from the current test class using QuorumTestHarness haven't left behind an unexpected thread
   */
  @AfterAll
  def tearDownClass(): Unit = {
    TestUtils.verifyNoUnexpectedThreads("@AfterAll")
  }
}
