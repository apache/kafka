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

import java.io.File
import java.net.InetSocketAddress
import java.util
import java.util.{Collections, Optional, OptionalInt, Properties}
import java.util.concurrent.{CompletableFuture, TimeUnit}
import javax.security.auth.login.Configuration
import kafka.utils.{CoreUtils, Logging, TestInfoUtils, TestUtils}
import kafka.zk.{AdminZkClient, EmbeddedZookeeper, KafkaZkClient}
import org.apache.kafka.clients.consumer.GroupProtocol
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{Exit, Time, Utils}
import org.apache.kafka.common.{DirectoryId, Uuid}
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag.{REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion}
import org.apache.kafka.metadata.storage.Formatter
import org.apache.kafka.network.SocketServerConfigs
import org.apache.kafka.raft.QuorumConfig
import org.apache.kafka.server.common.MetadataVersion
import org.apache.kafka.server.config.{KRaftConfigs, ServerConfigs, ServerLogConfigs}
import org.apache.kafka.server.fault.{FaultHandler, MockFaultHandler}
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach, Tag, TestInfo}

import java.nio.file.{Files, Paths}
import scala.collection.Seq
import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters.RichOptional

trait QuorumImplementation {
  def createBroker(
    config: KafkaConfig,
    time: Time = Time.SYSTEM,
    startup: Boolean = true,
    threadNamePrefix: Option[String] = None,
  ): KafkaBroker

  def shutdown(): Unit
}

class ZooKeeperQuorumImplementation(
  val zookeeper: EmbeddedZookeeper,
  val zkConnect: String,
  val zkClient: KafkaZkClient,
  val adminZkClient: AdminZkClient,
  val log: Logging
) extends QuorumImplementation {
  override def createBroker(
    config: KafkaConfig,
    time: Time,
    startup: Boolean,
    threadNamePrefix: Option[String],
  ): KafkaBroker = {
    val server = new KafkaServer(config, time, threadNamePrefix, false)
    if (startup) server.startup()
    server
  }

  override def shutdown(): Unit = {
    Utils.closeQuietly(zkClient, "zk client")
    CoreUtils.swallow(zookeeper.shutdown(), log)
  }
}

class KRaftQuorumImplementation(
  val controllerServer: ControllerServer,
  val faultHandlerFactory: FaultHandlerFactory,
  val metadataDir: File,
  val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, InetSocketAddress]],
  val clusterId: String,
  val log: Logging,
  val faultHandler: FaultHandler
) extends QuorumImplementation {
  override def createBroker(
    config: KafkaConfig,
    time: Time,
    startup: Boolean,
    threadNamePrefix: Option[String],
  ): KafkaBroker = {
    val metaPropertiesEnsemble = {
      val loader = new MetaPropertiesEnsemble.Loader()
      loader.addLogDirs(config.logDirs.asJava)
      loader.addMetadataLogDir(config.metadataLogDir)
      val ensemble = loader.load()
      val copier = new MetaPropertiesEnsemble.Copier(ensemble)
      ensemble.emptyLogDirs().forEach(logDir => {
        copier.setLogDirProps(logDir, new MetaProperties.Builder().
          setVersion(MetaPropertiesVersion.V1).
          setClusterId(clusterId).
          setNodeId(config.nodeId).
          setDirectoryId(DirectoryId.random()).
          build())
      })
      copier.setPreWriteHandler((logDir, _, _) => {
        Files.createDirectories(Paths.get(logDir))
      })
      copier.writeLogDirChanges()
      copier.copy()
    }
    metaPropertiesEnsemble.verify(Optional.of(clusterId),
      OptionalInt.of(config.nodeId),
      util.EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR))
    val sharedServer = new SharedServer(
      config,
      metaPropertiesEnsemble,
      time,
      new Metrics(),
      controllerQuorumVotersFuture,
      controllerQuorumVotersFuture.get().values(),
      faultHandlerFactory
    )
    var broker: BrokerServer = null
    try {
      broker = new BrokerServer(sharedServer)
      if (startup) broker.startup()
      broker
    } catch {
      case e: Throwable => {
        if (broker != null) CoreUtils.swallow(broker.shutdown(), log)
        CoreUtils.swallow(sharedServer.stopForBroker(), log)
        throw e
      }
    }
  }

  override def shutdown(): Unit = {
    CoreUtils.swallow(controllerServer.shutdown(), log)
  }
}

class QuorumTestHarnessFaultHandlerFactory(
  val faultHandler: MockFaultHandler
) extends FaultHandlerFactory {
  override def build(
    name: String,
    fatal: Boolean,
    action: Runnable
  ): FaultHandler = faultHandler
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

  protected def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    Seq(new Properties())
  }

  protected def metadataVersion: MetadataVersion = MetadataVersion.latestTesting()

  private var testInfo: TestInfo = _
  protected var implementation: QuorumImplementation = _

  def isKRaftTest(): Boolean = {
    TestInfoUtils.isKRaft(testInfo)
  }

  def isZkMigrationTest(): Boolean = {
    TestInfoUtils.isZkMigrationTest(testInfo)
  }

  def isShareGroupTest(): Boolean = {
    TestInfoUtils.isShareGroupTest(testInfo)
  }

  def maybeGroupProtocolSpecified(testInfo: TestInfo): Option[GroupProtocol] = {
    TestInfoUtils.maybeGroupProtocolSpecified(testInfo)
  }

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

  val faultHandlerFactory = new QuorumTestHarnessFaultHandlerFactory(new MockFaultHandler("quorumTestHarnessFaultHandler"))

  val faultHandler = faultHandlerFactory.faultHandler

  // Note: according to the junit documentation: "JUnit Jupiter does not guarantee the execution
  // order of multiple @BeforeEach methods that are declared within a single test class or test
  // interface." Therefore, if you have things you would like to do before each test case runs, it
  // is best to override this function rather than declaring a new @BeforeEach function.
  // That way you control the initialization order.
  @BeforeEach
  def setUp(testInfo: TestInfo): Unit = {
    this.testInfo = testInfo
    Exit.setExitProcedure((code, message) => {
      try {
        throw new RuntimeException(s"exit($code, $message) called!")
      } catch {
        case e: Throwable => error("test error", e)
          throw e
      } finally {
        tearDown()
      }
    })
    Exit.setHaltProcedure((code, message) => {
      try {
        throw new RuntimeException(s"halt($code, $message) called!")
      } catch {
        case e: Throwable => error("test error", e)
          throw e
      } finally {
        tearDown()
      }
    })
    val name = testInfo.getTestMethod.toScala
      .map(_.toString)
      .getOrElse("[unspecified]")
    if (TestInfoUtils.isKRaft(testInfo)) {
      info(s"Running KRAFT test $name")
      implementation = newKRaftQuorum(testInfo)
    } else {
      info(s"Running ZK test $name")
      implementation = newZooKeeperQuorum()
    }
  }

  def createBroker(
    config: KafkaConfig,
    time: Time = Time.SYSTEM,
    startup: Boolean = true,
    threadNamePrefix: Option[String] = None
  ): KafkaBroker = {
    implementation.createBroker(config, time, startup, threadNamePrefix)
  }

  def shutdownZooKeeper(): Unit = asZk().shutdown()

  def shutdownKRaftController(): Unit = {
    // Note that the RaftManager instance is left running; it will be shut down in tearDown()
    val kRaftQuorumImplementation = asKRaft()
    CoreUtils.swallow(kRaftQuorumImplementation.controllerServer.shutdown(), kRaftQuorumImplementation.log)
  }

  def addFormatterSettings(formatter: Formatter): Unit = {}

  private def newKRaftQuorum(testInfo: TestInfo): KRaftQuorumImplementation = {
    newKRaftQuorum(new Properties())
  }

  protected def extraControllerSecurityProtocols(): Seq[SecurityProtocol] = {
    Seq.empty
  }

  protected def newKRaftQuorum(overridingProps: Properties): KRaftQuorumImplementation = {
    val propsList = kraftControllerConfigs(testInfo)
    if (propsList.size != 1) {
      throw new RuntimeException("Only one KRaft controller is supported for now.")
    }
    val props = propsList.head
    props.putAll(overridingProps)
    props.setProperty(KRaftConfigs.SERVER_MAX_STARTUP_TIME_MS_CONFIG, TimeUnit.MINUTES.toMillis(10).toString)
    props.setProperty(KRaftConfigs.PROCESS_ROLES_CONFIG, "controller")
    props.setProperty(ServerConfigs.UNSTABLE_FEATURE_VERSIONS_ENABLE_CONFIG, "true")
    if (props.getProperty(KRaftConfigs.NODE_ID_CONFIG) == null) {
      props.setProperty(KRaftConfigs.NODE_ID_CONFIG, "1000")
    }
    val nodeId = Integer.parseInt(props.getProperty(KRaftConfigs.NODE_ID_CONFIG))
    val metadataDir = TestUtils.tempDir()
    props.setProperty(KRaftConfigs.METADATA_LOG_DIR_CONFIG, metadataDir.getAbsolutePath)
    val proto = controllerListenerSecurityProtocol.toString
    val securityProtocolMaps = extraControllerSecurityProtocols().map(sc => sc + ":" + sc).mkString(",")
    val listeners = extraControllerSecurityProtocols().map(sc => sc + "://localhost:0").mkString(",")
    val listenerNames = extraControllerSecurityProtocols().mkString(",")
    props.setProperty(SocketServerConfigs.LISTENER_SECURITY_PROTOCOL_MAP_CONFIG, s"CONTROLLER:$proto,$securityProtocolMaps")
    props.setProperty(SocketServerConfigs.LISTENERS_CONFIG, s"CONTROLLER://localhost:0,$listeners")
    props.setProperty(KRaftConfigs.CONTROLLER_LISTENER_NAMES_CONFIG, s"CONTROLLER,$listenerNames")
    props.setProperty(QuorumConfig.QUORUM_VOTERS_CONFIG, s"$nodeId@localhost:0")
    // Setting the configuration to the same value set on the brokers via TestUtils to keep KRaft based and Zk based controller configs are consistent.
    props.setProperty(ServerLogConfigs.LOG_DELETE_DELAY_MS_CONFIG, "1000")
    val config = new KafkaConfig(props)

    val formatter = new Formatter().
      setClusterId(Uuid.randomUuid().toString).
      setNodeId(nodeId)
    formatter.addDirectory(metadataDir.getAbsolutePath)
    formatter.setReleaseVersion(metadataVersion)
    formatter.setUnstableFeatureVersionsEnabled(true)
    formatter.setControllerListenerName(config.controllerListenerNames.head)
    formatter.setMetadataLogDirectory(config.metadataLogDir)
    addFormatterSettings(formatter)
    formatter.run()
    val bootstrapMetadata = formatter.bootstrapMetadata()

    val controllerQuorumVotersFuture = new CompletableFuture[util.Map[Integer, InetSocketAddress]]
    val metaPropertiesEnsemble = new MetaPropertiesEnsemble.Loader().
      addMetadataLogDir(metadataDir.getAbsolutePath).
      load()
    metaPropertiesEnsemble.verify(Optional.of(formatter.clusterId()),
      OptionalInt.of(nodeId),
      util.EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR))
    val sharedServer = new SharedServer(
      config,
      metaPropertiesEnsemble,
      Time.SYSTEM,
      new Metrics(),
      controllerQuorumVotersFuture,
      Collections.emptyList(),
      faultHandlerFactory
    )
    var controllerServer: ControllerServer = null
    try {
      controllerServer = new ControllerServer(
        sharedServer,
        KafkaRaftServer.configSchema,
        bootstrapMetadata
      )
      controllerServer.socketServerFirstBoundPortFuture.whenComplete((port, e) => {
        if (e != null) {
          error("Error completing controller socket server future", e)
          controllerQuorumVotersFuture.completeExceptionally(e)
        } else {
          controllerQuorumVotersFuture.complete(
            Collections.singletonMap(nodeId, new InetSocketAddress("localhost", port))
          )
        }
      })
      controllerServer.startup()
    } catch {
      case e: Throwable =>
        if (controllerServer != null) CoreUtils.swallow(controllerServer.shutdown(), this)
        CoreUtils.swallow(sharedServer.stopForController(), this)
        throw e
    }
    new KRaftQuorumImplementation(
      controllerServer,
      faultHandlerFactory,
      metadataDir,
      controllerQuorumVotersFuture,
      formatter.clusterId(),
      this,
      faultHandler
    )
  }

  private def newZooKeeperQuorum(): ZooKeeperQuorumImplementation = {
    val zookeeper = new EmbeddedZookeeper()
    var zkClient: KafkaZkClient = null
    var adminZkClient: AdminZkClient = null
    val zkConnect = s"127.0.0.1:${zookeeper.port}"
    try {
      zkClient = KafkaZkClient(
        zkConnect,
        zkAclsEnabled.getOrElse(JaasUtils.isZkSaslEnabled),
        zkSessionTimeout,
        zkConnectionTimeout,
        zkMaxInFlightRequests,
        Time.SYSTEM,
        name = "ZooKeeperTestHarness",
        new ZKClientConfig,
        enableEntityConfigControllerCheck = false)
      adminZkClient = new AdminZkClient(zkClient)
    } catch {
      case t: Throwable =>
        CoreUtils.swallow(zookeeper.shutdown(), this)
        Utils.closeQuietly(zkClient, "zk client")
        throw t
    }
    new ZooKeeperQuorumImplementation(
      zookeeper,
      zkConnect,
      zkClient,
      adminZkClient,
      this
    )
  }

  @AfterEach
  def tearDown(): Unit = {
    if (implementation != null) {
      implementation.shutdown()
    }
    Exit.resetExitProcedure()
    Exit.resetHaltProcedure()
    TestUtils.clearYammerMetrics()
    System.clearProperty(JaasUtils.JAVA_LOGIN_CONFIG_PARAM)
    Configuration.setConfiguration(null)
    faultHandler.maybeRethrowFirstException()
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
