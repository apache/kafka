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
import java.util.{Collections, Optional, OptionalInt, Properties}
import java.util.concurrent.{CompletableFuture, TimeUnit}
import javax.security.auth.login.Configuration
import kafka.tools.StorageTool
import kafka.utils.{CoreUtils, Logging, TestInfoUtils, TestUtils}
import kafka.zk.{AdminZkClient, EmbeddedZookeeper, KafkaZkClient}
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.security.JaasUtils
import org.apache.kafka.common.security.auth.SecurityProtocol
import org.apache.kafka.common.utils.{Exit, Time}
import org.apache.kafka.metadata.bootstrap.BootstrapMetadata
import org.apache.kafka.common.metadata.FeatureLevelRecord
import org.apache.kafka.metadata.properties.MetaPropertiesEnsemble.VerificationFlag.{REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR}
import org.apache.kafka.metadata.properties.{MetaProperties, MetaPropertiesEnsemble, MetaPropertiesVersion}
import org.apache.kafka.raft.RaftConfig.{AddressSpec, InetAddressSpec}
import org.apache.kafka.server.common.{ApiMessageAndVersion, MetadataVersion}
import org.apache.kafka.server.fault.{FaultHandler, MockFaultHandler}
import org.apache.zookeeper.client.ZKClientConfig
import org.apache.zookeeper.{WatchedEvent, Watcher, ZooKeeper}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.{AfterAll, AfterEach, BeforeAll, BeforeEach, Tag, TestInfo}

import java.nio.file.{Files, Paths}
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.ListBuffer
import scala.collection.{Seq, immutable}
import scala.compat.java8.OptionConverters._

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
    CoreUtils.swallow(zkClient.close(), log)
    CoreUtils.swallow(zookeeper.shutdown(), log)
  }
}

class KRaftQuorumImplementation(
  val controllerServer: ControllerServer,
  val faultHandlerFactory: FaultHandlerFactory,
  val metadataDir: File,
  val controllerQuorumVotersFuture: CompletableFuture[util.Map[Integer, AddressSpec]],
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
      config.logDirs.foreach(loader.addLogDir(_))
      loader.addMetadataLogDir(config.metadataLogDir)
      val ensemble = loader.load()
      val copier = new MetaPropertiesEnsemble.Copier(ensemble)
      ensemble.emptyLogDirs().forEach(logDir => {
        copier.setLogDirProps(logDir, new MetaProperties.Builder().
          setVersion(MetaPropertiesVersion.V1).
          setClusterId(clusterId).
          setNodeId(config.nodeId).
          build())
      })
      copier.setPreWriteHandler((logDir, _, _) => {
        Files.createDirectories(Paths.get(logDir));
      })
      copier.writeLogDirChanges()
      copier.copy()
    }
    metaPropertiesEnsemble.verify(Optional.of(clusterId),
      OptionalInt.of(config.nodeId),
      util.EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR))
    val sharedServer = new SharedServer(config,
      metaPropertiesEnsemble,
      Time.SYSTEM,
      new Metrics(),
      controllerQuorumVotersFuture,
      faultHandlerFactory)
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

  protected def kraftControllerConfigs(): Seq[Properties] = {
    Seq(new Properties())
  }

  protected def metadataVersion: MetadataVersion = MetadataVersion.latest()

  private var testInfo: TestInfo = _
  private var implementation: QuorumImplementation = _

  val bootstrapRecords: ListBuffer[ApiMessageAndVersion] = ListBuffer()

  def isKRaftTest(): Boolean = {
    TestInfoUtils.isKRaft(testInfo)
  }

  def isZkMigrationTest(): Boolean = {
    TestInfoUtils.isZkMigrationTest(testInfo)
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
    val name = testInfo.getTestMethod.asScala
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

  def optionalMetadataRecords: Option[ArrayBuffer[ApiMessageAndVersion]] = None

  private def formatDirectories(directories: immutable.Seq[String],
                                metaProperties: MetaProperties): Unit = {
    val stream = new ByteArrayOutputStream()
    var out: PrintStream = null
    try {
      out = new PrintStream(stream)
      val bootstrapMetadata = StorageTool.buildBootstrapMetadata(metadataVersion,
                                                                 optionalMetadataRecords, "format command")
      if (StorageTool.formatCommand(out, directories, metaProperties, bootstrapMetadata, metadataVersion,
                                    ignoreFormatted = false) != 0) {
        throw new RuntimeException(stream.toString())
      }
      debug(s"Formatted storage directory(ies) ${directories}")
    } finally {
      if (out != null) out.close()
      stream.close()
    }
  }

  private def newKRaftQuorum(testInfo: TestInfo): KRaftQuorumImplementation = {
    val propsList = kraftControllerConfigs()
    if (propsList.size != 1) {
      throw new RuntimeException("Only one KRaft controller is supported for now.")
    }
    val props = propsList(0)
    props.setProperty(KafkaConfig.ServerMaxStartupTimeMsProp, TimeUnit.MINUTES.toMillis(10).toString)
    props.setProperty(KafkaConfig.ProcessRolesProp, "controller")
    if (props.getProperty(KafkaConfig.NodeIdProp) == null) {
      props.setProperty(KafkaConfig.NodeIdProp, "1000")
    }
    val nodeId = Integer.parseInt(props.getProperty(KafkaConfig.NodeIdProp))
    val metadataDir = TestUtils.tempDir()
    val metaProperties = new MetaProperties.Builder().
      setVersion(MetaPropertiesVersion.V1).
      setClusterId(Uuid.randomUuid().toString).
      setNodeId(nodeId).
      build()
    formatDirectories(immutable.Seq(metadataDir.getAbsolutePath), metaProperties)

    val metadataRecords = new util.ArrayList[ApiMessageAndVersion]
    metadataRecords.add(new ApiMessageAndVersion(new FeatureLevelRecord().
                        setName(MetadataVersion.FEATURE_NAME).
                        setFeatureLevel(metadataVersion.featureLevel()), 0.toShort));

    optionalMetadataRecords.foreach { metadataArguments =>
      for (record <- metadataArguments) metadataRecords.add(record)
    }

    val bootstrapMetadata = BootstrapMetadata.fromRecords(metadataRecords, "test harness")

    props.setProperty(KafkaConfig.MetadataLogDirProp, metadataDir.getAbsolutePath)
    val proto = controllerListenerSecurityProtocol.toString
    props.setProperty(KafkaConfig.ListenerSecurityProtocolMapProp, s"CONTROLLER:${proto}")
    props.setProperty(KafkaConfig.ListenersProp, s"CONTROLLER://localhost:0")
    props.setProperty(KafkaConfig.ControllerListenerNamesProp, "CONTROLLER")
    props.setProperty(KafkaConfig.QuorumVotersProp, s"${nodeId}@localhost:0")
    val config = new KafkaConfig(props)
    val controllerQuorumVotersFuture = new CompletableFuture[util.Map[Integer, AddressSpec]]
    val metaPropertiesEnsemble = new MetaPropertiesEnsemble.Loader().
      addMetadataLogDir(metadataDir.getAbsolutePath).
      load()
    metaPropertiesEnsemble.verify(Optional.of(metaProperties.clusterId().get()),
      OptionalInt.of(nodeId),
      util.EnumSet.of(REQUIRE_AT_LEAST_ONE_VALID, REQUIRE_METADATA_LOG_DIR))
    val sharedServer = new SharedServer(config,
      metaPropertiesEnsemble,
      Time.SYSTEM,
      new Metrics(),
      controllerQuorumVotersFuture,
      faultHandlerFactory)
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
          controllerQuorumVotersFuture.complete(Collections.singletonMap(nodeId,
            new InetAddressSpec(new InetSocketAddress("localhost", port))))
        }
      })
      controllerServer.startup()
    } catch {
      case e: Throwable =>
        if (controllerServer != null) CoreUtils.swallow(controllerServer.shutdown(), this)
        CoreUtils.swallow(sharedServer.stopForController(), this)
        throw e
    }
    new KRaftQuorumImplementation(controllerServer,
      faultHandlerFactory,
      metadataDir,
      controllerQuorumVotersFuture,
      metaProperties.clusterId.get(),
      this,
      faultHandler)
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
        new ZKClientConfig)
      adminZkClient = new AdminZkClient(zkClient)
    } catch {
      case t: Throwable =>
        CoreUtils.swallow(zookeeper.shutdown(), this)
        if (zkClient != null) CoreUtils.swallow(zkClient.close(), this)
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
    Exit.resetExitProcedure()
    Exit.resetHaltProcedure()
    if (implementation != null) {
      implementation.shutdown()
    }
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
