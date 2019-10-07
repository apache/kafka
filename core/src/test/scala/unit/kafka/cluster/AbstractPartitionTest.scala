package unit.kafka.cluster

import java.io.File
import java.util.Properties

import kafka.api.ApiVersion
import kafka.cluster.{DelayedOperations, Partition, PartitionStateStore}
import kafka.log.{CleanerConfig, LogConfig, LogManager}
import kafka.server.{Defaults, MetadataCache}
import kafka.server.checkpoints.OffsetCheckpoints
import kafka.utils.{MockTime, TestUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.utils.Utils
import org.junit.{After, Before}
import org.mockito.ArgumentMatchers
import org.mockito.Mockito.{mock, when}

class AbstractPartitionTest {

  val brokerId = 101
  val topicPartition = new TopicPartition("test-topic", 0)
  val time = new MockTime()
  var tmpDir: File = _
  var logDir1: File = _
  var logDir2: File = _
  var logManager: LogManager = _
  var logConfig: LogConfig = _
  val stateStore: PartitionStateStore = mock(classOf[PartitionStateStore])
  val delayedOperations: DelayedOperations = mock(classOf[DelayedOperations])
  val metadataCache: MetadataCache = mock(classOf[MetadataCache])
  val offsetCheckpoints: OffsetCheckpoints = mock(classOf[OffsetCheckpoints])
  var partition: Partition = _

  @Before
  def setup(): Unit = {
    TestUtils.clearYammerMetrics()

    val logProps = createLogProperties(Map.empty)
    logConfig = LogConfig(logProps)

    tmpDir = TestUtils.tempDir()
    logDir1 = TestUtils.randomPartitionLogDir(tmpDir)
    logDir2 = TestUtils.randomPartitionLogDir(tmpDir)
    logManager = TestUtils.createLogManager(
      logDirs = Seq(logDir1, logDir2), defaultConfig = logConfig, CleanerConfig(enableCleaner = false), time)
    logManager.startup()

    partition = new Partition(topicPartition,
      replicaLagTimeMaxMs = Defaults.ReplicaLagTimeMaxMs,
      interBrokerProtocolVersion = ApiVersion.latestVersion,
      localBrokerId = brokerId,
      time,
      stateStore,
      delayedOperations,
      metadataCache,
      logManager)

    when(stateStore.fetchTopicConfig()).thenReturn(createLogProperties(Map.empty))
    when(offsetCheckpoints.fetch(ArgumentMatchers.anyString, ArgumentMatchers.eq(topicPartition)))
      .thenReturn(None)
  }

  def createLogProperties(overrides: Map[String, String]): Properties = {
    val logProps = new Properties()
    logProps.put(LogConfig.SegmentBytesProp, 512: java.lang.Integer)
    logProps.put(LogConfig.SegmentIndexBytesProp, 1000: java.lang.Integer)
    logProps.put(LogConfig.RetentionMsProp, 999: java.lang.Integer)
    overrides.foreach { case (k, v) => logProps.put(k, v) }
    logProps
  }

  @After
  def tearDown(): Unit = {
    if (tmpDir.exists()) {
      logManager.shutdown()
      Utils.delete(tmpDir)
      TestUtils.clearYammerMetrics()
    }
  }
}
