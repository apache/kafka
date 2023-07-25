/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.admin

import kafka.api.IntegrationTestHarness
import kafka.server.KafkaConfig
import kafka.utils.{Logging, TestInfoUtils, TestUtils}
import org.apache.kafka.common.{TopicIdPartition, Uuid}
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException
import org.apache.kafka.common.utils.MockTime
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManagerConfig, RemoteLogSegmentId, RemoteLogSegmentMetadata, RemoteLogSegmentState}
import org.junit.jupiter.api.Assertions._
import org.junit.jupiter.api.function.Executable
import org.junit.jupiter.api.{BeforeEach, Tag, TestInfo}
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource

import java.util
import java.util.{Collections, Optional, Properties}
import java.util.concurrent.ExecutionException
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.Seq
import scala.util.Random

@Tag("integration")
class RemoteTopicDeletionTest extends IntegrationTestHarness with Logging {

  private var testTopicName: String = _

  override protected def brokerCount: Int = 2

  override protected def modifyConfigs(props: Seq[Properties]): Unit = {
    props.foreach(p => p.putAll(overrideProps()))
  }

  override protected def kraftControllerConfigs(): Seq[Properties] = {
    Seq(overrideProps())
  }

  @BeforeEach
  override def setUp(info: TestInfo): Unit = {
    super.setUp(info)
    testTopicName = s"${info.getTestMethod.get().getName}-${Random.alphanumeric.take(10).mkString}"
  }

  @ParameterizedTest(name = TestInfoUtils.TestWithParameterizedQuorumName)
  @ValueSource(strings = Array("zk", "kraft"))
  def testTopicDeletion(quorum: String): Unit = {
    val numPartitions = 2
    val topicConfig = new Properties()
    topicConfig.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    topicConfig.put(TopicConfig.RETENTION_MS_CONFIG, "200")
    topicConfig.put(TopicConfig.LOCAL_LOG_RETENTION_MS_CONFIG, "100")
    TestUtils.createTopicWithAdmin(createAdminClient(), testTopicName, brokers, numPartitions, brokerCount,
      topicConfig = topicConfig)
    TestUtils.deleteTopicWithAdmin(createAdminClient(), testTopicName, brokers)
    assertThrowsException(classOf[UnknownTopicOrPartitionException],
      () => TestUtils.describeTopic(createAdminClient(), testTopicName), "Topic should be deleted")

    // FIXME: It seems the storage manager is being instantiated in different class loader so couldn't verify the value
    //  but ensured it by adding a log statement in the storage manager (manually).
//    assertEquals(numPartitions * MyRemoteLogMetadataManager.segmentCount,
//      MyRemoteStorageManager.deleteSegmentEventCounter.get(),
//      "Remote log segments should be deleted only once by the leader")
  }

  private def overrideProps(): Properties = {
    val props = new Properties()
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP,
      classOf[MyRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP,
      classOf[MyRemoteLogMetadataManager].getName)

    props.put(KafkaConfig.LogRetentionTimeMillisProp, "2000")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_MS_PROP, "1000")
    props.put(KafkaConfig.LogRetentionBytesProp, "2048")
    props.put(RemoteLogManagerConfig.LOG_LOCAL_RETENTION_BYTES_PROP, "1024")
    props
  }

  private def assertThrowsException(exceptionType: Class[_ <: Throwable],
                                    executable: Executable,
                                    message: String): Throwable = {
    assertThrows(exceptionType, () => {
      try {
        executable.execute()
      } catch {
        case e: ExecutionException => throw e.getCause
      }
    }, message)
  }
}

object MyRemoteStorageManager {
  val deleteSegmentEventCounter = new AtomicInteger(0)
}

class MyRemoteStorageManager extends NoOpRemoteStorageManager with Logging {
  import MyRemoteStorageManager._

  override def deleteLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {
    deleteSegmentEventCounter.incrementAndGet()
    info(s"Deleted the remote log segment: $remoteLogSegmentMetadata, counter: ${deleteSegmentEventCounter.get()}")
  }
}

class MyRemoteLogMetadataManager extends NoOpRemoteLogMetadataManager {

  import MyRemoteLogMetadataManager._
  val time = new MockTime()

  override def listRemoteLogSegments(topicIdPartition: TopicIdPartition): util.Iterator[RemoteLogSegmentMetadata] = {
    val segmentMetadataList = new util.ArrayList[RemoteLogSegmentMetadata]()
    for (idx <- 0 until segmentCount) {
      val timestamp = time.milliseconds()
      val startOffset = idx * recordsPerSegment
      val endOffset = startOffset + recordsPerSegment - 1
      val segmentLeaderEpochs: util.Map[Integer, java.lang.Long] = Collections.singletonMap(0, 0L)
      segmentMetadataList.add(new RemoteLogSegmentMetadata(new RemoteLogSegmentId(topicIdPartition, Uuid.randomUuid()),
        startOffset, endOffset, timestamp, 0, timestamp, segmentSize, Optional.empty(),
        RemoteLogSegmentState.COPY_SEGMENT_FINISHED, segmentLeaderEpochs))
    }
    segmentMetadataList.iterator()
  }
}

object MyRemoteLogMetadataManager {
  val segmentCount = 10
  val recordsPerSegment = 100
  val segmentSize = 1024
}

