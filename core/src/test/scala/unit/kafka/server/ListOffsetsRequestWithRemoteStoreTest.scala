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

import kafka.utils.TestUtils
import org.apache.kafka.common.TopicIdPartition
import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.server.log.remote.storage.{LogSegmentData, RemoteLogManagerConfig, RemoteLogMetadataManager, RemoteLogSegmentMetadata, RemoteLogSegmentMetadataUpdate, RemotePartitionDeleteMetadata, RemoteStorageManager}

import java.io.InputStream
import java.{lang, util}
import java.util.{Collections, Optional, Properties}

class ListOffsetsRequestWithRemoteStoreTest extends ListOffsetsRequestTest {

  override def brokerPropertyOverrides(props: Properties): Unit = {
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CONFIG_PREFIX_PROP, "remote.storage.config.")
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CONFIG_PREFIX_PROP, "rlmm.config.")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[DummyRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[DummyRemoteLogMetadataManager].getName)
  }

  override def createTopic(numPartitions: Int, replicationFactor: Int): Map[Int, Int] = {
    val props = new Properties()
    props.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    TestUtils.createTopic(zkClient, topic, numPartitions, replicationFactor, servers, props)
  }
}

private class DummyRemoteStorageManager extends RemoteStorageManager {
  override def copyLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, logSegmentData: LogSegmentData): Unit = {}
  override def fetchLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, startPosition: Int): InputStream = {null}
  override def fetchLogSegment(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, startPosition: Int, endPosition: Int): InputStream = {null}
  override def fetchIndex(remoteLogSegmentMetadata: RemoteLogSegmentMetadata, indexType: RemoteStorageManager.IndexType): InputStream = {null}
  override def deleteLogSegmentData(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {}
  override def close(): Unit = {}
  override def configure(configs: util.Map[String, _]): Unit = {}
}

private class DummyRemoteLogMetadataManager extends RemoteLogMetadataManager {
  override def addRemoteLogSegmentMetadata(remoteLogSegmentMetadata: RemoteLogSegmentMetadata): Unit = {}
  override def updateRemoteLogSegmentMetadata(remoteLogSegmentMetadataUpdate: RemoteLogSegmentMetadataUpdate): Unit = {}
  override def remoteLogSegmentMetadata(topicIdPartition: TopicIdPartition, epochForOffset: Int, offset: Long): Optional[RemoteLogSegmentMetadata] = {Optional.empty()}
  override def highestOffsetForEpoch(topicIdPartition: TopicIdPartition, leaderEpoch: Int): Optional[lang.Long] = {Optional.empty()}
  override def putRemotePartitionDeleteMetadata(remotePartitionDeleteMetadata: RemotePartitionDeleteMetadata): Unit = {}
  override def listRemoteLogSegments(topicIdPartition: TopicIdPartition): util.Iterator[RemoteLogSegmentMetadata] = {Collections.emptyIterator()}
  override def listRemoteLogSegments(topicIdPartition: TopicIdPartition, leaderEpoch: Int): util.Iterator[RemoteLogSegmentMetadata] = {Collections.emptyIterator()}
  override def onPartitionLeadershipChanges(leaderPartitions: util.Set[TopicIdPartition], followerPartitions: util.Set[TopicIdPartition]): Unit = {}
  override def onStopPartitions(partitions: util.Set[TopicIdPartition]): Unit = {}
  override def close(): Unit = {}
  override def configure(configs: util.Map[String, _]): Unit = {}
}
