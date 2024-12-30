/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import org.apache.kafka.common.config.TopicConfig
import org.apache.kafka.server.log.remote.storage.{NoOpRemoteLogMetadataManager, NoOpRemoteStorageManager, RemoteLogManagerConfig}
import org.junit.jupiter.api.TestInfo

import java.util.Properties
import scala.collection.Seq

class ListOffsetsRequestWithRemoteStoreTest extends ListOffsetsRequestTest {

  override def kraftControllerConfigs(testInfo: TestInfo): Seq[Properties] = {
    val props = new Properties
    brokerPropertyOverrides(props)
    Seq(props)
  }

  override def brokerPropertyOverrides(props: Properties): Unit = {
    props.put(RemoteLogManagerConfig.REMOTE_LOG_STORAGE_SYSTEM_ENABLE_PROP, "true")
    props.put(RemoteLogManagerConfig.REMOTE_STORAGE_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteStorageManager].getName)
    props.put(RemoteLogManagerConfig.REMOTE_LOG_METADATA_MANAGER_CLASS_NAME_PROP, classOf[NoOpRemoteLogMetadataManager].getName)
  }

  override def createTopic(numPartitions: Int, replicationFactor: Int): Map[Int, Int] = {
    val props = new Properties()
    props.put(TopicConfig.REMOTE_LOG_STORAGE_ENABLE_CONFIG, "true")
    super.createTopic(topic, numPartitions, replicationFactor, props)
  }
}
