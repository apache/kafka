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
package kafka.tiered.storage

import java.io.File

import org.apache.kafka.common.TopicPartition

final class StorageWatcher(private val storageDirname: String) {
  private val storageDirectory = new File(storageDirname)

  def getEarliestOffset(topicPartition: TopicPartition): Unit = {
    val files = storageDirectory.listFiles().toSeq
    val topicPartitionDir = files.map(_.getName).find(_ == topicPartition.toString).getOrElse {
      throw new IllegalArgumentException(s"Directory for the topic-partition $topicPartition was not found")
    }

    //val topicPartitionFiles =
  }

}
