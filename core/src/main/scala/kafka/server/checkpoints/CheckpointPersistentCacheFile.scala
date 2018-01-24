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
package kafka.server.checkpoints

import java.io.File

import kafka.server.LogDirFailureChannel

class CheckpointPersistentCacheFile[K, V](file: File,
                                          formatter: CheckpointFileFormatter[(K, V)],
                                          logDirFailureChannel: LogDirFailureChannel = null,
                                          logDir: String = null) extends CheckpointPersistentCache[K, V] {
  private val _checkpointFile: CheckpointFile[(K, V)] = new CheckpointFile[(K, V)](
    file,
    0,
    formatter,
    logDirFailureChannel,
    if (logDir == null) file.getParentFile.toString else logDir)

  @volatile private var _entriesMap: collection.Map[K, V] = _checkpointFile.read().toMap

  override def getCheckpoint(key: K): Option[V] = {
    _entriesMap.get(key)
  }

  override def persist(): Unit = {
    _checkpointFile.write(_entriesMap.toSeq)
  }

  override def update(entries: Map[K, V]): Unit = {
    _entriesMap = entries
  }
}
