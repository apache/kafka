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
import kafka.server.epoch.EpochEntry

import scala.collection._

trait LeaderEpochCheckpoint extends Any {
  def write(epochs: Seq[EpochEntry])
  def read(): Seq[EpochEntry]
}

/**
  * This class persists a map of (LeaderEpoch => Offsets) to a file (for a certain replica)
  */
class LeaderEpochCheckpointFile(val checkpointFile: CheckpointFile[EpochEntry]) extends AnyVal with LeaderEpochCheckpoint {
  override def write(epochs: Seq[EpochEntry]): Unit = {
    checkpointFile.write(epochs)
  }

  override def read(): Seq[EpochEntry] = {
    checkpointFile.read()
  }
}

object LeaderEpochCheckpointFile {
  private val LeaderEpochCheckpointFilename = "leader-epoch-checkpoint"
  val CurrentVersion = 0
  def newFile(dir: File): File = new File(dir, LeaderEpochCheckpointFilename)
  def apply(file: File, logDirFailureChannel: LogDirFailureChannel = null): LeaderEpochCheckpointFile = {
    val checkpointFile = new CheckpointFile[EpochEntry](file, CurrentVersion, logDirFailureChannel, file.getParentFile.getParent, (t: String) => EpochEntry(t))
    new LeaderEpochCheckpointFile(checkpointFile)
  }
}