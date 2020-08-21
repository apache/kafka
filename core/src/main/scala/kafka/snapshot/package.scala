/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka

import java.nio.file.Path
import java.text.NumberFormat
import org.apache.kafka.raft.OffsetAndEpoch

package object snapshot {
  private[this] val SnapshotDir = "snapshots"
  private[this] val Suffix =  ".snapshot"

  def snapshotDir(logDir: Path): Path = {
    logDir.resolve(SnapshotDir)
  }

  def snapshotPath(logDir: Path, snapshotId: OffsetAndEpoch): Path = {
    snapshotDir(logDir).resolve(filenameFromSnapshotId(snapshotId) + Suffix)
  }

  def filenameFromSnapshotId(snapshotId: OffsetAndEpoch): String = {
    val formatter = NumberFormat.getInstance()
    formatter.setMinimumIntegerDigits(20)
    formatter.setGroupingUsed(false)

    formatter.format(snapshotId.offset) + "-" + formatter.format(snapshotId.epoch)
  }

  def moveRename(source: Path, snapshotId: OffsetAndEpoch): Path = {
    source.resolveSibling(filenameFromSnapshotId(snapshotId) + Suffix)
  }
}
