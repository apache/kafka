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

package kafka.server.metadata

import kafka.utils.Logging
import org.apache.kafka.common.utils.LogContext
import org.apache.kafka.metadata.{MetadataVersionProvider, MetadataVersions, VersionRange}

import scala.collection.mutable


/**
 * This class holds the broker's view of the current metadata.version. Listeners can be registered with this class to
 * get a synchronous callback when version has changed.
 */
class MetadataVersionManager extends MetadataVersionProvider with Logging {
  val logContext = new LogContext(s"[MetadataVersionManager] ")

  this.logIdent = logContext.logPrefix()

  @volatile var version: MetadataVersionDelta = NoVersion()

  private val listeners: mutable.Set[MetadataVersionChangeListener] = mutable.HashSet()

  override def activeVersion(): MetadataVersions = {
    version.asMetadataVersions()
  }

  /**
   * This is called every time we publish metadata updates from BrokerMetadataPublisher
   */
  def delta(metadataVersionOpt: Option[VersionRange], highestMetadataOffset: Long): MetadataVersionDelta = {
    metadataVersionOpt.foreach { metadataVersionChange =>
      val prev = version
      val curr = metadataVersionChange.max
      if (prev.version != curr) {
        version = {
          prev match {
            case _: NoVersion => Version(curr)
            case p: MetadataVersionDelta if p.version < curr => UpgradedVersion(p.version, curr)
            case p: MetadataVersionDelta if p.version > curr => DowngradedVersion(p.version, curr)
            case _: MetadataVersionDelta => throw new IllegalStateException()
          }
        }
        info(s"Updating active metadata.version to $version at offset $highestMetadataOffset")
        listeners.foreach { listener =>
          try {
            listener.apply(version)
          } catch {
            case t: Throwable => error(s"Had an error when calling MetadataVersionChangeListener", t)
          }
        }
      } else {
        version = Version(prev.version)
      }
    }
    version
  }

  def listen(listener: MetadataVersionChangeListener): Unit = {
    listeners.add(listener)
  }
}

trait MetadataVersionChangeListener {
  def apply(version: MetadataVersionDelta): Unit
}

sealed trait MetadataVersionDelta {
  val version: Short

  def asMetadataVersions(): MetadataVersions = {
    MetadataVersions.of(version)
  }
}

case class NoVersion() extends MetadataVersionDelta {
  val version: Short = -1
}
case class Version(version: Short) extends MetadataVersionDelta {}
case class UpgradedVersion(previous: Short, version: Short) extends MetadataVersionDelta {}
case class DowngradedVersion(previous: Short, version: Short) extends MetadataVersionDelta {}