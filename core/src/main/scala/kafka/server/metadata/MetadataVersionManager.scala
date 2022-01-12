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
import org.apache.kafka.metadata.{MetadataVersionProvider, MetadataVersions}

import scala.collection.mutable


/**
 * This class holds the broker's view of the current metadata.version. Listeners can be registered with this class to
 * get a synchronous callback when version has changed.
 */
class MetadataVersionManager extends MetadataVersionProvider with Logging {
  val logContext = new LogContext(s"[MetadataVersionManager] ")

  this.logIdent = logContext.logPrefix()

  var version: MetadataVersions = MetadataVersions.UNINITIALIZED

  private val listeners: mutable.Set[MetadataVersionChangeListener] = mutable.HashSet()

  override def activeVersion(): MetadataVersions = {
    version
  }

  /**
   * This is called every time we publish metadata updates from BrokerMetadataPublisher. It will update its
   * cached version as well as return an object representing the version change (if any).
   *
   * This method is not thread-safe and is intended to be called from the event processor of BrokerMetadataListener's
   * internal queue.
   */
  def update(metadataVersionOpt: Option[Short], highestMetadataOffset: Long): MetadataVersionDelta = {
    if (metadataVersionOpt.isEmpty && version == MetadataVersions.UNINITIALIZED) {
      // No version initialized and no change
      throw new IllegalStateException("Cannot proceed without metadata.version set")
    } else if (metadataVersionOpt.isEmpty) {
      // No change
      Version(version.version)
    } else {
      // Check for new version
      val prev = version
      val updated = metadataVersionOpt.get
      val delta = prev match {
        case p if p.version < updated => UpgradedVersion(p.version, updated)
        case p if p.version > updated => DowngradedVersion(p.version, updated)
        case p if p.version == updated => Version(p.version)
        case _ => throw new IllegalStateException("Should have a previous and current version to compare")
      }
      version = MetadataVersions.fromValue(updated)

      info(s"Updating active metadata.version to $version at offset $highestMetadataOffset")
      listeners.foreach { listener =>
        try {
          listener.apply(delta)
        } catch {
          case t: Throwable => error(s"Had an error when calling a MetadataVersionChangeListener", t)
        }
      }
      delta
    }
  }

  def listen(listener: MetadataVersionChangeListener): Unit = {
    listeners.add(listener)
  }
}

trait MetadataVersionChangeListener {
  def apply(version: MetadataVersionDelta): Unit
}

/**
 * Represents the state of a metadata version change. Can be an upgrade, downgrade, or no change.
 */
sealed trait MetadataVersionDelta {
  val version: Short

  def asMetadataVersions(): MetadataVersions = {
    MetadataVersions.fromValue(version)
  }
}

case class Version(version: Short) extends MetadataVersionDelta {}
case class UpgradedVersion(previous: Short, version: Short) extends MetadataVersionDelta {}
case class DowngradedVersion(previous: Short, version: Short) extends MetadataVersionDelta {}
