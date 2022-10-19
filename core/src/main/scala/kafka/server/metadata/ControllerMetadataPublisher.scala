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

import kafka.server.ConfigAdminManager.toLoggableProps
import kafka.server.{ConfigEntityName, ConfigHandler, ConfigType, KafkaConfig}
import kafka.utils.Logging
import org.apache.kafka.common.config.ConfigResource.Type.BROKER
import org.apache.kafka.image.loader.{LogDeltaManifest, SnapshotManifest}
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.server.fault.FaultHandler


class ControllerMetadataPublisher(
  conf: KafkaConfig,
  dynamicConfigHandlers: Map[String, ConfigHandler],
  fatalFaultHandler: FaultHandler,
) extends MetadataPublisher with Logging {
  logIdent = s"[ControllerMetadataPublisher id=${conf.nodeId}] "
  var firstPublish = true

  val nodeId: Int = conf.nodeId

  override def name(): String = "ControllerMetadataPublisher"

  /**
   * Publish a new cluster metadata snapshot that we loaded.
   *
   * @param delta    The delta between the previous state and the new one.
   * @param newImage The complete new state.
   * @param manifest The contents of what was published.
   */
  override def publishSnapshot(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: SnapshotManifest
  ): Unit = {
    publish(delta, newImage)
  }

  /**
   * Publish a change to the cluster metadata.
   *
   * @param delta    The delta between the previous state and the new one.
   * @param newImage The complete new state.
   * @param manifest The contents of what was published.
   */
  override def publishLogDelta(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LogDeltaManifest
  ): Unit = {
    publish(delta, newImage)
  }

  def publish(delta: MetadataDelta, newImage: MetadataImage): Unit = {
    val deltaName = if (firstPublish) {
      s"initial MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    } else {
      s"MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    }
    firstPublish = false

    // Apply configuration deltas.
    Option(delta.configsDelta()).foreach { configsDelta =>
      configsDelta.changes().keySet().forEach { resource =>
        val props = newImage.configs().configProperties(resource)
        resource.`type`() match {
          case BROKER =>
            if (resource.name().isEmpty) {
              try {
                // Apply changes to "cluster configs" (also known as default BROKER configs).
                // These are stored in KRaft with an empty name field.
                info("Updating cluster configuration : " +
                  toLoggableProps(resource, props).mkString(","))
                dynamicConfigHandlers(ConfigType.Broker).
                  processConfigChanges(ConfigEntityName.Default, props)
              } catch {
                case t: Throwable => fatalFaultHandler.handleFault("Error updating " +
                  s"cluster with new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                  s"in ${deltaName}", t)
              }
            } else if (resource.name() == nodeId.toString) {
              try {
                // Apply changes to this broker's dynamic configuration.
                info(s"Updating controller $nodeId with new configuration : " +
                  toLoggableProps(resource, props).mkString(","))
                dynamicConfigHandlers(ConfigType.Broker).
                  processConfigChanges(resource.name(), props)
                // When applying a per broker config (not a cluster config), we also
                // reload any associated file. For example, if the ssl.keystore is still
                // set to /tmp/foo, we still want to reload /tmp/foo in case its contents
                // have changed. This doesn't apply to topic configs or cluster configs.
                conf.dynamicConfig.reloadUpdatedFilesWithoutConfigChange(props)
              } catch {
                case t: Throwable => fatalFaultHandler.handleFault("Error updating the " +
                  s"controller with a new configuration: ${toLoggableProps(resource, props).mkString(",")} " +
                  s"in ${deltaName}", t)
              }
            }
          case _ => // nothing to do
        }
      }
    }
  }

  override def close(): Unit = {
    // nothing to do
  }
}
