/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/
package kafka.server.metadata

import kafka.server.{KafkaConfig, MetadataCache}
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.server.fault.FaultHandler

/**
 * Publishing dynamic topic or cluster changes to the client quota manager.
 * Temporary solution since Cluster objects are immutable and costly to update for every metadata change.
 * See KAFKA-18239 to trace the issue.
 */
class DynamicTopicClusterQuotaPublisher (
  clusterId: String,
  conf: KafkaConfig,
  faultHandler: FaultHandler,
  nodeType: String,
  quotaManagers: QuotaManagers
) extends Logging with org.apache.kafka.image.publisher.MetadataPublisher {
  logIdent = s"[${name()}] "

  override def name(): String = s"DynamicTopicClusterQuotaPublisher $nodeType id=${conf.nodeId}"

  override def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LoaderManifest
  ): Unit = {
    onMetadataUpdate(delta, newImage)
  }

  def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
  ): Unit = {
    try {
      quotaManagers.clientQuotaCallback().ifPresent(clientQuotaCallback => {
        if (delta.topicsDelta() != null || delta.clusterDelta() != null) {
          val cluster = MetadataCache.toCluster(clusterId, newImage)
          if (clientQuotaCallback.updateClusterMetadata(cluster)) {
            quotaManagers.fetch.updateQuotaMetricConfigs()
            quotaManagers.produce.updateQuotaMetricConfigs()
            quotaManagers.request.updateQuotaMetricConfigs()
            quotaManagers.controllerMutation.updateQuotaMetricConfigs()
          }
        }
      })
    } catch {
      case t: Throwable =>
        val deltaName = s"MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
        faultHandler.handleFault("Uncaught exception while " +
          s"publishing dynamic topic or cluster changes from $deltaName", t)
    }
  }
}
 