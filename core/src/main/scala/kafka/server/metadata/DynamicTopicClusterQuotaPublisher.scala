package kafka.server.metadata

import kafka.server.KafkaConfig
import kafka.server.QuotaFactory.QuotaManagers
import kafka.utils.Logging
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.server.fault.FaultHandler

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
    val deltaName = s"MetadataDelta up to ${newImage.highestOffsetAndEpoch().offset}"
    try {
      quotaManagers.clientQuotaCallback().ifPresent(clientQuotaCallback => {
        if (delta.topicsDelta() != null || delta.clusterDelta() != null) {
          val cluster = KRaftMetadataCache.toCluster(clusterId, newImage)
          if (clientQuotaCallback.updateClusterMetadata(cluster)) {
            quotaManagers.fetch.updateQuotaMetricConfigs()
            quotaManagers.produce.updateQuotaMetricConfigs()
            quotaManagers.request.updateQuotaMetricConfigs()
            quotaManagers.controllerMutation.updateQuotaMetricConfigs()
          }
        }
      })
    } catch {
      case t: Throwable => faultHandler.handleFault("Uncaught exception while " +
        s"publishing dynamic topic or cluster changes from $deltaName", t)
    }
  }
}