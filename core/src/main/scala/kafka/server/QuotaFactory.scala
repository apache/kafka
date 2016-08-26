package kafka.server

import kafka.common.TopicAndPartition
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.utils.SystemTime

import scala.collection.Map

object QuotaType  {
  case object Fetch extends QuotaType
  case object Produce extends QuotaType
  case object LeaderReplication extends QuotaType
  case object  FollowerReplication extends QuotaType
}
sealed trait QuotaType

object QuotaFactory {


  object UnbreakableQuota extends ReadOnlyQuota {
    override def bound(): Int = Int.MaxValue

    override def isThrottled(topicAndPartition: TopicAndPartition): Boolean = false

    override def isQuotaExceededBy(bytes: Int): Boolean = false
  }

  case class QuotaManagers(client: Map[QuotaType, ClientQuotaManager], leaderReplication: ReplicationQuotaManager, followerReplication: ReplicationQuotaManager)

  /*
   * Returns a Map of all quota managers configured. The request Api key is the key for the Map
   */
  def instantiate(cfg: KafkaConfig, metrics: Metrics): QuotaManagers = {
    val producerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.producerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val consumerQuotaManagerCfg = ClientQuotaManagerConfig(
      quotaBytesPerSecondDefault = cfg.consumerQuotaBytesPerSecondDefault,
      numQuotaSamples = cfg.numQuotaSamples,
      quotaWindowSizeSeconds = cfg.quotaWindowSizeSeconds
    )

    val replicationQuotaManagerCfg = ReplicationQuotaManagerConfig(
      numQuotaSamples = cfg.numReplicationQuotaSamples,
      quotaWindowSizeSeconds = cfg.replicationQuotaWindowSizeSeconds
    )

    val clientQuotaManagers = Map[QuotaType, ClientQuotaManager](
      QuotaType.Produce ->
        new ClientQuotaManager(producerQuotaManagerCfg, metrics, QuotaType.Produce.toString, new org.apache.kafka.common.utils.SystemTime),
      QuotaType.Fetch ->
        new ClientQuotaManager(consumerQuotaManagerCfg, metrics, QuotaType.Fetch.toString, new org.apache.kafka.common.utils.SystemTime)
    )

    val leader = new ReplicationQuotaManager(replicationQuotaManagerCfg, metrics, QuotaType.LeaderReplication.toString, new SystemTime)
    val follower = new ReplicationQuotaManager(replicationQuotaManagerCfg, metrics, QuotaType.FollowerReplication.toString, new SystemTime)
    QuotaManagers(clientQuotaManagers, leader, follower)
  }
}