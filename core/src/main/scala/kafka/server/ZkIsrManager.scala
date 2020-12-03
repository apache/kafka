package kafka.server

import kafka.utils.ReplicationUtils
import kafka.zk.KafkaZkClient
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.protocol.Errors

class ZkIsrManager(zkClient: KafkaZkClient) extends AlterIsrManager {
  override def start(): Unit = {
    // No-op
  }

  override def clearPending(topicPartition: TopicPartition): Unit = {
    // No-op
  }

  override def enqueue(alterIsrItem: AlterIsrItem): Boolean = {
    val (updateSucceeded, newVersion) = ReplicationUtils.updateLeaderAndIsr(zkClient, alterIsrItem.topicPartition,
      alterIsrItem.leaderAndIsr, alterIsrItem.controllerEpoch)

    if (updateSucceeded) {
      alterIsrItem.callback.apply(Right(alterIsrItem.leaderAndIsr.withZkVersion(newVersion)))
    } else {
      alterIsrItem.callback.apply(Left(Errors.INVALID_UPDATE_VERSION))
    }
    true
  }
}
