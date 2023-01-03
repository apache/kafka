package kafka.server

import kafka.controller.KafkaController
import org.apache.kafka.common.requests.AbstractControlRequest

class BrokerEpochManager(metadataCache: MetadataCache,
                         controller: KafkaController,
                         lifecycleManagerOpt: Option[BrokerLifecycleManager]) {
  def get(): Long = {
    lifecycleManagerOpt match {
      case Some(lifecycleManager) => metadataCache.getControllerId match {
        case Some(_: ZkCachedControllerId) => controller.brokerEpoch
        case Some(_: KRaftCachedControllerId) => lifecycleManager.brokerEpoch
        case None | _ => controller.brokerEpoch
      }
      case None => controller.brokerEpoch
    }
  }

  def isBrokerEpochStale(brokerEpochInRequest: Long, isKRaftControllerRequest: Boolean): Boolean = {
    if (brokerEpochInRequest == AbstractControlRequest.UNKNOWN_BROKER_EPOCH) {
      false
    } else if (isKRaftControllerRequest) {
      if (lifecycleManagerOpt.isDefined) {
        brokerEpochInRequest < lifecycleManagerOpt.get.brokerEpoch
      } else {
        throw new IllegalStateException("Expected BrokerLifecycleManager to not be null.")
      }
    } else {
      // brokerEpochInRequest > controller.brokerEpoch is possible in rare scenarios where the controller gets notified
      // about the new broker epoch and sends a control request with this epoch before the broker learns about it
      brokerEpochInRequest < controller.brokerEpoch
    }
  }
}
