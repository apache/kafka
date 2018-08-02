package kafka.server

import kafka.metrics.KafkaMetricsGroup
import kafka.utils.Logging

object DelayedOperationManager {
  def apply(delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
            delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
            delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords]) =
    new DelayedOperationManager(delayedProducePurgatory, delayedFetchPurgatory, delayedDeleteRecordsPurgatory)
}


class DelayedOperationManager(val delayedProducePurgatory: DelayedOperationPurgatory[DelayedProduce],
                              val delayedFetchPurgatory: DelayedOperationPurgatory[DelayedFetch],
                              val delayedDeleteRecordsPurgatory: DelayedOperationPurgatory[DelayedDeleteRecords]) extends Logging with KafkaMetricsGroup {

  /**
    * Try to complete some delayed produce requests with the request key;
    * this can be triggered when:
    *
    * 1. The partition HW has changed (for acks = -1)
    * 2. A follower replica's fetch operation is received (for acks > 1)
    */
  def tryCompleteDelayedProduce(key: DelayedOperationKey) {
    val completed = delayedProducePurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d producer requests.".format(key.keyLabel, completed))
  }

  /**
    * Try to complete some delayed fetch requests with the request key;
    * this can be triggered when:
    *
    * 1. The partition HW has changed (for regular fetch)
    * 2. A new message set is appended to the local log (for follower fetch)
    */
  def tryCompleteDelayedFetch(key: DelayedOperationKey) {
    val completed = delayedFetchPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d fetch requests.".format(key.keyLabel, completed))
  }

  /**
    * Try to complete some delayed DeleteRecordsRequest with the request key;
    * this needs to be triggered when the partition low watermark has changed
    */
  def tryCompleteDelayedDeleteRecords(key: DelayedOperationKey) {
    val completed = delayedDeleteRecordsPurgatory.checkAndComplete(key)
    debug("Request key %s unblocked %d DeleteRecordsRequest.".format(key.keyLabel, completed))
  }
}
