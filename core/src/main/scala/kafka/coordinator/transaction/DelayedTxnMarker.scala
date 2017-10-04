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
package kafka.coordinator.transaction

import java.util.concurrent.TimeUnit

import kafka.server.DelayedOperation
import org.apache.kafka.common.protocol.Errors

/**
  * Delayed transaction state change operations that are added to the purgatory without timeout (i.e. these operations should never time out)
  */
private[transaction] class DelayedTxnMarker(txnMetadata: TransactionMetadata,
                                           completionCallback: Errors => Unit)
  extends DelayedOperation(TimeUnit.DAYS.toMillis(100 * 365)) {

  override def tryComplete(): Boolean = {
    txnMetadata synchronized {
      if (txnMetadata.topicPartitions.isEmpty)
        forceComplete()
      else false
    }
  }

  override def onExpiration(): Unit = {
    // this should never happen
    throw new IllegalStateException(s"Delayed write txn marker operation for metadata $txnMetadata has timed out, this should never happen.")
  }

  // TODO: if we will always return NONE upon completion, we can remove the error code in the param
  override def onComplete(): Unit = completionCallback(Errors.NONE)

}
