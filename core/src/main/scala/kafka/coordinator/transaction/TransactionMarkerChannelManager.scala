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


import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import kafka.server.{DelayedOperationPurgatory, KafkaConfig, MetadataCache}
import kafka.utils.Logging
import org.apache.kafka.clients._
import org.apache.kafka.common.metrics.Metrics
import org.apache.kafka.common.network._
import org.apache.kafka.common.requests.TransactionResult
import org.apache.kafka.common.security.JaasContext
import org.apache.kafka.common.utils.Time

import org.apache.kafka.common.protocol.Errors

import collection.JavaConverters._

object TransactionMarkerChannelManager {
  def apply(config: KafkaConfig,
            metrics: Metrics,
            metadataCache: MetadataCache,
            txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
            time: Time): TransactionMarkerChannelManager = {


    val channelBuilder = ChannelBuilders.clientChannelBuilder(
      config.interBrokerSecurityProtocol,
      JaasContext.Type.SERVER,
      config,
      config.interBrokerListenerName,
      config.saslMechanismInterBrokerProtocol,
      config.saslInterBrokerHandshakeRequestEnable
    )
    val threadName = "TxnMarkerSenderThread-" + config.brokerId
    val selector = new Selector(
      NetworkReceive.UNLIMITED,
      config.connectionsMaxIdleMs,
      metrics,
      time,
      "txn-marker-channel",
      Map.empty[String, String].asJava,
      false,
      channelBuilder
    )
    val networkClient = new NetworkClient(
      selector,
      new ManualMetadataUpdater(),
      threadName,
      1,
      50,
      Selectable.USE_DEFAULT_BUFFER_SIZE,
      config.socketReceiveBufferBytes,
      config.requestTimeoutMs,
      time,
      false,
      new ApiVersions
    )
    val channel = new TransactionMarkerChannel(config.interBrokerListenerName, metadataCache, networkClient, time)

    val sendThread: InterBrokerSendThread = {
      networkClient.wakeup()
      new InterBrokerSendThread(threadName, networkClient, requestGenerator(channel, txnMarkerPurgatory), time)
    }

    new TransactionMarkerChannelManager(config,
      metadataCache,
      txnMarkerPurgatory,
      sendThread,
      channel)
  }


  private[transaction] def requestGenerator(transactionMarkerChannel: TransactionMarkerChannel,
                                            txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker]): () => Iterable[RequestAndCompletionHandler] = {
    def generateRequests(): Iterable[RequestAndCompletionHandler] = transactionMarkerChannel.drainQueuedTransactionMarkers(txnMarkerPurgatory)

    generateRequests
  }
}



class TransactionMarkerChannelManager(config: KafkaConfig,
                                      metadataCache: MetadataCache,
                                      txnMarkerPurgatory: DelayedOperationPurgatory[DelayedTxnMarker],
                                      interBrokerSendThread: InterBrokerSendThread,
                                      transactionMarkerChannel: TransactionMarkerChannel) extends Logging {

  type WriteTxnMarkerCallback = Errors => Unit

  def start(): Unit = {
    interBrokerSendThread.start()
  }

  def shutdown(): Unit = {
    interBrokerSendThread.shutdown()
    transactionMarkerChannel.clear()
  }


  def addTxnMarkerRequest(txnTopicPartition: Int, metadata: TransactionMetadata, coordinatorEpoch: Int, completionCallback: WriteTxnMarkerCallback): Unit = {
    val metadataToWrite = metadata synchronized metadata.copy()

    if (!transactionMarkerChannel.maybeAddPendingRequest(txnTopicPartition, metadata))
      // TODO: Not sure this is the correct response here?
      completionCallback(Errors.INVALID_TXN_STATE)
    else {
      val delayedTxnMarker = new DelayedTxnMarker(metadataToWrite, completionCallback)
      txnMarkerPurgatory.tryCompleteElseWatch(delayedTxnMarker, Seq(metadata.pid))

      val result = metadataToWrite.state match {
        case PrepareCommit => TransactionResult.COMMIT
        case PrepareAbort => TransactionResult.ABORT
        case s => throw new IllegalStateException("Unexpected txn metadata state while writing markers: " + s)
      }
      transactionMarkerChannel.addRequestToSend(txnTopicPartition,
        metadataToWrite.pid,
        metadataToWrite.producerEpoch,
        result,
        coordinatorEpoch,
        metadataToWrite.topicPartitions.toSet)
    }
  }

  def removeCompleted(txnTopicPartition: Int, pid: Long): Unit = {
    transactionMarkerChannel.removeCompletedTxn(txnTopicPartition, pid)
  }

  def removeStateForPartition(transactionStateTopicPartitionId: Int): Unit = {
    transactionMarkerChannel.removeStateForPartition(transactionStateTopicPartitionId)
      .foreach{pid =>
        txnMarkerPurgatory.cancelForKey(pid)
      }
  }

}
