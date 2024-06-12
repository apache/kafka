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

package kafka.server

import kafka.server.AddPartitionsToTxnManager.{VerificationFailureRateMetricName, VerificationTimeMsMetricName}
import kafka.utils.Implicits.MapExtensionMethods
import kafka.utils.Logging
import org.apache.kafka.clients.{ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.internals.Topic
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTopic, AddPartitionsToTxnTopicCollection, AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse, MetadataResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.server.metrics.KafkaMetricsGroup
import org.apache.kafka.server.util.{InterBrokerSendThread, RequestAndCompletionHandler}

import java.util
import java.util.concurrent.TimeUnit
import scala.collection.{Seq, mutable}
import scala.jdk.CollectionConverters._

object AddPartitionsToTxnManager {
  type AppendCallback = Map[TopicPartition, Errors] => Unit

  val VerificationFailureRateMetricName = "VerificationFailureRate"
  val VerificationTimeMsMetricName = "VerificationTimeMs"
}

/**
 * This is an enum which handles the Partition Response based on the Request Version and the exact operation
 *    defaultError:       This is the default workflow which maps to cases when the Produce Request Version or the Txn_offset_commit request was lower than the first version supporting the new Error Class
 *    genericError:       This maps to the case when the clients are updated to handle the TransactionAbortableException
 *    addPartition:       This is a WIP. To be updated as a part of KIP-890 Part 2
 */
sealed trait TransactionSupportedOperation
case object defaultError extends TransactionSupportedOperation
case object genericError extends TransactionSupportedOperation
case object addPartition extends TransactionSupportedOperation

/*
 * Data structure to hold the transactional data to send to a node. Note -- at most one request per transactional ID
 * will exist at a time in the map. If a given transactional ID exists in the map, and a new request with the same ID
 * comes in, one request will be in the map and one will return to the producer with a response depending on the epoch.
 */
class TransactionDataAndCallbacks(val transactionData: AddPartitionsToTxnTransactionCollection,
                                  val callbacks: mutable.Map[String, AddPartitionsToTxnManager.AppendCallback],
                                  val startTimeMs: mutable.Map[String, Long],
                                  val transactionSupportedOperation: TransactionSupportedOperation)

class AddPartitionsToTxnManager(
  config: KafkaConfig,
  client: NetworkClient,
  metadataCache: MetadataCache,
  partitionFor: String => Int,
  time: Time
) extends InterBrokerSendThread(
  "AddPartitionsToTxnSenderThread-" + config.brokerId,
  client,
  config.requestTimeoutMs,
  time
) with Logging {

  this.logIdent = logPrefix

  private val interBrokerListenerName = config.interBrokerListenerName
  private val inflightNodes = mutable.HashSet[Node]()
  private val nodesToTransactions = mutable.Map[Node, TransactionDataAndCallbacks]()

  private val metricsGroup = new KafkaMetricsGroup(this.getClass)
  private val verificationFailureRate = metricsGroup.newMeter(VerificationFailureRateMetricName, "failures", TimeUnit.SECONDS)
  private val verificationTimeMs = metricsGroup.newHistogram(VerificationTimeMsMetricName)

  def verifyTransaction(
    transactionalId: String,
    producerId: Long,
    producerEpoch: Short,
    topicPartitions: Seq[TopicPartition],
    callback: AddPartitionsToTxnManager.AppendCallback,
    transactionSupportedOperation: TransactionSupportedOperation
  ): Unit = {
    val coordinatorNode = getTransactionCoordinator(partitionFor(transactionalId))
    if (coordinatorNode.isEmpty) {
      callback(topicPartitions.map(tp => tp -> Errors.COORDINATOR_NOT_AVAILABLE).toMap)
    } else {
      val topicCollection = new AddPartitionsToTxnTopicCollection()
      topicPartitions.groupBy(_.topic).forKeyValue { (topic, tps) =>
        topicCollection.add(new AddPartitionsToTxnTopic()
          .setName(topic)
          .setPartitions(tps.map(tp => Int.box(tp.partition)).toList.asJava))
      }

      val transactionData = new AddPartitionsToTxnTransaction()
        .setTransactionalId(transactionalId)
        .setProducerId(producerId)
        .setProducerEpoch(producerEpoch)
        .setVerifyOnly(true)
        .setTopics(topicCollection)

      addTxnData(coordinatorNode.get, transactionData, callback, transactionSupportedOperation)

    }
  }

  private def addTxnData(
    node: Node,
    transactionData: AddPartitionsToTxnTransaction,
    callback: AddPartitionsToTxnManager.AppendCallback,
    transactionSupportedOperation: TransactionSupportedOperation
  ): Unit = {
    nodesToTransactions.synchronized {
      val curTime = time.milliseconds()
      // Check if we have already have either node or individual transaction. Add the Node if it isn't there.
      val existingNodeAndTransactionData = nodesToTransactions.getOrElseUpdate(node,
        new TransactionDataAndCallbacks(
          new AddPartitionsToTxnTransactionCollection(1),
          mutable.Map[String, AddPartitionsToTxnManager.AppendCallback](),
          mutable.Map[String, Long](),
          transactionSupportedOperation))

      val existingTransactionData = existingNodeAndTransactionData.transactionData.find(transactionData.transactionalId)

      // There are 3 cases if we already have existing data
      // 1. Incoming data has a higher epoch -- return INVALID_PRODUCER_EPOCH for existing data since it is fenced
      // 2. Incoming data has the same epoch -- return NETWORK_EXCEPTION for existing data, since the client is likely retrying and we want another retriable exception
      // 3. Incoming data has a lower epoch -- return INVALID_PRODUCER_EPOCH for the incoming data since it is fenced, do not add incoming data to verify
      if (existingTransactionData != null) {
        if (existingTransactionData.producerEpoch <= transactionData.producerEpoch) {
          val error = if (existingTransactionData.producerEpoch < transactionData.producerEpoch)
            Errors.INVALID_PRODUCER_EPOCH
          else
            Errors.NETWORK_EXCEPTION
          val oldCallback = existingNodeAndTransactionData.callbacks(transactionData.transactionalId)
          existingNodeAndTransactionData.transactionData.remove(transactionData)
          sendCallback(oldCallback, topicPartitionsToError(existingTransactionData, error), existingNodeAndTransactionData.startTimeMs(transactionData.transactionalId))
        } else {
          // If the incoming transactionData's epoch is lower, we can return with INVALID_PRODUCER_EPOCH immediately.
          sendCallback(callback, topicPartitionsToError(transactionData, Errors.INVALID_PRODUCER_EPOCH), curTime)
          return
        }
      }

      existingNodeAndTransactionData.transactionData.add(transactionData)
      existingNodeAndTransactionData.callbacks.put(transactionData.transactionalId, callback)
      existingNodeAndTransactionData.startTimeMs.put(transactionData.transactionalId, curTime)
      wakeup()
    }
  }

  private def getTransactionCoordinator(partition: Int): Option[Node] = {
   metadataCache.getPartitionInfo(Topic.TRANSACTION_STATE_TOPIC_NAME, partition)
      .filter(_.leader != MetadataResponse.NO_LEADER_ID)
      .flatMap(metadata => metadataCache.getAliveBrokerNode(metadata.leader, interBrokerListenerName))
  }

  private def topicPartitionsToError(transactionData: AddPartitionsToTxnTransaction, error: Errors): Map[TopicPartition, Errors] = {
    val topicPartitionsToError = mutable.Map[TopicPartition, Errors]()
    transactionData.topics.forEach { topic =>
      topic.partitions.forEach { partition =>
        topicPartitionsToError.put(new TopicPartition(topic.name, partition), error)
      }
    }
    verificationFailureRate.mark(topicPartitionsToError.size)
    topicPartitionsToError.toMap
  }

  private def sendCallback(callback: AddPartitionsToTxnManager.AppendCallback, errorMap: Map[TopicPartition, Errors], startTimeMs: Long): Unit = {
    verificationTimeMs.update(time.milliseconds() - startTimeMs)
    callback(errorMap)
  }

  private class AddPartitionsToTxnHandler(node: Node, transactionDataAndCallbacks: TransactionDataAndCallbacks) extends RequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      // Note: Synchronization is not needed on inflightNodes since it is always accessed from this thread.
      inflightNodes.remove(node)
      if (response.authenticationException != null) {
        error(s"AddPartitionsToTxnRequest failed for node ${response.destination} with an " +
          "authentication exception.", response.authenticationException)
        sendCallbacksToAll(Errors.forException(response.authenticationException).code)
      } else if (response.versionMismatch != null) {
        // We may see unsupported version exception if we try to send a verify only request to a broker that can't handle it.
        // In this case, skip verification.
        warn(s"AddPartitionsToTxnRequest failed for node ${response.destination} with invalid version exception. This suggests verification is not supported." +
          s"Continuing handling the produce request.")
        transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
          sendCallback(callback, Map.empty, transactionDataAndCallbacks.startTimeMs(txnId))
        }
      } else if (response.wasDisconnected || response.wasTimedOut) {
        warn(s"AddPartitionsToTxnRequest failed for node ${response.destination} with a network exception.")
        sendCallbacksToAll(Errors.NETWORK_EXCEPTION.code)
      } else {
        val addPartitionsToTxnResponseData = response.responseBody.asInstanceOf[AddPartitionsToTxnResponse].data
        if (addPartitionsToTxnResponseData.errorCode != 0) {
          error(s"AddPartitionsToTxnRequest for node ${response.destination} returned with error ${Errors.forCode(addPartitionsToTxnResponseData.errorCode)}.")
          // The client should not be exposed to CLUSTER_AUTHORIZATION_FAILED so modify the error to signify the verification did not complete.
          // Return INVALID_TXN_STATE.
          val finalError = if (addPartitionsToTxnResponseData.errorCode == Errors.CLUSTER_AUTHORIZATION_FAILED.code)
            Errors.INVALID_TXN_STATE.code
          else
            addPartitionsToTxnResponseData.errorCode

          sendCallbacksToAll(finalError)
        } else {
          addPartitionsToTxnResponseData.resultsByTransaction.forEach { transactionResult =>
            val unverified = mutable.Map[TopicPartition, Errors]()
            transactionResult.topicResults.forEach { topicResult =>
              topicResult.resultsByPartition.forEach { partitionResult =>
                val tp = new TopicPartition(topicResult.name, partitionResult.partitionIndex)
                if (partitionResult.partitionErrorCode != Errors.NONE.code) {
                  // Producers expect to handle INVALID_PRODUCER_EPOCH in this scenario.
                  val code =
                    if (partitionResult.partitionErrorCode == Errors.PRODUCER_FENCED.code)
                      Errors.INVALID_PRODUCER_EPOCH.code
                    else if (partitionResult.partitionErrorCode() == Errors.TRANSACTION_ABORTABLE.code && transactionDataAndCallbacks.transactionSupportedOperation != genericError) // For backward compatibility with clients.
                      Errors.INVALID_TXN_STATE.code
                    else
                      partitionResult.partitionErrorCode
                  unverified.put(tp, Errors.forCode(code))
                }
              }
            }
            verificationFailureRate.mark(unverified.size)
            val callback = transactionDataAndCallbacks.callbacks(transactionResult.transactionalId)
            sendCallback(callback, unverified.toMap, transactionDataAndCallbacks.startTimeMs(transactionResult.transactionalId))
          }
        }
      }
      wakeup()
    }

    private def buildErrorMap(transactionalId: String, errorCode: Short): Map[TopicPartition, Errors] = {
      val transactionData = transactionDataAndCallbacks.transactionData.find(transactionalId)
      topicPartitionsToError(transactionData, Errors.forCode(errorCode))
    }

    private def sendCallbacksToAll(errorCode: Short): Unit = {
      transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
        sendCallback(callback, buildErrorMap(txnId, errorCode), transactionDataAndCallbacks.startTimeMs(txnId))
      }
    }
  }

  override def generateRequests(): util.Collection[RequestAndCompletionHandler] = {
    // build and add requests to queue
    val list = new util.ArrayList[RequestAndCompletionHandler]()
    val currentTimeMs = time.milliseconds()
    val removedNodes = mutable.Set[Node]()
    nodesToTransactions.synchronized {
      nodesToTransactions.foreach { case (node, transactionDataAndCallbacks) =>
        if (!inflightNodes.contains(node)) {
          list.add(new RequestAndCompletionHandler(
            currentTimeMs,
            node,
            AddPartitionsToTxnRequest.Builder.forBroker(transactionDataAndCallbacks.transactionData),
            new AddPartitionsToTxnHandler(node, transactionDataAndCallbacks)
          ))

          removedNodes.add(node)
        }
      }
      removedNodes.foreach { node =>
        inflightNodes.add(node)
        nodesToTransactions.remove(node)
      }
    }
    list
  }

  override def shutdown(): Unit = {
    super.shutdown()
    metricsGroup.removeMetric(VerificationFailureRateMetricName)
    metricsGroup.removeMetric(VerificationTimeMsMetricName)
  }

}
