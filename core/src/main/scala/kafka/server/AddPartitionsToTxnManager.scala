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

import kafka.common.{InterBrokerSendThread, RequestAndCompletionHandler}
import org.apache.kafka.clients.{ClientResponse, NetworkClient, RequestCompletionHandler}
import org.apache.kafka.common.{Node, TopicPartition}
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse}
import org.apache.kafka.common.utils.Time

import scala.collection.mutable

object AddPartitionsToTxnManager {
  type AppendCallback = Map[TopicPartition, Errors] => Unit
}


class TransactionDataAndCallbacks(val transactionData: AddPartitionsToTxnTransactionCollection,
                                  val callbacks: mutable.Map[String, AddPartitionsToTxnManager.AppendCallback])


class AddPartitionsToTxnManager(config: KafkaConfig, client: NetworkClient, time: Time) 
  extends InterBrokerSendThread("AddPartitionsToTxnSenderThread-" + config.brokerId, client, config.requestTimeoutMs, time) {
  
  private val inflightNodes = mutable.HashSet[Node]()
  private val nodesToTransactions = mutable.Map[Node, TransactionDataAndCallbacks]()
  
  def addTxnData(node: Node, transactionData: AddPartitionsToTxnTransaction, callback: AddPartitionsToTxnManager.AppendCallback): Unit = {
    nodesToTransactions.synchronized {
      // Check if we have already have either node or individual transaction. Add the Node if it isn't there.
      val currentNodeAndTransactionData = nodesToTransactions.getOrElseUpdate(node,
        new TransactionDataAndCallbacks(
          new AddPartitionsToTxnTransactionCollection(1),
          mutable.Map[String, AddPartitionsToTxnManager.AppendCallback]()))

      val currentTransactionData = currentNodeAndTransactionData.transactionData.find(transactionData.transactionalId)

      // Check if we already have txn ID -- if the epoch is bumped, return invalid producer epoch, otherwise, the client likely disconnected and 
      // reconnected so return the retriable network exception.
      if (currentTransactionData != null) {
        val error = if (currentTransactionData.producerEpoch() < transactionData.producerEpoch())
          Errors.INVALID_PRODUCER_EPOCH
        else 
          Errors.NETWORK_EXCEPTION
        val topicPartitionsToError = mutable.Map[TopicPartition, Errors]()
        currentTransactionData.topics().forEach { topic =>
          topic.partitions().forEach { partition =>
            topicPartitionsToError.put(new TopicPartition(topic.name(), partition), error)
          }
        }
        val oldCallback = currentNodeAndTransactionData.callbacks(transactionData.transactionalId())
        currentNodeAndTransactionData.transactionData.remove(transactionData)
        oldCallback(topicPartitionsToError.toMap)
      }
      currentNodeAndTransactionData.transactionData.add(transactionData)
      currentNodeAndTransactionData.callbacks.put(transactionData.transactionalId(), callback)
      wakeup()
    }
  }

  private class AddPartitionsToTxnHandler(node: Node, transactionDataAndCallbacks: TransactionDataAndCallbacks) extends RequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      // Note: Synchronization is not needed on inflightNodes since it is always accessed from this thread.
      inflightNodes.remove(node)
      if (response.authenticationException() != null) {
        error(s"AddPartitionsToTxnRequest failed for node ${response.destination()} with an " +
          "authentication exception.", response.authenticationException)
        transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
          callback(buildErrorMap(txnId, Errors.forException(response.authenticationException()).code()))
        }
      } else if (response.versionMismatch != null) {
        // We may see unsupported version exception if we try to send a verify only request to a broker that can't handle it. 
        // In this case, skip verification.
        warn(s"AddPartitionsToTxnRequest failed for node ${response.destination()} with invalid version exception. This suggests verification is not supported." +
          s"Continuing handling the produce request.")
        transactionDataAndCallbacks.callbacks.values.foreach(_(Map.empty))
      } else if (response.wasDisconnected() || response.wasTimedOut()) {
        warn(s"AddPartitionsToTxnRequest failed for node ${response.destination()} with a network exception.")
        transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
          callback(buildErrorMap(txnId, Errors.NETWORK_EXCEPTION.code()))
        }
      } else {
        val addPartitionsToTxnResponseData = response.responseBody.asInstanceOf[AddPartitionsToTxnResponse].data
        if (addPartitionsToTxnResponseData.errorCode != 0) {
          error(s"AddPartitionsToTxnRequest for node ${response.destination()} returned with error ${Errors.forCode(addPartitionsToTxnResponseData.errorCode)}.")
          // The client should not be exposed to CLUSTER_AUTHORIZATION_FAILED so modify the error to signify the verification did not complete.
          // Older clients return with INVALID_RECORD and newer ones can return with INVALID_TXN_STATE.
          val finalError = if (addPartitionsToTxnResponseData.errorCode() == Errors.CLUSTER_AUTHORIZATION_FAILED.code)
            Errors.INVALID_RECORD.code
          else 
            addPartitionsToTxnResponseData.errorCode()
          
          transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
            callback(buildErrorMap(txnId, finalError))
          }
        } else {
          addPartitionsToTxnResponseData.resultsByTransaction().forEach { transactionResult =>
            val unverified = mutable.Map[TopicPartition, Errors]()
            transactionResult.topicResults().forEach { topicResult =>
              topicResult.resultsByPartition().forEach { partitionResult =>
                val tp = new TopicPartition(topicResult.name(), partitionResult.partitionIndex())
                if (partitionResult.partitionErrorCode() != Errors.NONE.code()) {
                  // Producers expect to handle INVALID_PRODUCER_EPOCH in this scenario.
                  val code = 
                    if (partitionResult.partitionErrorCode() == Errors.PRODUCER_FENCED.code)
                      Errors.INVALID_PRODUCER_EPOCH.code
                    // Older clients return INVALID_RECORD  
                    else if (partitionResult.partitionErrorCode() == Errors.INVALID_TXN_STATE.code)
                      Errors.INVALID_RECORD.code  
                    else 
                      partitionResult.partitionErrorCode()
                  unverified.put(tp, Errors.forCode(code))
                }
              }
            }
            val callback = transactionDataAndCallbacks.callbacks(transactionResult.transactionalId())
            callback(unverified.toMap)
          }
        }
      }
      wakeup()
    }
    
    private def buildErrorMap(transactionalId: String, errorCode: Short): Map[TopicPartition, Errors] = {
      val errors = new mutable.HashMap[TopicPartition, Errors]()
      val transactionData = transactionDataAndCallbacks.transactionData.find(transactionalId)
      transactionData.topics.forEach { topic =>
        topic.partitions().forEach { partition =>
          errors.put(new TopicPartition(topic.name(), partition), Errors.forCode(errorCode))
        }
      }
      errors.toMap
    }
  }

  override def generateRequests(): Iterable[RequestAndCompletionHandler] = {
    
    // build and add requests to queue
    val buffer = mutable.Buffer[RequestAndCompletionHandler]()
    val currentTimeMs = time.milliseconds()
    val removedNodes = mutable.Set[Node]()
    nodesToTransactions.synchronized {
      nodesToTransactions.foreach { case (node, transactionDataAndCallbacks) =>
        if (!inflightNodes.contains(node)) {
          buffer += RequestAndCompletionHandler(
            currentTimeMs,
            node,
            AddPartitionsToTxnRequest.Builder.forBroker(transactionDataAndCallbacks.transactionData),
            new AddPartitionsToTxnHandler(node, transactionDataAndCallbacks)
          )

          removedNodes.add(node)
        }
      }
      removedNodes.foreach { node =>
        inflightNodes.add(node)
        nodesToTransactions.remove(node)
      }
    }
    buffer
  }

}
