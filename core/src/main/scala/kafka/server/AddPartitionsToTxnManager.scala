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
import org.apache.kafka.common.{InvalidRecordException, Node, TopicPartition}
import org.apache.kafka.common.message.AddPartitionsToTxnRequestData.{AddPartitionsToTxnTransaction, AddPartitionsToTxnTransactionCollection}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{AddPartitionsToTxnRequest, AddPartitionsToTxnResponse}
import org.apache.kafka.common.utils.Time

import java.util.Collections
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
    // Check if we have already (either node or individual transaction). 
    val currentNodeAndTransactionDataOpt = nodesToTransactions.get(node)
    currentNodeAndTransactionDataOpt match {
      case None =>
        nodesToTransactions.put(node,
          new TransactionDataAndCallbacks(new AddPartitionsToTxnTransactionCollection(Collections.singletonList(transactionData).iterator()),
            mutable.Map(transactionData.transactionalId() -> callback)))
      case Some(currentNodeAndTransactionData) =>
        // Check if we already have txn ID -- this should only happen in epoch bump case. If so, we should return error for old entry and remove from queue.
        val currentTransactionData = currentNodeAndTransactionData.transactionData.find(transactionData.transactionalId)
        if (currentTransactionData != null) {
          if (currentTransactionData.producerEpoch() < transactionData.producerEpoch()) {
            val topicPartitionsToError = mutable.Map[TopicPartition, Errors]()
            currentTransactionData.topics().forEach { topic => 
              topic.partitions().forEach { partition =>
                topicPartitionsToError.put(new TopicPartition(topic.name(), partition), Errors.INVALID_PRODUCER_EPOCH)
              }
            }
            val callback = currentNodeAndTransactionData.callbacks(transactionData.transactionalId())
            currentNodeAndTransactionData.transactionData.remove(transactionData.transactionalId())
            callback(topicPartitionsToError.toMap)
          } else {
            // We should never see a request on the same epoch since we haven't finished handling the one in queue
            throw new InvalidRecordException("Received a second request from the same connection without finishing the first.")
          }
        }
        currentNodeAndTransactionData.transactionData.add(transactionData)
        currentNodeAndTransactionData.callbacks.put(transactionData.transactionalId(), callback)
    }
    wakeup()
  }

  private class AddPartitionsToTxnHandler(node: Node, transactionDataAndCallbacks: TransactionDataAndCallbacks) extends RequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      inflightNodes.synchronized(inflightNodes.remove(node))
      if (response.authenticationException() != null) {
        error(s"AddPartitionsToTxnRequest failed for broker ${config.brokerId} with an " +
          "authentication exception.", response.authenticationException)
        transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
          callback(buildErrorMap(txnId, transactionDataAndCallbacks.transactionData, Errors.forException(response.authenticationException()).code()))
        }
      } else if (response.versionMismatch != null) {
        // We may see unsupported version exception if we try to send a verify only request to a broker that can't handle it. 
        // In this case, skip verification.
        error(s"AddPartitionsToTxnRequest failed for broker ${config.brokerId} with invalid version exception. This suggests verification is not supported." +
              s"Continuing handling the produce request.")
        transactionDataAndCallbacks.callbacks.values.foreach(_(Map.empty))
      } else {
        val addPartitionsToTxnResponseData = response.responseBody.asInstanceOf[AddPartitionsToTxnResponse].data
        if (addPartitionsToTxnResponseData.errorCode != 0) {
          error(s"AddPartitionsToTxnRequest for broker ${config.brokerId}  returned with error ${Errors.forCode(addPartitionsToTxnResponseData.errorCode)}.")
          // TODO: send error back correctly -- we need to verify all possible errors can be handled by the client.
          // errors -- versionmismatch --> handled above
          //        -- clusterauth --> should handle differently
          transactionDataAndCallbacks.callbacks.foreach { case (txnId, callback) =>
            callback(buildErrorMap(txnId, transactionDataAndCallbacks.transactionData, addPartitionsToTxnResponseData.errorCode()))
          }
        } else {
          val unverified = new mutable.HashMap[TopicPartition, Errors]()
          addPartitionsToTxnResponseData.resultsByTransaction().forEach { transactionResult =>
            transactionResult.topicResults().forEach { topicResult =>
              topicResult.resultsByPartition().forEach { partitionResult =>
                val tp = new TopicPartition(topicResult.name(), partitionResult.partitionIndex())
                if (partitionResult.partitionErrorCode() != Errors.NONE.code()) {
                  // Producers expect to handle INVALID_PRODUCER_EPOCH in this scenario.
                  val code = 
                    if (partitionResult.partitionErrorCode() == Errors.PRODUCER_FENCED.code())
                      Errors.INVALID_PRODUCER_EPOCH.code() 
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
    }
    
    private def buildErrorMap(transactionalId: String, addPartitionsToTxnCollection: AddPartitionsToTxnTransactionCollection, errorCode: Short): Map[TopicPartition, Errors] = {
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
    inflightNodes.synchronized {
      nodesToTransactions.foreach { case (node, transactionDataAndCallbacks) =>
        if (!inflightNodes.contains(node)) {
          buffer += RequestAndCompletionHandler(
            currentTimeMs,
            node,
            AddPartitionsToTxnRequest.Builder.forBroker(transactionDataAndCallbacks.transactionData),
            new AddPartitionsToTxnHandler(node, transactionDataAndCallbacks)
          )

          inflightNodes.add(node)
        }
      }
      nodesToTransactions.clear() // Clear before we give up the lock so that no new data is removed.
    }
    buffer
  }
  

}
