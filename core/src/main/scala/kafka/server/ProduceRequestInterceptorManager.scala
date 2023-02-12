/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.server

import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.{ProduceRequestInterceptorSkipRecordException, ProduceRequestInterceptorUnhandledException, TimeoutException => KafkaTimeoutException}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.record.{MemoryRecords, Record, SimpleRecord}
import org.apache.kafka.common.requests.ProduceRequest
import org.apache.kafka.common.requests.ProduceResponse.PartitionResponse
import org.apache.kafka.server.interceptors.ProduceRequestInterceptor

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, Executors, TimeoutException}
import scala.annotation.tailrec
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ProduceRequestInterceptorManager(interceptors: Iterable[ProduceRequestInterceptor] = Iterable.empty,
                                       interceptorTimeoutMs: Int = 10, maxRetriesOnTimeout: Int = 0) extends AutoCloseable {

  private val executorService: ExecutorService = Executors.newSingleThreadExecutor()
  private implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(executorService)
  private val interceptorTimeoutDuration: Duration = interceptorTimeoutMs.millis

  override def close(): Unit = executorService.shutdownNow()

  def applyInterceptors(produceRequest: ProduceRequest): mutable.Map[TopicPartition, PartitionResponse] = {
    val interceptorTopicResponses = mutable.Map[TopicPartition, PartitionResponse]()

    if (interceptors.isEmpty) {

    } else {
      produceRequest.data().topicData().forEach { topic => {
        val interceptorsForTopic = interceptors.filter(shouldInterceptTopic(topic.name(), _)).toSeq
        if (interceptorsForTopic.nonEmpty) {
          topic.partitionData().forEach {
            partitionData => {
              val parsedInput = Try {
                val memoryRecords = partitionData.records.asInstanceOf[MemoryRecords]
                val originalRecords = memoryRecords.records().asScala.toSeq
                (memoryRecords, originalRecords)
              }
              parsedInput match {
                case Success((memoryRecords, originalRecords)) =>
                  Try(processRecordsWithTimeout(originalRecords, topic.name(), partitionData.index(), interceptorsForTopic)) match {
                    case Success(processedRecords) =>
                      val newMemoryRecords = rebuildMemoryRecords(memoryRecords, processedRecords)
                      // We mutate the original partitionData so the changes will be visible for all subsequent processes
                      partitionData.setRecords(newMemoryRecords)
                    case Failure(throwable) =>
                      val tp = new TopicPartition(topic.name(), partitionData.index())
                      interceptorTopicResponses += tp -> new PartitionResponse(Errors.forException(throwable))
                  }
                case _ => // There is something wrong with the input that will be handled by donwstream validators
              }
            }
          }
        }
      }}
    }
    interceptorTopicResponses
  }

  private def shouldInterceptTopic(topic: String, interceptor: ProduceRequestInterceptor): Boolean = {
    val pattern = interceptor.interceptorTopicPattern().pattern().r
    pattern.findFirstMatchIn(topic).nonEmpty
  }

  @tailrec
  private def processRecordsWithTimeout(originalRecords: Seq[Record], topic: String, partition: Int, interceptors: Seq[ProduceRequestInterceptor], attempt: Int = 0): Seq[SimpleRecord] = {
    val res = Try {
      Await.result(
        Future { processRecords(originalRecords, topic, partition, interceptors) },
        interceptorTimeoutDuration
      )
    }
    res match {
      case Success(newRecords) => newRecords
      case Failure(throwable) => {
       throwable match {
         case _: TimeoutException =>
           if (attempt < maxRetriesOnTimeout) processRecordsWithTimeout(originalRecords, topic, partition, interceptors, attempt + 1)
           else {
             throw new KafkaTimeoutException(throwable)
           }
         case _ => throw throwable
       }
      }
    }
  }

  private def processRecords(originalRecords: Seq[Record], topic: String, partition: Int, interceptors: Seq[ProduceRequestInterceptor]): Seq[SimpleRecord] = {
    val accumulator = List.empty[SimpleRecord]
    originalRecords.foldLeft(accumulator) { (acc, record) =>
      val newRecordTry = processRecord(record, topic, partition, interceptors)
      newRecordTry match {
        case Success(newRecord) => acc :+ newRecord
        case Failure(throwable) => {
          throwable match {
            case _: ProduceRequestInterceptorSkipRecordException =>
              // TODO: Log error
              // Skip adding the record to the accumulator, and move onto the next record
              acc
            case _ => throw new ProduceRequestInterceptorUnhandledException(throwable)
          }
        }
      }
    }

  }

  private def processRecord(record: Record, topic: String, partition: Int, interceptors: Seq[ProduceRequestInterceptor]): Try[SimpleRecord] = {
    val asSimpleRecord = new SimpleRecord(record)
    // The interceptor might throw, so we wrap this method in a try
    Try {
      interceptors.foldLeft(asSimpleRecord) { case (rec, inter) =>
        val originalKey = getFieldBytes(rec.key())
        val originalValue = getFieldBytes(rec.value())
        val interceptorResult = inter.processRecord(originalKey, originalValue, topic, partition, rec.headers())
        new SimpleRecord(rec.timestamp(), interceptorResult.getKey, interceptorResult.getValue, rec.headers())
      }
    }
  }

  private def getFieldBytes(field: ByteBuffer): Array[Byte] = {
    if (field == null) null
    else {
      val bytes = Array.ofDim[Byte](field.remaining())
      field.get(bytes)
      bytes
    }
  }

  private def rebuildMemoryRecords(originalMemoryRecords: MemoryRecords, newRecords: Seq[SimpleRecord]): MemoryRecords = {
    val firstBatch = originalMemoryRecords.firstBatch()
    if (newRecords.isEmpty || Option(firstBatch).isEmpty) MemoryRecords.EMPTY
    else {
      MemoryRecords.withRecords(
        firstBatch.magic(),
        firstBatch.baseOffset(),
        firstBatch.compressionType(),
        firstBatch.timestampType(),
        firstBatch.producerId(),
        firstBatch.producerEpoch(),
        firstBatch.baseSequence(),
        firstBatch.partitionLeaderEpoch(),
        firstBatch.isTransactional,
        newRecords: _*
      )
    }
  }
}

object ProduceRequestInterceptorManager {
  def apply() = new ProduceRequestInterceptorManager()
  def apply(interceptors: List[ProduceRequestInterceptor], interceptorTimeoutMs: Int, maxRetriesOnTimeout: Int) =
    new ProduceRequestInterceptorManager(interceptors, interceptorTimeoutMs, maxRetriesOnTimeout)
}
