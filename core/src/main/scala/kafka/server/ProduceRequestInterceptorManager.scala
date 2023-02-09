package kafka.server

import org.apache.kafka.common.record.{MemoryRecords, Record, SimpleRecord}
import org.apache.kafka.common.requests.ProduceRequest

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, Executors, TimeoutException}
import scala.annotation.tailrec
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

  def applyInterceptors(produceRequest: ProduceRequest): Unit = {
    if (interceptors.isEmpty) {

    } else {
      produceRequest.data().topicData().forEach {
        topic =>
          topic.partitionData().forEach {
            partitionData => {
              val parsedInput = Try {
                val memoryRecords = partitionData.records.asInstanceOf[MemoryRecords]
                val originalRecords = memoryRecords.records().asScala.toSeq
                (memoryRecords, originalRecords)
              }
              parsedInput match {
                case Success((memoryRecords, originalRecords)) =>
                  val processedRecords = processRecordsWithTimeout(originalRecords, topic.name(), partitionData.index())
                  val newMemoryRecords = rebuildMemoryRecords(memoryRecords, processedRecords)
                  partitionData.setRecords(newMemoryRecords)
                case _ => // There is something wrong with the input that will be handled by donwstream validators
              }
            }
          }
      }
    }
  }

  @tailrec
  private def processRecordsWithTimeout(originalRecords: Seq[Record], topic: String, partition: Int, attempt: Int = 0): Seq[SimpleRecord] = {
    val res = Try {
      Await.result(
        Future { processRecords(originalRecords, topic, partition) },
        interceptorTimeoutDuration
      )
    }
    res match {
      case Success(newRecords) => newRecords
      case Failure(throwable) => {
       throwable match {
         case _: TimeoutException =>
           if (attempt < maxRetriesOnTimeout) processRecordsWithTimeout(originalRecords, topic, partition, attempt + 1)
           else {
             // TODO: Replace with an error that corresponds to a meaningful Kafka error code
             throw throwable
           }
         case _ => throw throwable
       }
      }
    }
  }

  private def processRecords(originalRecords: Seq[Record], topic: String, partition: Int): Seq[SimpleRecord] = {
    val accumulator = List.empty[SimpleRecord]
    originalRecords.foldLeft(accumulator) { (acc, record) =>
      val newRecordTry = applyInterceptors(record, topic, partition)
      newRecordTry match {
        case Success(newRecord) => acc :+ newRecord
        case Failure(throwable) => {
          throwable match {
            case _: ProduceRequestInterceptorSkipRecordException =>
              // TODO: Log error
              // Skip adding the record to the accumulator, and move onto the next record
              acc
            case _ =>
              // TODO: Replace this error with one that corresponds to a meaningful failed response status
              throw throwable
          }
        }
      }
    }

  }

  private def applyInterceptors(record: Record, topic: String, partition: Int): Try[SimpleRecord] = {
    val asSimpleRecord = new SimpleRecord(record)
    // The interceptor might throw, so we wrap this method in a try
    Try {
      interceptors.foldLeft(asSimpleRecord) { case (rec, inter) =>
        val originalKey = getFieldBytes(rec.key())
        val originalValue = getFieldBytes(rec.value())
        val interceptorResult = inter.processRecord(originalKey, originalValue, topic, partition, rec.headers())
        new SimpleRecord(rec.timestamp(), interceptorResult.key, interceptorResult.value, rec.headers())
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
