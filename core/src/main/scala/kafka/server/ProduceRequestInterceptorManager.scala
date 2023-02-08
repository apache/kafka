package kafka.server

import org.apache.kafka.common.record.{MemoryRecords, Record, SimpleRecord}
import org.apache.kafka.common.requests.ProduceRequest

import java.nio.ByteBuffer
import java.util.concurrent.{ExecutorService, Executors}
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ProduceRequestInterceptorManager(interceptors: Iterable[ProduceRequestInterceptor] = Iterable.empty,
                                       recordProcessingTimeoutMs: Int = 10) extends AutoCloseable {

  private val executorService: ExecutorService = Executors.newSingleThreadExecutor()
  private implicit val executionContext: ExecutionContext =
    ExecutionContext.fromExecutorService(executorService)
  private val recordProcessingTimeoutDuration: Duration = recordProcessingTimeoutMs.millis

  override def close(): Unit = executorService.shutdownNow()

  def applyInterceptors(produceRequest: ProduceRequest): Unit = {
    if (interceptors.isEmpty) {

    } else {
      produceRequest.data().topicData().forEach {
        topic =>
          topic.partitionData().forEach {
            partitionData => {
              val memoryRecords = partitionData.records.asInstanceOf[MemoryRecords]
              val newMemoryRecords = Try { memoryRecords.records().asScala.toSeq }
                .flatMap(originalRecords => processRecords(originalRecords))
                .map(newRecords => rebuildMemoryRecords(memoryRecords, newRecords))
                .getOrElse {
                  // TODO: Add option to fail the entire request if an interceptor fails
                  memoryRecords
                }
              partitionData.setRecords(newMemoryRecords)
            }
          }
      }
    }
  }

  private def processRecords(originalRecords: Seq[Record]): Try[Seq[SimpleRecord]] = {
    val accumulator = List.empty[SimpleRecord]
    Try {
      originalRecords.foldLeft(accumulator) { (acc, record) =>
        val newRecordTry = applyInterceptors(record)
        newRecordTry match {
          case Success(newRecord) => acc :+ newRecord
          case Failure(throwable) => {
            throwable match {
              case prie: ProduceRequestInterceptorException =>
                // TODO: Log error
                acc
              case _ => throw throwable
            }
          }
        }
      }
    }
  }

  def applyInterceptors(record: Record): Try[SimpleRecord] = {
    val asSimpleRecord = new SimpleRecord(record)
    Try {
      interceptors.foldLeft(asSimpleRecord) { case (rec, inter) =>
        applySingleInterceptor(inter, rec)
      }
    }
  }

  private def applySingleInterceptor(producerRequestInterceptor: ProduceRequestInterceptor, record: SimpleRecord): SimpleRecord = {
    val originalKey = record.key()
    val originalValue = record.value()
    processRecordField(originalKey, producerRequestInterceptor.processKey)
      .flatMap { newKey =>
        processRecordField(originalValue, producerRequestInterceptor.processValue).map(newValue => (newKey, newValue))
      }
      .map { case (newKey, newValue) =>
        new SimpleRecord(record.timestamp(), newKey, newValue, record.headers())
    }.get
  }

  private def processRecordField(field: ByteBuffer, processFn: Array[Byte] => Array[Byte]): Try[ByteBuffer] = {
    if (field == null) Success(field)
    else {
      Try {
        val bytes = Array.ofDim[Byte](field.remaining())
        field.get(bytes)
        val processed: Array[Byte] = Await.result(Future {
          processFn(bytes)
        }, recordProcessingTimeoutDuration)
        ByteBuffer.wrap(processed)
      }
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
  def apply(interceptors: List[ProduceRequestInterceptor], recordProcessingTimeoutMs: Int) =
    new ProduceRequestInterceptorManager(interceptors, recordProcessingTimeoutMs)
}
