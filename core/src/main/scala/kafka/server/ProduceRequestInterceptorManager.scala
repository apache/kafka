package kafka.server

import org.apache.kafka.common.record.{MemoryRecords, Record, SimpleRecord}
import org.apache.kafka.common.requests.ProduceRequest

import java.nio.ByteBuffer
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

class ProduceRequestInterceptorManager(interceptors: Iterable[ProduceRequestInterceptor] = Iterable.empty) {
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
        val asSimpleRecord = new SimpleRecord(record)
        val newRecord = interceptors.foldLeft(Success(asSimpleRecord): Try[SimpleRecord]) { case (rec, inter) =>
          rec match {
            case Success(r) => applySingleInterceptor(inter, r)
            case _ => rec
          }
        }
        newRecord match {
          case Success(newRec) => acc :+ newRec
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

  private def applySingleInterceptor(producerRequestInterceptor: ProduceRequestInterceptor, record: SimpleRecord): Try[SimpleRecord] = {
    val originalKey = record.key()
    val originalValue = record.value()
    processOriginalRecordField(originalKey, producerRequestInterceptor.processKey)
      .flatMap { newKey =>
        processOriginalRecordField(originalValue, producerRequestInterceptor.processValue).map(newValue => (newKey, newValue))
      }.map { case (newKey, newValue) =>
      new SimpleRecord(record.timestamp(), newKey, newValue, record.headers())
    }
  }

  private def processOriginalRecordField(field: ByteBuffer, processFn: Array[Byte] => Array[Byte]): Try[ByteBuffer] = {
    Try {
      val bytes = Array.ofDim[Byte](field.remaining())
      field.get(bytes)
      val processed = processFn(bytes)
      ByteBuffer.wrap(processed)
    }
  }

  private def rebuildMemoryRecords(originalMemoryRecords: MemoryRecords, newRecords: Seq[SimpleRecord]): MemoryRecords = {
    val firstBatch = originalMemoryRecords.firstBatch()
    if (newRecords.isEmpty || Option(firstBatch).isEmpty) MemoryRecords.EMPTY
    else {
      // All the batches should have the same magic version according to this test:
      // org.apache.kafka.server.log.internals.LogValidator.validateBatch
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
  def apply(interceptors: List[ProduceRequestInterceptor]) = new ProduceRequestInterceptorManager(interceptors)
}
