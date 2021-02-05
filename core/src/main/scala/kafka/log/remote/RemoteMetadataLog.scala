package kafka.log.remote

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.MessageFormatter
import org.apache.kafka.common.log.remote.metadata.storage.RLSMSerDe
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentMetadata
import org.apache.kafka.common.record.Record
import org.apache.kafka.common.serialization.Serdes.StringSerde
import org.apache.kafka.common.utils.Utils

import java.io.PrintStream
import java.nio.ByteBuffer
import java.nio.ByteBuffer.wrap

object RemoteMetadataLog {

  val keySerde = new StringSerde
  val valSerde = new RLSMSerDe

  private[remote] def keyToBytes(data: String): Array[Byte] = {
    keySerde.serializer().serialize(null, data)
  }

  private def readMetadataRecordKey(data: ByteBuffer): String = {
    keySerde.deserializer().deserialize(null, Utils.readBytes(data))
  }

  private[remote] def valueToBytes(data: RemoteLogSegmentMetadata): Array[Byte] = {
    valSerde.serializer().serialize(null, data)
  }

  private def readMetadataRecordValue(buffer: ByteBuffer): Option[RemoteLogSegmentMetadata] = {
    if (buffer == null) {
      None
    } else {
      Some(valSerde.deserializer().deserialize(null, Utils.readBytes(buffer)))
    }
  }

  class RMLMessageFormatter extends MessageFormatter {
    /**
     * Parses and formats a record for display
     *
     * @param consumerRecord the record to format
     * @param output         the print stream used to output the record
     */
    override def writeTo(consumerRecord: ConsumerRecord[Array[Byte], Array[Byte]], output: PrintStream): Unit = {
      Option(consumerRecord.key()).map(key => readMetadataRecordKey(wrap(key))).foreach { recordKey =>
        val value = readMetadataRecordValue(wrap(consumerRecord.value()))
        var result = "partition: " + recordKey + ", message-offset: " + consumerRecord.offset()
        value.foreach(x => {
          result += ", type: " + x.getClass.getSimpleName
          result += ", event-value: " + x.toString
        })
        output.println(result)
      }
    }
  }

  /**
   * Exposed for printing records using [[kafka.tools.DumpLogSegments]]
   */
  def formatRecordKeyAndValue(record: Record): (Option[String], Option[String]) = {
    val key = readMetadataRecordKey(record.key())
    val payload = readMetadataRecordValue(record.value()) match {
      case None => "<EMPTY>"
      case Some(value) => value.toString
    }
    (Some(key), Some(payload))
  }
}