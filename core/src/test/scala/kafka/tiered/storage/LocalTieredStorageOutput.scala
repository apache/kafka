package kafka.tiered.storage

import java.nio.ByteBuffer

import kafka.utils.nonthreadsafe
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.log.remote.storage.RemoteLogSegmentFileset.RemoteLogSegmentFileType.{OFFSET_INDEX, SEGMENT, TIME_INDEX}
import org.apache.kafka.common.log.remote.storage.{LocalTieredStorage, LocalTieredStorageTraverser, RemoteLogSegmentFileset}
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.utils.Utils

import scala.jdk.CollectionConverters._

@nonthreadsafe
final class LocalTieredStorageOutput[K, V](val keyDe: Deserializer[K],
                                           val valueDe: Deserializer[V]) extends LocalTieredStorageTraverser {
  private[storage] var output: String =
    row("File", "Base offset", "End offset", "First record", "Last record")

  output += "-" * (55 + 12 + 12 + 13 + 13 + (4 * 2)) + "\n" // Columns length + 4 column separators.

  private def row(c1: String = "", c2: Any = "", c3: Any = "", c4: String = "", c5: String = "", ident: String = " " * 4) = {
    f"${ident + c1}%-55s |${c2.toString}%12s |${c3.toString}%12s |$c4%13s |$c5%13s\n"
  }

  private var currentTopic: String = ""

  override def visitTopicPartition(topicPartition: TopicPartition): Unit = {
    currentTopic = topicPartition.topic()
    output += row(topicPartition.toString, ident = "")
  }

  override def visitSegment(fileset: RemoteLogSegmentFileset): Unit = {
    val records = fileset.getRecords.asScala.toList
    var (baseOffset, endOffset, baseKey, baseValue, endKey, endValue) = (-1L, -1L, "", "", "", "")

    def des(de: Deserializer[_])(bytes: ByteBuffer): String = {
      de.deserialize(currentTopic, Utils.toNullableArray(bytes)).toString
    }

    if (!records.isEmpty) {
      val (head, tail) = (records.head, records.last)
      baseOffset = head.offset()
      baseKey = des(keyDe)(head.key())
      baseValue = des(valueDe)(head.value())
      endOffset = tail.offset()
      endKey = des(keyDe)(tail.key())
      endValue = des(valueDe)(tail.value())
    }

    output += row(
      fileset.getFile(SEGMENT).getName,
      baseOffset,
      endOffset,
      s"($baseKey, $baseValue)",
      s"($endKey, $endValue)"
    )

    output += row(fileset.getFile(OFFSET_INDEX).getName)
    output += row(fileset.getFile(TIME_INDEX).getName)
    output += row()
  }
}

object DumpLocalTieredStorage {

  def dump[K, V](storage: LocalTieredStorage, keyDe: Deserializer[K], valueDe: Deserializer[V]): String = {
    val output = new LocalTieredStorageOutput(keyDe, valueDe)
    storage.traverse(output)
    output.output
  }

}
