package kafka.server.checkpoints

import org.apache.kafka.common.TopicPartition
import java.util
import java.io.IOException

trait CheckpointFileEntry {
  /**
    * Encodes this CheckpointFileEntry as a line of text for the given StringBuilder.
    */
  def writeLine(stringBuilder: StringBuilder): Unit
}

final case class OffsetCheckpointFileEntry(topicPartition: TopicPartition, offset: Long) extends CheckpointFileEntry() {
  /**
    * Encodes this CheckpointFileEntry as a line of text for the given StringBuilder.
    */
  override def writeLine(stringBuilder: StringBuilder): Unit = {
    stringBuilder
      .append(topicPartition.topic)
      .append(' ')
      .append(topicPartition.partition)
      .append(' ')
      .append(offset)
  }
}

object OffsetCheckpointFileEntry {
  private val SEPARATOR: util.regex.Pattern = util.regex.Pattern.compile("\\s")
  def apply(line: String): OffsetCheckpointFileEntry = {
    SEPARATOR.split(line) match {
      case Array(topic, partition, offset) => OffsetCheckpointFileEntry(new TopicPartition(topic, partition.toInt), offset.toLong)
      case _ =>
        throw new IOException(s"Expected 3 elements for OffsetCheckpointFileEntry")
    }
  }
}
