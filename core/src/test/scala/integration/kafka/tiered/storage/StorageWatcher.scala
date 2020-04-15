package integration.kafka.tiered.storage

import java.io.File

import org.apache.kafka.common.TopicPartition

final class StorageWatcher(private val storageDirname: String) {
  private val storageDirectory = new File(storageDirname)

  def getEarliestOffset(topicPartition: TopicPartition): Unit = {
    val files = storageDirectory.listFiles().toSeq
    val topicPartitionDir = files.map(_.getName).find(_ == topicPartition.toString).getOrElse {
      throw new IllegalArgumentException(s"Directory for the topic-partition $topicPartition was not found")
    }

    //val topicPartitionFiles =
  }

}
