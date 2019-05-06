package unit.kafka.server.checkpoints

import java.io.File
import java.util.UUID

import kafka.server.LogDirFailureChannel
import kafka.server.checkpoints.{CheckpointFile, OffsetCheckpointFileEntry}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.KafkaStorageException
import org.junit.function.ThrowingRunnable
import org.junit.{Assert, Test}

class CheckpointFileTest {
  @Test
  def testRoundTripCheckpoints(): Unit = {
    val file = File.createTempFile("temp-checkpoint-file", UUID.randomUUID().toString)
    file.deleteOnExit()
    val failureChannel = new LogDirFailureChannel(1)
    val checkpointFile = CheckpointFile(file, 0, failureChannel, file.getParent, OffsetCheckpointFileEntry.apply)
    val entries = Seq(
      new OffsetCheckpointFileEntry(new TopicPartition("foo", 0), 0L),
      new OffsetCheckpointFileEntry(new TopicPartition("foo", 1), 1L),
      new OffsetCheckpointFileEntry(new TopicPartition("foo", 2), 2L))
    checkpointFile.write(entries)
    val readEntries = checkpointFile.read()
    for (entry <- readEntries) {
      Assert.assertEquals(entry.topicPartition.topic(), "foo")
      Assert.assertEquals(entry.topicPartition.partition(), entry.offset)
    }
  }

  @Test
  def testReadDeletedFile(): Unit = {
    val file = File.createTempFile("temp-checkpoint-file", UUID.randomUUID().toString)
    file.deleteOnExit()
    val failureChannel = new LogDirFailureChannel(1)
    val checkpointFile = CheckpointFile(file, 0, failureChannel, file.getParent, OffsetCheckpointFileEntry.apply)
    file.delete()
    Assert.assertThrows(
      "Expected read of deleted file to throw KafkaStorageException",
      classOf[KafkaStorageException], new ThrowingRunnable {
        override def run(): Unit = {
          checkpointFile.read()
        }
      })
    Assert.assertEquals("Expected checkpoint logdir to be failed", failureChannel.takeNextOfflineLogDir(), file.getParent)
  }
}
