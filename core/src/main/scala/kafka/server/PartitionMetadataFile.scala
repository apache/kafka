package kafka.server

import java.io.{BufferedReader, BufferedWriter, File, FileOutputStream, IOException, OutputStreamWriter}
import java.nio.charset.StandardCharsets
import java.nio.file.{FileAlreadyExistsException, Files, Paths}
import java.util.UUID
import java.util.regex.Pattern

import kafka.utils.Logging
import org.apache.kafka.common.errors.KafkaStorageException
import org.apache.kafka.common.utils.Utils



object PartitionMetadataFile {
  private val LeaderEpochCheckpointFilename = "partition.metadata"
  private val WhiteSpacesPattern = Pattern.compile(":\\s+")
  private val CurrentVersion = 0

  def newFile(dir: File): File = new File(dir, LeaderEpochCheckpointFilename)

  object PartitionMetadataFileFormatter {
    def toFile(data: PartitionMetadata): String = {
      s"Metadata schema version: ${data.version}\nTopic ID: ${data.topicId}"
    }

  }

  class PartitionMetadataReadBuffer[T](location: String,
                                       reader: BufferedReader,
                                       version: Int) extends Logging {
    def read(): PartitionMetadata = {
      def malformedLineException(line: String) =
        new IOException(s"Malformed line in checkpoint file ($location): '$line'")

      var line: String = null
      var metadataTopicId: UUID = null
      try {
        line = reader.readLine()
        WhiteSpacesPattern.split(line) match {
          case Array(_, version) =>
            if (version.toInt == CurrentVersion) {
              line = reader.readLine()
              WhiteSpacesPattern.split(line) match {
                case Array(_, topicId) => metadataTopicId = UUID.fromString(topicId)
                case _ => throw malformedLineException(line)
              }
              new PartitionMetadata(CurrentVersion, metadataTopicId)
            } else {
              throw new IOException(s"Unrecognized version of the checkpoint file ($location): " + version)
            }
          case _ => throw malformedLineException(line)
        }
      } catch {
        case _: NumberFormatException => throw malformedLineException(line)
      }
    }
  }

}

class PartitionMetadata(val version: Int, val topicId: UUID)


class PartitionMetadataFile(val file: File,
                            logDirFailureChannel: LogDirFailureChannel) extends Logging {
  import kafka.server.PartitionMetadataFile.{CurrentVersion, PartitionMetadataFileFormatter, PartitionMetadataReadBuffer}

  private val path = file.toPath.toAbsolutePath
  private val tempPath = Paths.get(path.toString + ".tmp")
  private val lock = new Object()
  private val logDir = file.getParentFile.getParent


  try Files.createFile(file.toPath) // create the file if it doesn't exist
  catch { case _: FileAlreadyExistsException => }

  def write(topicId: UUID): Unit = {
    lock synchronized {
      try {
        // write to temp file and then swap with the existing file
        val fileOutputStream = new FileOutputStream(tempPath.toFile)
        val writer = new BufferedWriter(new OutputStreamWriter(fileOutputStream, StandardCharsets.UTF_8))
        try {
          writer.write(PartitionMetadataFileFormatter.toFile(new PartitionMetadata(CurrentVersion,topicId)))
          writer.flush()
          fileOutputStream.getFD().sync()
        } finally {
          writer.close()
        }

        Utils.atomicMoveWithFallback(tempPath, path)
      } catch {
        case e: IOException =>
          val msg = s"Error while writing to partition metadata file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }

  def read(): PartitionMetadata = {
    lock synchronized {
      try {
        val reader = Files.newBufferedReader(path)
        try {
          val partitionBuffer = new PartitionMetadataReadBuffer(file.getAbsolutePath, reader, CurrentVersion)
          partitionBuffer.read()
        } finally {
          reader.close()
        }
      } catch {
        case e: IOException =>
          val msg = s"Error while reading partition metadata file ${file.getAbsolutePath}"
          logDirFailureChannel.maybeAddOfflineLogDir(logDir, msg, e)
          throw new KafkaStorageException(msg, e)
      }
    }
  }

  def notExists(): Boolean = {
    !file.exists()
  }

  def isEmpty(): Boolean = {
    file.length() == 0
  }
}
