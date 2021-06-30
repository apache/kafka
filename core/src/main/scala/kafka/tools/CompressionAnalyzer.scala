package kafka.tools

import java.io._
import java.nio.ByteBuffer

import kafka.log._
import kafka.utils._
import org.apache.kafka.common.record._

import scala.collection.JavaConverters._
import scala.collection.mutable

object CompressionAnalyzer {

  def main(args: Array[String]) {
    val opts = new DumpLogSegmentsOptions(args)
    CommandLineUtils.printHelpAndExitIfNeeded(opts, "This tool analyzes the compressibility of a log segment, which is useful for determining the most appropriate compression algorithm for a topic.")
    opts.checkArgs()

    for (arg <- opts.logFiles) {
      val file = new File(arg)
      val filename = file.getName
      val suffix = filename.substring(filename.lastIndexOf("."))
      suffix match {
        case Log.LogFileSuffix =>
          println(s"\nAnalyzing $file")
          analyzeLog(file, opts.verbose)
        case _ =>
          System.err.println(s"Ignoring unknown file $file")
      }
    }
  }

  private def compressBatch(batch: RecordBatch, compressionType: CompressionType) : Int = {
    val buffer = ByteBuffer.allocate(128)
    val builder = MemoryRecords.builder(buffer, batch.magic(), compressionType, batch.timestampType(), batch.baseOffset())
    for (record <- batch.asScala) {
      builder.append(record)
    }
    builder.build.sizeInBytes
  }

  private def maybeDecompressBatch(batch: RecordBatch): RecordBatch = {
    if (batch.compressionType() == CompressionType.NONE) {
      batch
    } else {
      val buffer = ByteBuffer.allocate(128)
      val builder = MemoryRecords.builder(buffer, batch.magic(), CompressionType.NONE, batch.timestampType(), batch.baseOffset())
      for (record <- batch.asScala) {
        // There are some edge cases here for older magic, but we don't care right now
        builder.append(record)
      }
      builder.build().firstBatch()
    }
  }

  private def analyzeLog(file: File, verbose: Boolean) {
    var validBytes = 0L
    var uncompressedBytes = 0L
    val fileRecords = FileRecords.open(file, false)

    var batchCount = 0
    var largeBatches = 0
    var messageCount = 0
    var avgBatchSize = 0.0
    var avgUncompressedBatchSize = 0.0

    val geoMeanMap = new mutable.HashMap[CompressionType, Double].withDefaultValue(0)
    val compressedSize = new mutable.HashMap[CompressionType, Int].withDefaultValue(0)
    val compressionTimeMs = new mutable.HashMap[CompressionType, Long].withDefaultValue(0)
    val batchesByCompressionType = new mutable.HashMap[CompressionType, Int].withDefaultValue(0)

    for (batch <- fileRecords.batches.asScala) {
      if (verbose)
        printBatchInfo(batch, validBytes)

      validBytes += batch.sizeInBytes
      avgBatchSize = avgBatchSize + Math.log(batch.sizeInBytes)
      batchesByCompressionType(batch.compressionType) += 1

      // Decompress the input batch if needed
      val uncompressedBatch = maybeDecompressBatch(batch)
      uncompressedBytes += uncompressedBatch.sizeInBytes
      avgUncompressedBatchSize += Math.log(uncompressedBatch.sizeInBytes)

      if (verbose)
        println(" uncompressedSize: " + uncompressedBatch.sizeInBytes)

      // Compress and record results
      for (compressionType <- CompressionType.values().filter(_ != CompressionType.NONE)) {
        val startTime = System.currentTimeMillis()
        val bytes = compressBatch(uncompressedBatch, compressionType)
        compressionTimeMs(compressionType) += System.currentTimeMillis() - startTime

        compressedSize(compressionType) += bytes
        geoMeanMap(compressionType) += Math.log(bytes / uncompressedBatch.sizeInBytes.toDouble)
      }

      batchCount += 1
      messageCount += batch.countOrNull
      if (batch.countOrNull > 1)
        largeBatches += 1
    }
    avgBatchSize = Math.exp(avgBatchSize.toFloat / batchCount)
    avgUncompressedBatchSize = Math.exp(avgUncompressedBatchSize.toFloat / batchCount)

    println(f"Original log size: $validBytes bytes")
    println(f"Uncompressed log size: $uncompressedBytes bytes")
    println(f"Original compression ratio: ${uncompressedBytes / validBytes.toFloat}%.2f")
    println(f"Original space savings: ${100 * (1 - validBytes / uncompressedBytes.toFloat)}%.2f%%")

    println("\nBatch stats:")
    println(f"  $largeBatches/$batchCount batches contain >1 message")
    println(f"  Avg messages/batch: ${messageCount.toFloat / batchCount}%.2f")
    println(f"  Avg batch size (original): $avgBatchSize%.0f bytes")
    println(f"  Avg batch size (uncompressed): $avgUncompressedBatchSize%.0f bytes")

    println("\nNumber of input batches by compression type:")
    for ((k, v) <- batchesByCompressionType.toSeq.sorted) {
      println(f"  ${k.name}: $v")
    }

    println(s"\n%16s %16s %14s %12s %16s %11s %12s"
      .format("COMPRESSION-TYPE", "COMPRESSED-SIZE", "SPACE-SAVINGS", "TOTAL-RATIO", "AVG-RATIO/BATCH", "TOTAL-TIME", "SPEED"))

    for ((k, v) <- geoMeanMap.toSeq.sorted) {
      val compressedSizeBytes = compressedSize(k)
      val spaceSavings = 100 * (1 - compressedSizeBytes / uncompressedBytes.toFloat)
      val compressionRatio = uncompressedBytes.toFloat / compressedSizeBytes
      val batchCompressionRatio = 1.0 / Math.exp(v / batchCount)
      val time = compressionTimeMs(k)
      val speed = (uncompressedBytes.toFloat / 1024 / 1024) / (time.toFloat / 1000)
      val compressionType = k.name.padTo(7, ' ')

      println(f"$compressionType%-16s $compressedSizeBytes%16d $spaceSavings%13.2f%% $compressionRatio%12.2f $batchCompressionRatio%16.2f $time%9dms $speed%7.2f MB/s")
    }
  }

  private def printBatchInfo(batch: FileLogInputStream.FileChannelRecordBatch, accumulativeBytes: Long): Unit = {
    if (batch.magic >= RecordBatch.MAGIC_VALUE_V2)
      print("baseOffset: " + batch.baseOffset + " lastOffset: " + batch.lastOffset + " count: " + batch.countOrNull +
        " isTransactional: " + batch.isTransactional + " isControl: " + batch.isControlBatch)
    else
      print("offset: " + batch.lastOffset)

    print(" position: " + accumulativeBytes + " size: " + batch.sizeInBytes + " magic: " + batch.magic +
      " compresscodec: " + batch.compressionType)
  }

  private class DumpLogSegmentsOptions(args: Array[String]) extends CommandDefaultOptions(args) {
    val logsOpt = parser.accepts("logs", "REQUIRED: The comma separated list of log files to be analyzed.")
      .withRequiredArg
      .describedAs("log1, log2, ...")
      .ofType(classOf[String])
    val verboseOpt = parser.accepts("verbose", "if set, display verbose batch information.")
      .withOptionalArg()
      .ofType(classOf[Boolean])

    options = parser.parse(args : _*)

    lazy val verbose: Boolean = options.has(verboseOpt)
    lazy val logFiles = options.valueOf(logsOpt).split(",")

    def checkArgs(): Unit = CommandLineUtils.checkRequiredArgs(parser, options, logsOpt)
  }
}
