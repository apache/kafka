/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka

import java.io._
import java.nio._
import java.nio.channels._
import java.util.{Properties, Random}

import joptsimple._
import kafka.log._
import kafka.message._
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.common.record._
import org.apache.kafka.common.utils.{Time, Utils}

import scala.math._

/**
 * This test does linear writes using either a kafka log or a file and measures throughput and latency.
 */
object TestLinearWriteSpeed {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser(false)
    val dirOpt = parser.accepts("dir", "The directory to write to.")
                           .withRequiredArg
                           .describedAs("path")
                           .ofType(classOf[java.lang.String])
                           .defaultsTo(System.getProperty("java.io.tmpdir"))
    val bytesOpt = parser.accepts("bytes", "REQUIRED: The total number of bytes to write.")
                           .withRequiredArg
                           .describedAs("num_bytes")
                           .ofType(classOf[java.lang.Long])
    val sizeOpt = parser.accepts("size", "REQUIRED: The size of each write.")
                           .withRequiredArg
                           .describedAs("num_bytes")
                           .ofType(classOf[java.lang.Integer])
    val messageSizeOpt = parser.accepts("message-size", "REQUIRED: The size of each message in the message set.")
                           .withRequiredArg
                           .describedAs("num_bytes")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1024)
    val filesOpt = parser.accepts("files", "REQUIRED: The number of logs or files.")
                           .withRequiredArg
                           .describedAs("num_files")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
   val reportingIntervalOpt = parser.accepts("reporting-interval", "The number of ms between updates.")
                           .withRequiredArg
                           .describedAs("ms")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(1000L)
   val maxThroughputOpt = parser.accepts("max-throughput-mb", "The maximum throughput.")
                           .withRequiredArg
                           .describedAs("mb")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(Integer.MAX_VALUE)
   val flushIntervalOpt = parser.accepts("flush-interval", "The number of messages between flushes")
                           .withRequiredArg()
                           .describedAs("message_count")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(Long.MaxValue)
   val compressionCodecOpt = parser.accepts("compression", "The compression codec to use")
                            .withRequiredArg
                            .describedAs("codec")
                            .ofType(classOf[java.lang.String])
                            .defaultsTo(NoCompressionCodec.name)
   val mmapOpt = parser.accepts("mmap", "Do writes to memory-mapped files.")
   val channelOpt = parser.accepts("channel", "Do writes to file channels.")
   val logOpt = parser.accepts("log", "Do writes to kafka logs.")

    val options = parser.parse(args : _*)

    CommandLineUtils.checkRequiredArgs(parser, options, bytesOpt, sizeOpt, filesOpt)

    var bytesToWrite = options.valueOf(bytesOpt).longValue
    val bufferSize = options.valueOf(sizeOpt).intValue
    val numFiles = options.valueOf(filesOpt).intValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).longValue
    val dir = options.valueOf(dirOpt)
    val maxThroughputBytes = options.valueOf(maxThroughputOpt).intValue * 1024L * 1024L
    val buffer = ByteBuffer.allocate(bufferSize)
    val messageSize = options.valueOf(messageSizeOpt).intValue
    val flushInterval = options.valueOf(flushIntervalOpt).longValue
    val compressionCodec = CompressionCodec.getCompressionCodec(options.valueOf(compressionCodecOpt))
    val rand = new Random
    rand.nextBytes(buffer.array)
    val numMessages = bufferSize / (messageSize + MessageSet.LogOverhead)
    val createTime = System.currentTimeMillis
    val messageSet = {
      val compressionType = CompressionType.forId(compressionCodec.codec)
      val records = (0 until numMessages).map(_ => new SimpleRecord(createTime, null, new Array[Byte](messageSize)))
      MemoryRecords.withRecords(compressionType, records: _*)
    }

    val writables = new Array[Writable](numFiles)
    val scheduler = new KafkaScheduler(1)
    scheduler.startup()
    for(i <- 0 until numFiles) {
      if(options.has(mmapOpt)) {
        writables(i) = new MmapWritable(new File(dir, "kafka-test-" + i + ".dat"), bytesToWrite / numFiles, buffer)
      } else if(options.has(channelOpt)) {
        writables(i) = new ChannelWritable(new File(dir, "kafka-test-" + i + ".dat"), buffer)
      } else if(options.has(logOpt)) {
        val segmentSize = rand.nextInt(512)*1024*1024 + 64*1024*1024 // vary size to avoid herd effect
        val logProperties = new Properties()
        logProperties.put(LogConfig.SegmentBytesProp, segmentSize: java.lang.Integer)
        logProperties.put(LogConfig.FlushMessagesProp, flushInterval: java.lang.Long)
        writables(i) = new LogWritable(new File(dir, "kafka-test-" + i), new LogConfig(logProperties), scheduler, messageSet)
      } else {
        System.err.println("Must specify what to write to with one of --log, --channel, or --mmap")
        Exit.exit(1)
      }
    }
    bytesToWrite = (bytesToWrite / numFiles) * numFiles

    println("%10s\t%10s\t%10s".format("mb_sec", "avg_latency", "max_latency"))

    val beginTest = System.nanoTime
    var maxLatency = 0L
    var totalLatency = 0L
    var count = 0L
    var written = 0L
    var totalWritten = 0L
    var lastReport = beginTest
    while(totalWritten + bufferSize < bytesToWrite) {
      val start = System.nanoTime
      val writeSize = writables((count % numFiles).toInt.abs).write()
      val ellapsed = System.nanoTime - start
      maxLatency = max(ellapsed, maxLatency)
      totalLatency += ellapsed
      written += writeSize
      count += 1
      totalWritten += writeSize
      if((start - lastReport)/(1000.0*1000.0) > reportingInterval.doubleValue) {
        val ellapsedSecs = (start - lastReport) / (1000.0*1000.0*1000.0)
        val mb = written / (1024.0*1024.0)
        println("%10.3f\t%10.3f\t%10.3f".format(mb / ellapsedSecs, totalLatency / count.toDouble / (1000.0*1000.0), maxLatency / (1000.0 * 1000.0)))
        lastReport = start
        written = 0
        maxLatency = 0L
        totalLatency = 0L
      } else if(written > maxThroughputBytes * (reportingInterval / 1000.0)) {
        // if we have written enough, just sit out this reporting interval
        val lastReportMs = lastReport / (1000*1000)
        val now = System.nanoTime / (1000*1000)
        val sleepMs = lastReportMs + reportingInterval - now
        if(sleepMs > 0)
          Thread.sleep(sleepMs)
      }
    }
    val elapsedSecs = (System.nanoTime - beginTest) / (1000.0*1000.0*1000.0)
    println(bytesToWrite / (1024.0 * 1024.0 * elapsedSecs) + " MB per sec")
    scheduler.shutdown()
  }

  trait Writable {
    def write(): Int
    def close()
  }

  class MmapWritable(val file: File, size: Long, val content: ByteBuffer) extends Writable {
    file.deleteOnExit()
    val raf = new RandomAccessFile(file, "rw")
    raf.setLength(size)
    val buffer = raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length())
    def write(): Int = {
      buffer.put(content)
      content.rewind()
      content.limit()
    }
    def close() {
      raf.close()
    }
  }

  class ChannelWritable(val file: File, val content: ByteBuffer) extends Writable {
    file.deleteOnExit()
    val raf = new RandomAccessFile(file, "rw")
    val channel = raf.getChannel
    def write(): Int = {
      channel.write(content)
      content.rewind()
      content.limit()
    }
    def close() {
      raf.close()
    }
  }

  class LogWritable(val dir: File, config: LogConfig, scheduler: Scheduler, val messages: MemoryRecords) extends Writable {
    Utils.delete(dir)
    val log = Log(dir, config, 0L, 0L, scheduler, new BrokerTopicStats, Time.SYSTEM, 60 * 60 * 1000,
      LogManager.ProducerIdExpirationCheckIntervalMs, new LogDirFailureChannel(10))
    def write(): Int = {
      log.appendAsLeader(messages, leaderEpoch = 0)
      messages.sizeInBytes
    }
    def close() {
      log.close()
      Utils.delete(log.dir)
    }
  }

}
