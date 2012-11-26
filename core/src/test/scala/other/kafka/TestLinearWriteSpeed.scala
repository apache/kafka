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
import scala.math._
import joptsimple._

object TestLinearWriteSpeed {

  def main(args: Array[String]): Unit = {
    val parser = new OptionParser
    val dirOpt = parser.accepts("dir", "The directory to write to.")
                           .withRequiredArg
                           .describedAs("path")
                           .ofType(classOf[java.lang.String])
                           .defaultsTo(System.getProperty("java.io.tmpdir"))
    val bytesOpt = parser.accepts("bytes", "REQUIRED: The number of bytes to write.")
                           .withRequiredArg
                           .describedAs("num_bytes")
                           .ofType(classOf[java.lang.Long])
    val sizeOpt = parser.accepts("size", "REQUIRED: The size of each write.")
                           .withRequiredArg
                           .describedAs("num_bytes")
                           .ofType(classOf[java.lang.Integer])
    val filesOpt = parser.accepts("files", "REQUIRED: The number of files.")
                           .withRequiredArg
                           .describedAs("num_files")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(1)
   val reportingIntervalOpt = parser.accepts("reporting-interval", "The number of ms between updates.")
                           .withRequiredArg
                           .describedAs("ms")
                           .ofType(classOf[java.lang.Long])
                           .defaultsTo(1000)
   val maxThroughputOpt = parser.accepts("max-throughput-mb", "The maximum throughput.")
                           .withRequiredArg
                           .describedAs("mb")
                           .ofType(classOf[java.lang.Integer])
                           .defaultsTo(Integer.MAX_VALUE)
   val mmapOpt = parser.accepts("mmap", "Mmap file.")
                          
    val options = parser.parse(args : _*)
    
    for(arg <- List(bytesOpt, sizeOpt, filesOpt)) {
      if(!options.has(arg)) {
        System.err.println("Missing required argument \"" + arg + "\"") 
        parser.printHelpOn(System.err)
        System.exit(1)
      }
    }

    var bytesToWrite = options.valueOf(bytesOpt).longValue
    val mmap = options.has(mmapOpt)
    val bufferSize = options.valueOf(sizeOpt).intValue
    val numFiles = options.valueOf(filesOpt).intValue
    val reportingInterval = options.valueOf(reportingIntervalOpt).longValue
    val dir = options.valueOf(dirOpt)
    val maxThroughputBytes = options.valueOf(maxThroughputOpt).intValue * 1024L * 1024L
    val buffer = ByteBuffer.allocate(bufferSize)
    while(buffer.hasRemaining)
      buffer.put(123.asInstanceOf[Byte])
    
    val writables = new Array[Writable](numFiles)
    for(i <- 0 until numFiles) {
      val file = new File(dir, "kafka-test-" + i + ".dat")
      file.deleteOnExit()
      val raf = new RandomAccessFile(file, "rw")
      raf.setLength(bytesToWrite / numFiles)
      if(mmap)
        writables(i) = new MmapWritable(raf.getChannel().map(FileChannel.MapMode.READ_WRITE, 0, raf.length()))
      else
        writables(i) = new ChannelWritable(raf.getChannel())
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
      buffer.rewind()
      val start = System.nanoTime
      writables((count % numFiles).toInt.abs).write(buffer)
      val ellapsed = System.nanoTime - start
      maxLatency = max(ellapsed, maxLatency)
      totalLatency += ellapsed
      written += bufferSize
      count += 1
      totalWritten += bufferSize
      if((start - lastReport)/(1000.0*1000.0) > reportingInterval.doubleValue) {
        val ellapsedSecs = (start - lastReport) / (1000.0*1000.0*1000.0)
        val mb = written / (1024.0*1024.0)
        println("%10.3f\t%10.3f\t%10.3f".format(mb / ellapsedSecs, totalLatency / count.toDouble / (1000.0*1000.0), maxLatency / (1000.0 * 1000.0)))
        lastReport = start
        written = 0
        maxLatency = 0L
        totalLatency = 0L
      } else if(written > maxThroughputBytes) {
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
  }
  
  trait Writable {
    def write(buffer: ByteBuffer)
  }
  
  class MmapWritable(val buffer: ByteBuffer) extends Writable {
    def write(b: ByteBuffer) {
      buffer.put(b)
    }
  }
  
  class ChannelWritable(val channel: FileChannel) extends Writable {
    def write(b: ByteBuffer) {
      channel.write(b)
    }
  }
  
}
