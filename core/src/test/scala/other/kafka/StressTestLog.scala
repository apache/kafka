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

import java.util.Properties
import java.util.concurrent.atomic._

import kafka.log._
import kafka.server.{BrokerTopicStats, FetchLogEnd, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.utils.Utils

/**
 * A stress test that instantiates a log and then runs continual appends against it from one thread and continual reads against it
 * from another thread and checks a few basic assertions until the user kills the process.
 */
object StressTestLog {
  val running = new AtomicBoolean(true)

  def main(args: Array[String]): Unit = {
    val dir = TestUtils.randomPartitionLogDir(TestUtils.tempDir())
    val time = new MockTime
    val logProperties = new Properties()
    logProperties.put(LogConfig.SegmentBytesProp, 64*1024*1024: java.lang.Integer)
    logProperties.put(LogConfig.MaxMessageBytesProp, Int.MaxValue: java.lang.Integer)
    logProperties.put(LogConfig.SegmentIndexBytesProp, 1024*1024: java.lang.Integer)

    val log = Log(dir = dir,
      config = LogConfig(logProperties),
      logStartOffset = 0L,
      recoveryPoint = 0L,
      scheduler = time.scheduler,
      time = time,
      maxProducerIdExpirationMs = 60 * 60 * 1000,
      producerIdExpirationCheckIntervalMs = LogManager.ProducerIdExpirationCheckIntervalMs,
      brokerTopicStats = new BrokerTopicStats,
      logDirFailureChannel = new LogDirFailureChannel(10),
      topicId = None,
      keepPartitionMetadataFile = true)
    val writer = new WriterThread(log)
    writer.start()
    val reader = new ReaderThread(log)
    reader.start()

    Exit.addShutdownHook("stress-test-shutdown-hook", {
        running.set(false)
        writer.join()
        reader.join()
        Utils.delete(dir)
    })

    while(running.get) {
      Thread.sleep(1000)
      println("Reader offset = %d, writer offset = %d".format(reader.currentOffset, writer.currentOffset))
      writer.checkProgress()
      reader.checkProgress()
    }
  }

  abstract class WorkerThread extends Thread {
    val threadInfo = "Thread: " + Thread.currentThread.getName + " Class: " + getClass.getName

    override def run(): Unit = {
      try {
        while(running.get)
          work()
      } catch {
        case e: Exception => {
          e.printStackTrace()
        }
      } finally {
        running.set(false)
      }
    }

    def work(): Unit
    def isMakingProgress(): Boolean
  }

  trait LogProgress {
    @volatile var currentOffset = 0
    private var lastOffsetCheckpointed = currentOffset
    private var lastProgressCheckTime = System.currentTimeMillis

    def isMakingProgress(): Boolean = {
      if (currentOffset > lastOffsetCheckpointed) {
        lastOffsetCheckpointed = currentOffset
        return true
      }

      false
    }

    def checkProgress(): Unit = {
      // Check if we are making progress every 500ms
      val curTime = System.currentTimeMillis
      if ((curTime - lastProgressCheckTime) > 500) {
        require(isMakingProgress(), "Thread not making progress")
        lastProgressCheckTime = curTime
      }
    }
  }

  class WriterThread(val log: Log) extends WorkerThread with LogProgress {
    override def work(): Unit = {
      val logAppendInfo = log.appendAsLeader(TestUtils.singletonRecords(currentOffset.toString.getBytes), 0)
      require(logAppendInfo.firstOffset.forall(_.messageOffset == currentOffset) && logAppendInfo.lastOffset == currentOffset)
      currentOffset += 1
      if (currentOffset % 1000 == 0)
        Thread.sleep(50)
    }
  }

  class ReaderThread(val log: Log) extends WorkerThread with LogProgress {
    override def work(): Unit = {
      try {
        log.read(currentOffset,
          maxLength = 1,
          isolation = FetchLogEnd,
          minOneMessage = true).records match {
          case read: FileRecords if read.sizeInBytes > 0 => {
            val first = read.batches.iterator.next()
            require(first.lastOffset == currentOffset, "We should either read nothing or the message we asked for.")
            require(first.sizeInBytes == read.sizeInBytes, "Expected %d but got %d.".format(first.sizeInBytes, read.sizeInBytes))
            currentOffset += 1
          }
          case _ =>
        }
      } catch {
        case _: OffsetOutOfRangeException => // this is okay
      }
    }
  }
}
