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
import kafka.server.{BrokerTopicStats, LogDirFailureChannel}
import kafka.utils._
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException
import org.apache.kafka.common.record.FileRecords
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.utils.Utils

/**
 * A stress test that instantiates a log and then runs continual appends against it from one thread and continual reads against it
 * from another thread and checks a few basic assertions until the user kills the process.
 */
object StressTestLog {
  val running = new AtomicBoolean(true)

  def main(args: Array[String]) {
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
      logDirFailureChannel = new LogDirFailureChannel(10))
    val writer = new WriterThread(log)
    writer.start()
    val reader = new ReaderThread(log)
    reader.start()

    Runtime.getRuntime().addShutdownHook(new Thread() {
      override def run() = {
        running.set(false)
        writer.join()
        reader.join()
        Utils.delete(dir)
      }
    })

    while(running.get) {
      println("Reader offset = %d, writer offset = %d".format(reader.offset, writer.offset))
      Thread.sleep(1000)
    }
  }

  abstract class WorkerThread extends Thread {
    override def run() {
      try {
        while(running.get)
          work()
      } catch {
        case e: Exception =>
          e.printStackTrace()
          running.set(false)
      }
      println(getClass.getName + " exiting...")
    }
    def work()
  }

  class WriterThread(val log: Log) extends WorkerThread {
    @volatile var offset = 0
    override def work() {
      val logAppendInfo = log.appendAsFollower(TestUtils.singletonRecords(offset.toString.getBytes))
      require(logAppendInfo.firstOffset == offset && logAppendInfo.lastOffset == offset)
      offset += 1
      if(offset % 1000 == 0)
        Thread.sleep(500)
    }
  }

  class ReaderThread(val log: Log) extends WorkerThread {
    @volatile var offset = 0
    override def work() {
      try {
        log.read(offset, 1024, Some(offset+1), isolationLevel = IsolationLevel.READ_UNCOMMITTED).records match {
          case read: FileRecords if read.sizeInBytes > 0 => {
            val first = read.batches.iterator.next()
            require(first.lastOffset == offset, "We should either read nothing or the message we asked for.")
            require(first.sizeInBytes == read.sizeInBytes, "Expected %d but got %d.".format(first.sizeInBytes, read.sizeInBytes))
            offset += 1
          }
          case _ =>
        }
      } catch {
        case _: OffsetOutOfRangeException => // this is okay
      }
    }
  }
}
