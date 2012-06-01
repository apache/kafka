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

package kafka.log

import java.io._
import kafka.utils._
import scala.collection._
import java.util.concurrent.CountDownLatch
import kafka.server.KafkaConfig
import kafka.common.{InvalidTopicException, InvalidPartitionException}
import kafka.api.OffsetRequest
import kafka.log.Log._

/**
 * The guy who creates and hands out logs
 */
@threadsafe
private[kafka] class LogManager(val config: KafkaConfig,
                                private val time: Time,
                                val logCleanupIntervalMs: Long,
                                val logCleanupDefaultAgeMs: Long,
                                needRecovery: Boolean) extends Logging {
  
  val logDir: File = new File(config.logDir)
  private val numPartitions = config.numPartitions
  private val maxSize: Long = config.logFileSize
  private val flushInterval = config.flushInterval
  private val topicPartitionsMap = config.topicPartitionsMap
  private val logCreationLock = new Object
  private val random = new java.util.Random
  private val startupLatch: CountDownLatch = new CountDownLatch(1)
  private val logFlusherScheduler = new KafkaScheduler(1, "kafka-logflusher-", false)
  private val logFlushIntervalMap = config.flushIntervalMap
  private val logRetentionMSMap = getLogRetentionMSMap(config.logRetentionHoursMap)
  private val logRetentionSize = config.logRetentionSize
  private val scheduler = new KafkaScheduler(1, "kafka-logcleaner-", false)

  /* Initialize a log for each subdirectory of the main log directory */
  private val logs = new Pool[String, Pool[Int, Log]]()
  if(!logDir.exists()) {
    info("No log directory found, creating '" + logDir.getAbsolutePath() + "'")
    logDir.mkdirs()
  }
  if(!logDir.isDirectory() || !logDir.canRead())
    throw new IllegalArgumentException(logDir.getAbsolutePath() + " is not a readable log directory.")
  val subDirs = logDir.listFiles()
  if(subDirs != null) {
    for(dir <- subDirs) {
      if(!dir.isDirectory()) {
        warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "'--should it be there?")
      } else {
        info("Loading log '" + dir.getName() + "'")
        val log = new Log(dir, maxSize, flushInterval, needRecovery)
        val topicPartion = Utils.getTopicPartition(dir.getName)
        logs.putIfNotExists(topicPartion._1, new Pool[Int, Log]())
        val parts = logs.get(topicPartion._1)
        parts.put(topicPartion._2, log)
      }
    }
  }
  
  private def getLogRetentionMSMap(logRetentionHourMap: Map[String, Int]) : Map[String, Long] = {
    var ret = new mutable.HashMap[String, Long]
    for ( (topic, hour) <- logRetentionHourMap )
      ret.put(topic, hour * 60 * 60 * 1000L)
    ret
  }

  /**
   *  Register this broker in ZK for the first time.
   */
  def startup() {

    /* Schedule the cleanup task to delete old logs */
    if(scheduler != null) {
      if(scheduler.hasShutdown) {
        println("Restarting log cleaner scheduler")
        scheduler.startUp
      }
      info("starting log cleaner every " + logCleanupIntervalMs + " ms")
      scheduler.scheduleWithRate(cleanupLogs, 60 * 1000, logCleanupIntervalMs)
    }

    if(logFlusherScheduler.hasShutdown) logFlusherScheduler.startUp
    info("Starting log flusher every " + config.flushSchedulerThreadRate + " ms with the following overrides " + logFlushIntervalMap)
    logFlusherScheduler.scheduleWithRate(flushAllLogs, config.flushSchedulerThreadRate, config.flushSchedulerThreadRate)
    startupLatch.countDown
  }

  private def awaitStartup() {
    startupLatch.await
  }


  /**
   * Create a log for the given topic and the given partition
   */
  private def createLog(topic: String, partition: Int): Log = {
    logCreationLock synchronized {
      val d = new File(logDir, topic + "-" + partition)
      d.mkdirs()
      new Log(d, maxSize, flushInterval, false)
    }
  }

  /**
   * Return the Pool (partitions) for a specific log
   */
  private def getLogPool(topic: String, partition: Int): Pool[Int, Log] = {
    awaitStartup
    if (topic.length <= 0)
      throw new InvalidTopicException("topic name can't be empty")
    if (partition < 0 || partition >= topicPartitionsMap.getOrElse(topic, numPartitions)) {
      val error = "Wrong partition %d, valid partitions (0, %d)."
        .format(partition, (topicPartitionsMap.getOrElse(topic, numPartitions) - 1))
      warn(error)
      throw new InvalidPartitionException(error)
    }
    logs.get(topic)
  }

  /**
   * Pick a random partition from the given topic
   */
  def chooseRandomPartition(topic: String): Int = {
    random.nextInt(topicPartitionsMap.getOrElse(topic, numPartitions))
  }

  def getOffsets(offsetRequest: OffsetRequest): Array[Long] = {
    val log = getLog(offsetRequest.topic, offsetRequest.partition)
    log match {
      case Some(l) => l.getOffsetsBefore(offsetRequest)
      case None => getEmptyOffsets(offsetRequest)
    }
  }

  /**
   * Get the log if exists
   */
  def getLog(topic: String, partition: Int): Option[Log] = {
    val parts = getLogPool(topic, partition)
    if (parts == null) None
    else {
      val log = parts.get(partition)
      if(log == null) None
      else Some(log)
    }
  }

  /**
   * Create the log if it does not exist, if it exists just return it
   */
  def getOrCreateLog(topic: String, partition: Int): Log = {
    var hasNewTopic = false
    var parts = getLogPool(topic, partition)
    if (parts == null) {
      val found = logs.putIfNotExists(topic, new Pool[Int, Log])
      if (found == null)
        hasNewTopic = true
      parts = logs.get(topic)
    }
    var log = parts.get(partition)
    if(log == null) {
      // check if this broker hosts this partition
      log = createLog(topic, partition)
      val found = parts.putIfNotExists(partition, log)
      if(found != null) {
        // there was already somebody there
        log.close()
        log = found
      }
      else
        info("Created log for '" + topic + "'-" + partition)
    }

    log
  }

  /* Runs through the log removing segments older than a certain age */
  private def cleanupExpiredSegments(log: Log): Int = {
    val startMs = time.milliseconds
    val topic = Utils.getTopicPartition(log.name)._1
    val logCleanupThresholdMS = logRetentionMSMap.get(topic).getOrElse(this.logCleanupDefaultAgeMs)
    val toBeDeleted = log.markDeletedWhile(startMs - _.file.lastModified > logCleanupThresholdMS)
    val total = log.deleteSegments(toBeDeleted)
    total
  }

  /**
   *  Runs through the log removing segments until the size of the log
   *  is at least logRetentionSize bytes in size
   */
  private def cleanupSegmentsToMaintainSize(log: Log): Int = {
    if(logRetentionSize < 0 || log.size < logRetentionSize) return 0
    var diff = log.size - logRetentionSize
    def shouldDelete(segment: LogSegment) = {
      if(diff - segment.size >= 0) {
        diff -= segment.size
        true
      } else {
        false
      }
    }
    val toBeDeleted = log.markDeletedWhile( shouldDelete )
    val total = log.deleteSegments(toBeDeleted)
    total
  }

  /**
   * Delete any eligible logs. Return the number of segments deleted.
   */
  def cleanupLogs() {
    debug("Beginning log cleanup...")
    val iter = getLogIterator
    var total = 0
    val startMs = time.milliseconds
    while(iter.hasNext) {
      val log = iter.next
      debug("Garbage collecting '" + log.name + "'")
      total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log)
    }
    debug("Log cleanup completed. " + total + " files deleted in " + 
                 (time.milliseconds - startMs) / 1000 + " seconds")
  }
  
  /**
   * Close all the logs
   */
  def close() {
    info("Closing log manager")
    scheduler.shutdown()
    logFlusherScheduler.shutdown()
    val iter = getLogIterator
    while(iter.hasNext)
      iter.next.close()
  }
  
  private def getLogIterator(): Iterator[Log] = {
    new IteratorTemplate[Log] {
      val partsIter = logs.values.iterator
      var logIter: Iterator[Log] = null

      override def makeNext(): Log = {
        while (true) {
          if (logIter != null && logIter.hasNext)
            return logIter.next
          if (!partsIter.hasNext)
            return allDone
          logIter = partsIter.next.values.iterator
        }
        // should never reach here
        assert(false)
        return allDone
      }
    }
  }

  private def flushAllLogs() = {
    debug("flushing the high watermark of all logs")

    for (log <- getLogIterator)
    {
      try{
        val timeSinceLastFlush = System.currentTimeMillis - log.getLastFlushedTime
        var logFlushInterval = config.defaultFlushIntervalMs
        if(logFlushIntervalMap.contains(log.topicName))
          logFlushInterval = logFlushIntervalMap(log.topicName)
        debug(log.topicName + " flush interval  " + logFlushInterval +
            " last flushed " + log.getLastFlushedTime + " timesincelastFlush: " + timeSinceLastFlush)
        if(timeSinceLastFlush >= logFlushInterval)
          log.flush
      }
      catch {
        case e =>
          error("Error flushing topic " + log.topicName, e)
          e match {
            case _: IOException =>
              fatal("Halting due to unrecoverable I/O error while flushing logs: " + e.getMessage, e)
              System.exit(1)
            case _ =>
          }
      }
    }
  }


  def getAllTopics(): Iterator[String] = logs.keys.iterator
  def getTopicPartitionsMap() = topicPartitionsMap

  def getServerConfig: KafkaConfig = config
}
