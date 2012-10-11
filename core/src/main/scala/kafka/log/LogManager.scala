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
import kafka.log.Log._
import kafka.common.{TopicAndPartition, KafkaException}
import kafka.server.{HighwaterMarkCheckpoint, KafkaConfig}


/**
 * The guy who creates and hands out logs
 */
@threadsafe
private[kafka] class LogManager(val config: KafkaConfig,
                                scheduler: KafkaScheduler,
                                private val time: Time,
                                val logRollDefaultIntervalMs: Long,
                                val logCleanupIntervalMs: Long,
                                val logCleanupDefaultAgeMs: Long,
                                needRecovery: Boolean) extends Logging {

  val logDir: File = new File(config.logDir)
  private val logFileSizeMap = config.logFileSizeMap
  private val flushInterval = config.flushInterval
  private val logCreationLock = new Object
  private val logFlushIntervals = config.flushIntervalMap
  private val logRetentionSizeMap = config.logRetentionSizeMap
  private val logRetentionMsMap = config.logRetentionHoursMap.map(e => (e._1, e._2 * 60 * 60 * 1000L)) // convert hours to ms
  private val logRollMsMap = config.logRollHoursMap.map(e => (e._1, e._2 * 60 * 60 * 1000L))
  this.logIdent = "[Log Manager on Broker " + config.brokerId + "] "

  /* Initialize a log for each subdirectory of the main log directory */
  private val logs = new Pool[String, Pool[Int, Log]]()
  if(!logDir.exists()) {
    info("No log directory found, creating '" + logDir.getAbsolutePath() + "'")
    logDir.mkdirs()
  }
  if(!logDir.isDirectory() || !logDir.canRead())
    throw new KafkaException(logDir.getAbsolutePath() + " is not a readable log directory.")
  val subDirs = logDir.listFiles()
  if(subDirs != null) {
    for(dir <- subDirs) {
      if(dir.getName.equals(HighwaterMarkCheckpoint.highWatermarkFileName)){
        // skip valid metadata file
      }
      else if(!dir.isDirectory()) {
        warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "'--should it be there?")
      } else {
        info("Loading log '" + dir.getName() + "'")
        val topic = Utils.getTopicPartition(dir.getName)._1
        val rollIntervalMs = logRollMsMap.get(topic).getOrElse(this.logRollDefaultIntervalMs)
        val maxLogFileSize = logFileSizeMap.get(topic).getOrElse(config.logFileSize)
        val log = new Log(dir, 
                          maxLogFileSize, 
                          config.maxMessageSize, 
                          flushInterval, 
                          rollIntervalMs, 
                          needRecovery, 
                          config.logIndexMaxSizeBytes,
                          config.logIndexIntervalBytes,
                          time, 
                          config.brokerId)
        val topicPartition = Utils.getTopicPartition(dir.getName)
        logs.putIfNotExists(topicPartition._1, new Pool[Int, Log]())
        val parts = logs.get(topicPartition._1)
        parts.put(topicPartition._2, log)
      }
    }
  }

  /**
   *  Start the log flush thread
   */
  def startup() {
    /* Schedule the cleanup task to delete old logs */
    if(scheduler != null) {
      info("Starting log cleaner every " + logCleanupIntervalMs + " ms")
      scheduler.scheduleWithRate(cleanupLogs, "kafka-logcleaner-", 60 * 1000, logCleanupIntervalMs, false)
      info("Starting log flusher every " + config.flushSchedulerThreadRate +
                   " ms with the following overrides " + logFlushIntervals)
      scheduler.scheduleWithRate(flushDirtyLogs, "kafka-logflusher-",
                                 config.flushSchedulerThreadRate, config.flushSchedulerThreadRate, false)
    }
  }


  /**
   * Create a log for the given topic and the given partition
   */
  private def createLog(topic: String, partition: Int): Log = {
    logCreationLock synchronized {
      val d = new File(logDir, topic + "-" + partition)
      d.mkdirs()
      val rollIntervalMs = logRollMsMap.get(topic).getOrElse(this.logRollDefaultIntervalMs)
      val maxLogFileSize = logFileSizeMap.get(topic).getOrElse(config.logFileSize)
      new Log(d, maxLogFileSize, config.maxMessageSize, flushInterval, rollIntervalMs, needsRecovery = false, config.logIndexMaxSizeBytes, config.logIndexIntervalBytes, time, config.brokerId)
    }
  }

  def getOffsets(topicAndPartition: TopicAndPartition, timestamp: Long, maxNumOffsets: Int): Seq[Long] = {
    val log = getLog(topicAndPartition.topic, topicAndPartition.partition)
    log match {
      case Some(l) => l.getOffsetsBefore(timestamp, maxNumOffsets)
      case None => getEmptyOffsets(timestamp)
    }
  }

  /**
   * Get the log if it exists
   */
  def getLog(topic: String, partition: Int): Option[Log] = {
    val parts = logs.get(topic)
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
    var parts = logs.get(topic)
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
    val logCleanupThresholdMs = logRetentionMsMap.get(topic).getOrElse(this.logCleanupDefaultAgeMs)
    val toBeDeleted = log.markDeletedWhile(startMs - _.messageSet.file.lastModified > logCleanupThresholdMs)
    val total = log.deleteSegments(toBeDeleted)
    total
  }

  /**
   *  Runs through the log removing segments until the size of the log
   *  is at least logRetentionSize bytes in size
   */
  private def cleanupSegmentsToMaintainSize(log: Log): Int = {
    val topic = Utils.getTopicPartition(log.dir.getName)._1
    val maxLogRetentionSize = logRetentionSizeMap.get(topic).getOrElse(config.logRetentionSize)
    if(maxLogRetentionSize < 0 || log.size < maxLogRetentionSize) return 0
    var diff = log.size - maxLogRetentionSize
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
    var total = 0
    val startMs = time.milliseconds
    for(log <- allLogs) {
      debug("Garbage collecting '" + log.name + "'")
      total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log)
    }
    debug("Log cleanup completed. " + total + " files deleted in " +
                  (time.milliseconds - startMs) / 1000 + " seconds")
  }

  /**
   * Close all the logs
   */
  def shutdown() {
    debug("Shutting down.")
    allLogs.foreach(_.close())
    debug("Shutdown complete.")
  }

  /**
   * Get all the partition logs
   */
  def allLogs() = logs.values.flatMap(_.values)

  /**
   * Flush any log which has exceeded its flush interval and has unwritten messages.
   */
  private def flushDirtyLogs() = {
    debug("Checking for dirty logs to flush...")
    for (log <- allLogs) {
      try {
        val timeSinceLastFlush = System.currentTimeMillis - log.getLastFlushedTime
        var logFlushInterval = config.defaultFlushIntervalMs
        if(logFlushIntervals.contains(log.topicName))
          logFlushInterval = logFlushIntervals(log.topicName)
        debug(log.topicName + " flush interval  " + logFlushInterval +
                      " last flushed " + log.getLastFlushedTime + " time since last flush: " + timeSinceLastFlush)
        if(timeSinceLastFlush >= logFlushInterval)
          log.flush
      } catch {
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


  def topics(): Iterable[String] = logs.keys

}
