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
import kafka.server.KafkaConfig
import kafka.api.OffsetRequest
import kafka.log.Log._
import kafka.common.{KafkaException, InvalidTopicException, InvalidPartitionException}

/**
 * The guy who creates and hands out logs
 */
@threadsafe
private[kafka] class LogManager(val config: KafkaConfig,
                                scheduler: KafkaScheduler,
                                private val time: Time,
                                val logCleanupIntervalMs: Long,
                                val logCleanupDefaultAgeMs: Long,
                                needRecovery: Boolean) extends Logging {

  val logDir: File = new File(config.logDir)
  private val numPartitions = config.numPartitions
  private val maxSize: Long = config.logFileSize
  private val flushInterval = config.flushInterval
  private val logCreationLock = new Object
  private val logFlushIntervals = config.flushIntervalMap
  private val logRetentionMs = config.logRetentionHoursMap.map(e => (e._1, e._2 * 60 * 60 * 1000L)) // convert hours to ms
  private val logRetentionSize = config.logRetentionSize
  this.logIdent = "Log Manager on Broker " + config.brokerId + ", "

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
      if(!dir.isDirectory()) {
        warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "'--should it be there?")
      } else {
        info("Loading log '" + dir.getName() + "'")
        val log = new Log(dir, maxSize, flushInterval, needRecovery, time, config.brokerId)
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
      scheduler.scheduleWithRate(flushAllLogs, "kafka-logflusher-",
                                 config.flushSchedulerThreadRate, config.flushSchedulerThreadRate, false)
    }
  }


  /**
   * Create a log for the given topic and the given partition
   */
  private def createLog(topic: String, partition: Int): Log = {
    if (topic.length <= 0)
      throw new InvalidTopicException("Topic name can't be emtpy")
    if (partition < 0 || partition >= config.topicPartitionsMap.getOrElse(topic, numPartitions)) {
      val error = "Wrong partition %d, valid partitions (0, %d)."
              .format(partition, (config.topicPartitionsMap.getOrElse(topic, numPartitions) - 1))
      warn(error)
      throw new InvalidPartitionException(error)
    }
    logCreationLock synchronized {
      val d = new File(logDir, topic + "-" + partition)
      d.mkdirs()
      new Log(d, maxSize, flushInterval, false, time, config.brokerId)
    }
  }

  def getOffsets(offsetRequest: OffsetRequest): Array[Long] = {
    val log = getLog(offsetRequest.topic, offsetRequest.partition)
    log match {
      case Some(l) => l.getOffsetsBefore(offsetRequest)
      case None => getEmptyOffsets(offsetRequest)
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
    val logCleanupThresholdMs = logRetentionMs.get(topic).getOrElse(this.logCleanupDefaultAgeMs)
    val toBeDeleted = log.markDeletedWhile(startMs - _.file.lastModified > logCleanupThresholdMs)
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
    info("shut down")
    allLogs.foreach(_.close())
    info("shutted down completedly")
  }

  /**
   * Get all the partition logs
   */
  def allLogs() = logs.values.flatMap(_.values)

  private def flushAllLogs() = {
    debug("Flushing the high watermark of all logs")
    for (log <- allLogs)
    {
      try{
        val timeSinceLastFlush = System.currentTimeMillis - log.getLastFlushedTime
        var logFlushInterval = config.defaultFlushIntervalMs
        if(logFlushIntervals.contains(log.topicName))
          logFlushInterval = logFlushIntervals(log.topicName)
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


  def topics(): Iterable[String] = logs.keys

}
