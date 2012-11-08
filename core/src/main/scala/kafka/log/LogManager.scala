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
import kafka.server.KafkaConfig


/**
 * The entry point to the kafka log management subsystem. The log manager is responsible for log creation, retrieval, and cleaning.
 * All read and write operations are delegated to the individual log instances.
 * 
 * The log manager maintains logs in one or more directories. New logs are created in the data directory
 * with the fewest logs. No attempt is made to move partitions after the fact or balance based on
 * size or I/O rate.
 * 
 * A background thread handles log retention by periodically truncating excess log segments.
 */
@threadsafe
private[kafka] class LogManager(val config: KafkaConfig,
                                scheduler: KafkaScheduler,
                                private val time: Time) extends Logging {

  val CleanShutdownFile = ".kafka_cleanshutdown"
  val LockFile = ".lock"
  val logDirs: Array[File] = config.logDirs.map(new File(_)).toArray
  private val logFileSizeMap = config.logFileSizeMap
  private val logFlushInterval = config.flushInterval
  private val logFlushIntervals = config.flushIntervalMap
  private val logCreationLock = new Object
  private val logRetentionSizeMap = config.logRetentionSizeMap
  private val logRetentionMsMap = config.logRetentionHoursMap.map(e => (e._1, e._2 * 60 * 60 * 1000L)) // convert hours to ms
  private val logRollMsMap = config.logRollHoursMap.map(e => (e._1, e._2 * 60 * 60 * 1000L))
  private val logRollDefaultIntervalMs = 1000L * 60 * 60 * config.logRollHours
  private val logCleanupIntervalMs = 1000L * 60 * config.logCleanupIntervalMinutes
  private val logCleanupDefaultAgeMs = 1000L * 60 * 60 * config.logRetentionHours

  this.logIdent = "[Log Manager on Broker " + config.brokerId + "] "
  private val logs = new Pool[TopicAndPartition, Log]()
  
  createAndValidateLogDirs(logDirs)
  private var dirLocks = lockLogDirs(logDirs)
  loadLogs(logDirs)
  
  /**
   * 1. Ensure that there are no duplicates in the directory list
   * 2. Create each directory if it doesn't exist
   * 3. Check that each path is a readable directory 
   */
  private def createAndValidateLogDirs(dirs: Seq[File]) {
    if(dirs.map(_.getCanonicalPath).toSet.size < dirs.size)
      throw new KafkaException("Duplicate log directory found: " + logDirs.mkString(", "))
    for(dir <- dirs) {
      if(!dir.exists) {
        info("Log directory '" + dir.getAbsolutePath + "' not found, creating it.")
        val created = dir.mkdirs()
        if(!created)
          throw new KafkaException("Failed to create data directory " + dir.getAbsolutePath)
      }
      if(!dir.isDirectory || !dir.canRead)
        throw new KafkaException(dir.getAbsolutePath + " is not a readable log directory.")
    }
  }
  
  /**
   * Lock all the given directories
   */
  private def lockLogDirs(dirs: Seq[File]): Seq[FileLock] = {
    dirs.map { dir =>
      val lock = new FileLock(new File(dir, LockFile))
      if(!lock.tryLock())
        throw new KafkaException("Failed to acquire lock on file .lock in " + lock.file.getParentFile.getAbsolutePath + 
                               ". A Kafka instance in another process or thread is using this directory.")
      lock
    }
  }
  
  /**
   * Recovery and load all logs in the given data directories
   */
  private def loadLogs(dirs: Seq[File]) {
    for(dir <- dirs) {
      /* check if this set of logs was shut down cleanly */
      val cleanShutDownFile = new File(dir, CleanShutdownFile)
      val needsRecovery = cleanShutDownFile.exists
      cleanShutDownFile.delete
      /* load the logs */
      val subDirs = dir.listFiles()
      if(subDirs != null) {
        for(dir <- subDirs) {
          if(dir.isDirectory){
            info("Loading log '" + dir.getName + "'")
            val topicPartition = parseTopicPartitionName(dir.getName)
            val rollIntervalMs = logRollMsMap.get(topicPartition.topic).getOrElse(this.logRollDefaultIntervalMs)
            val maxLogFileSize = logFileSizeMap.get(topicPartition.topic).getOrElse(config.logFileSize)
            val log = new Log(dir, 
                              maxLogFileSize, 
                              config.maxMessageSize, 
                              logFlushInterval, 
                              rollIntervalMs, 
                              needsRecovery, 
                              config.logIndexMaxSizeBytes,
                              config.logIndexIntervalBytes,
                              time, 
                              config.brokerId)
            val previous = this.logs.put(topicPartition, log)
            if(previous != null)
              throw new IllegalArgumentException("Duplicate log directories found: %s, %s!".format(log.dir.getAbsolutePath, previous.dir.getAbsolutePath))
          }
        }
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
   * Get the log if it exists
   */
  def getLog(topic: String, partition: Int): Option[Log] = {
    val topicAndPartiton = TopicAndPartition(topic, partition)
    val log = logs.get(topicAndPartiton)
    if (log == null)
      None
    else
      Some(log)
  }

  /**
   * Create the log if it does not exist, if it exists just return it
   */
  def getOrCreateLog(topic: String, partition: Int): Log = {
    val topicAndPartition = TopicAndPartition(topic, partition)
    logs.get(topicAndPartition) match {
      case null => createLogIfNotExists(topicAndPartition)
      case log: Log => log
    }
  }

  /**
   * Create a log for the given topic and the given partition
   * If the log already exists, just return a copy of the existing log
   */
  private def createLogIfNotExists(topicAndPartition: TopicAndPartition): Log = {
    logCreationLock synchronized {
      var log = logs.get(topicAndPartition)
      
      // check if the log has already been created in another thread
      if(log != null)
        return log
      
      // if not, create it
      val dataDir = nextLogDir()
      val dir = new File(dataDir, topicAndPartition.topic + "-" + topicAndPartition.partition)
      dir.mkdirs()
      val rollIntervalMs = logRollMsMap.get(topicAndPartition.topic).getOrElse(this.logRollDefaultIntervalMs)
      val maxLogFileSize = logFileSizeMap.get(topicAndPartition.topic).getOrElse(config.logFileSize)
      log = new Log(dir, 
                    maxLogFileSize, 
                    config.maxMessageSize, 
                    logFlushInterval, 
                    rollIntervalMs, 
                    needsRecovery = false, 
                    config.logIndexMaxSizeBytes, 
                    config.logIndexIntervalBytes, 
                    time, 
                    config.brokerId)
      info("Created log for topic %s partition %d in %s.".format(topicAndPartition.topic, topicAndPartition.partition, dataDir.getAbsolutePath))
      logs.put(topicAndPartition, log)
      log
    }
  }
  
  /**
   * Choose the next directory in which to create a log. Currently this is done
   * by calculating the number of partitions in each directory and then choosing the
   * data directory with the fewest partitions.
   */
  private def nextLogDir(): File = {
    if(logDirs.size == 1) {
      logDirs(0)
    } else {
      // count the number of logs in each parent directory (including 0 for empty directories
      val logCounts = allLogs.groupBy(_.dir.getParent).mapValues(_.size)
      val zeros = logDirs.map(dir => (dir.getPath, 0)).toMap
      var dirCounts = (zeros ++ logCounts).toBuffer
    
      // choose the directory with the least logs in it
      val leastLoaded = dirCounts.sortBy(_._2).head
      new File(leastLoaded._1)
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
   * Runs through the log removing segments older than a certain age
   */
  private def cleanupExpiredSegments(log: Log): Int = {
    val startMs = time.milliseconds
    val topic = parseTopicPartitionName(log.name).topic
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
    val topic = parseTopicPartitionName(log.dir.getName).topic
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
    try {
      // close the logs
      allLogs.foreach(_.close())
      // mark that the shutdown was clean by creating the clean shutdown marker file
      logDirs.foreach(dir => Utils.swallow(new File(dir, CleanShutdownFile).createNewFile()))
    } finally {
      // regardless of whether the close succeeded, we need to unlock the data directories
      dirLocks.foreach(_.destroy())
    }
    debug("Shutdown complete.")
  }

  /**
   * Get all the partition logs
   */
  def allLogs(): Iterable[Log] = logs.values

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

  private def parseTopicPartitionName(name: String): TopicAndPartition = {
    val index = name.lastIndexOf('-')
    TopicAndPartition(name.substring(0,index), name.substring(index+1).toInt)
  }

  def topics(): Iterable[String] = logs.keys.map(_.topic)

}
