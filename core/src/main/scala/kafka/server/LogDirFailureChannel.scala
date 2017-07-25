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


package kafka.server

import java.util.concurrent.{ArrayBlockingQueue, ConcurrentHashMap}

/*
 * LogDirFailureChannel allows an external thread to block waiting for new offline log dir.
 *
 * LogDirFailureChannel should be a singleton object which can be accessed by any class that does disk-IO operation.
 * If IOException is encountered while accessing a log directory, the corresponding class can insert the the log directory name
 * to the LogDirFailureChannel using maybeAddLogFailureEvent(). Then a thread which is blocked waiting for new offline log directories
 * can take the name of the new offline log directory out of the LogDirFailureChannel and handles the log failure properly.
 *
 */
class LogDirFailureChannel(logDirNum: Int) {

  private val offlineLogDirs = new ConcurrentHashMap[String, String]
  private val logDirFailureEvent = new ArrayBlockingQueue[String](logDirNum)

  /*
   * If the given logDir is not already offline, add it to the
   * set of offline log dirs and enqueue it to the logDirFailureEvent queue
   */
  def maybeAddLogFailureEvent(logDir: String): Unit = {
    if (offlineLogDirs.putIfAbsent(logDir, logDir) == null) {
      logDirFailureEvent.add(logDir)
    }
  }

  /*
   * Get the next offline log dir from logDirFailureEvent queue.
   * The method will wait if necessary until a new offline log directory becomes available
   */
  def takeNextLogFailureEvent(): String = {
    logDirFailureEvent.take()
  }

}
