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

import java.util.concurrent.atomic.AtomicLong

trait LogStatsMBean {
  def getName(): String
  def getSize(): Long
  def getNumberOfSegments: Int
  def getCurrentOffset: Long
  def getNumAppendedMessages: Long
}

class LogStats(val log: Log) extends LogStatsMBean {
  private val numCumulatedMessages = new AtomicLong(0)

  def getName(): String = log.name
  
  def getSize(): Long = log.size
  
  def getNumberOfSegments: Int = log.numberOfSegments
  
  def getCurrentOffset: Long = log.getHighwaterMark
  
  def getNumAppendedMessages: Long = numCumulatedMessages.get

  def recordAppendedMessages(nMessages: Int) = numCumulatedMessages.getAndAdd(nMessages)
}
