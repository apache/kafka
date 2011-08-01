/*
 * Copyright 2010 LinkedIn
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.network

import java.util.concurrent.atomic._
import javax.management._
import kafka.utils._
import kafka.api.RequestKeys

trait SocketServerStatsMBean {
  def getProduceRequestsPerSecond: Double
  def getFetchRequestsPerSecond: Double
  def getAvgProduceRequestMs: Double
  def getMaxProduceRequestMs: Double
  def getAvgFetchRequestMs: Double
  def getMaxFetchRequestMs: Double
  def getBytesReadPerSecond: Double
  def getBytesWrittenPerSecond: Double
  def getNumFetchRequests: Long
  def getNumProduceRequests: Long
}

@threadsafe
class SocketServerStats(val monitorDurationNs: Long, val time: Time) extends SocketServerStatsMBean {
  
  def this(monitorDurationNs: Long) = this(monitorDurationNs, SystemTime)
  val produceTimeStats = new SnapshotStats(monitorDurationNs)
  val fetchTimeStats = new SnapshotStats(monitorDurationNs)
  val produceBytesStats = new SnapshotStats(monitorDurationNs)
  val fetchBytesStats = new SnapshotStats(monitorDurationNs)

  def recordRequest(requestTypeId: Short, durationNs: Long) {
    requestTypeId match {
      case r if r == RequestKeys.Produce || r == RequestKeys.MultiProduce =>
        produceTimeStats.recordRequestMetric(durationNs)
      case r if r == RequestKeys.Fetch || r == RequestKeys.MultiFetch =>
        fetchTimeStats.recordRequestMetric(durationNs)
      case _ => /* not collecting; let go */
    }
  }
  
  def recordBytesWritten(bytes: Int): Unit = fetchBytesStats.recordRequestMetric(bytes)

  def recordBytesRead(bytes: Int): Unit = produceBytesStats.recordRequestMetric(bytes)

  def getProduceRequestsPerSecond: Double = produceTimeStats.getRequestsPerSecond
  
  def getFetchRequestsPerSecond: Double = fetchTimeStats.getRequestsPerSecond

  def getAvgProduceRequestMs: Double = produceTimeStats.getAvgMetric / (1000.0 * 1000.0)
  
  def getMaxProduceRequestMs: Double = produceTimeStats.getMaxMetric / (1000.0 * 1000.0)

  def getAvgFetchRequestMs: Double = fetchTimeStats.getAvgMetric / (1000.0 * 1000.0)

  def getMaxFetchRequestMs: Double = fetchTimeStats.getMaxMetric / (1000.0 * 1000.0)

  def getBytesReadPerSecond: Double = produceBytesStats.getAvgMetric
  
  def getBytesWrittenPerSecond: Double = fetchBytesStats.getAvgMetric

  def getNumFetchRequests: Long = fetchTimeStats.getNumRequests

  def getNumProduceRequests: Long = produceTimeStats.getNumRequests
}
