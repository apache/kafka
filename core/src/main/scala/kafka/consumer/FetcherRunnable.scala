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

package kafka.consumer

import java.io.IOException
import java.util.concurrent.CountDownLatch
import kafka.api.{FetchRequestBuilder, OffsetRequest}
import kafka.cluster.Broker
import kafka.common.ErrorMapping
import kafka.message.ByteBufferMessageSet
import kafka.utils._
import org.I0Itec.zkclient.ZkClient

class FetcherRunnable(val name: String,
                      val zkClient : ZkClient,
                      val config: ConsumerConfig,
                      val broker: Broker,
                      val partitionTopicInfos: List[PartitionTopicInfo])
  extends Thread(name) with Logging {
  private val shutdownLatch = new CountDownLatch(1)
  private val simpleConsumer = new SimpleConsumer(broker.host, broker.port, config.socketTimeoutMs,
    config.socketBufferSize)
  @volatile
  private var stopped = false

  def shutdown(): Unit = {
    stopped = true
    interrupt
    debug("awaiting shutdown on fetcher " + name)
    shutdownLatch.await
    debug("shutdown of fetcher " + name + " thread complete")
  }

  override def run() {
    for (infopti <- partitionTopicInfos)
      info(name + " start fetching topic: " + infopti.topic + " part: " + infopti.partitionId + " offset: "
        + infopti.getFetchOffset + " from " + broker.host + ":" + broker.port)

    var reqId = 0
    try {
      while (!stopped) {
        // TODO: fix up the max wait and min bytes
        val builder = new FetchRequestBuilder().
          correlationId(reqId).
          clientId(config.consumerId.getOrElse(name)).
          maxWait(0).
          minBytes(0)
        partitionTopicInfos.foreach(pti =>
          builder.addFetch(pti.topic, pti.partitionId, pti.getFetchOffset(), config.fetchSize)
        )

        val fetchRequest = builder.build()
        trace("fetch request: " + fetchRequest)
        val response = simpleConsumer.fetch(fetchRequest)

        var read = 0L
        for(infopti <- partitionTopicInfos) {
          val messages = response.messageSet(infopti.topic, infopti.partitionId).asInstanceOf[ByteBufferMessageSet]
          try {
            var done = false
            if(messages.getErrorCode == ErrorMapping.OffsetOutOfRangeCode) {
              info("offset for " + infopti + " out of range")
              // see if we can fix this error
              val resetOffset = resetConsumerOffsets(infopti.topic, infopti.partitionId)
              if(resetOffset >= 0) {
                infopti.resetFetchOffset(resetOffset)
                infopti.resetConsumeOffset(resetOffset)
                done = true
              }
            }
            if (!done)
              read += infopti.enqueue(messages, infopti.getFetchOffset)
          } catch {
            case e1: IOException =>
              // something is wrong with the socket, re-throw the exception to stop the fetcher
              throw e1
            case e2 =>
              if (!stopped) {
                // this is likely a repeatable error, log it and trigger an exception in the consumer
                error("error in FetcherRunnable for " + infopti, e2)
                infopti.enqueueError(e2, infopti.getFetchOffset)
              }
              // re-throw the exception to stop the fetcher
              throw e2
          }
        }
        reqId = if(reqId == Int.MaxValue) 0 else reqId + 1

        trace("fetched bytes: " + read)
        if(read == 0) {
          debug("backing off " + config.fetcherBackoffMs + " ms")
          Thread.sleep(config.fetcherBackoffMs)
        }
      }
    } catch {
      case e =>
        if (stopped)
          info("FecherRunnable " + this + " interrupted")
        else
          error("error in FetcherRunnable ", e)
    }

    info("stopping fetcher " + name + " to host " + broker.host)
    Utils.swallow(logger.info, simpleConsumer.close)
    shutdownComplete()
  }

  /**
   * Record that the thread shutdown is complete
   */
  private def shutdownComplete() = shutdownLatch.countDown

  private def resetConsumerOffsets(topic : String,
                                   partitionId: Int) : Long = {
    var offset : Long = 0
    config.autoOffsetReset match {
      case OffsetRequest.SmallestTimeString => offset = OffsetRequest.EarliestTime
      case OffsetRequest.LargestTimeString => offset = OffsetRequest.LatestTime
      case _ => return -1
    }

    // get mentioned offset from the broker
    val offsets = simpleConsumer.getOffsetsBefore(topic, partitionId, offset, 1)
    val topicDirs = new ZKGroupTopicDirs(config.groupId, topic)

    // reset manually in zookeeper
    info("updating partition " + partitionId + " for topic " + topic + " with " +
            (if(offset == OffsetRequest.EarliestTime) "earliest " else " latest ") + "offset " + offsets(0))
    ZkUtils.updatePersistentPath(zkClient, topicDirs.consumerOffsetDir + "/" + partitionId, offsets(0).toString)

    offsets(0)
  }
}
