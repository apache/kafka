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

import kafka.network.RequestChannel
import kafka.utils.Logging
import kafka.server.KafkaRequestHandler.{threadCurrentRequest, threadRequestChannel}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicInteger
import com.yammer.metrics.core.Meter
import org.apache.kafka.common.internals.FatalExitError
import org.apache.kafka.common.utils.{Exit, KafkaThread, Time}
import org.apache.kafka.server.common.RequestLocal
import org.apache.kafka.server.metrics.KafkaMetricsGroup

import scala.collection.mutable

trait ApiRequestHandler {
  def handle(request: RequestChannel.Request, requestLocal: RequestLocal): Unit
  def tryCompleteActions(): Unit = {}
}

object KafkaRequestHandler {
  // Support for scheduling callbacks on a request thread.
  private val threadRequestChannel = new ThreadLocal[RequestChannel]
  private val threadCurrentRequest = new ThreadLocal[RequestChannel.Request]

  // For testing
  @volatile private var bypassThreadCheck = false
  def setBypassThreadCheck(bypassCheck: Boolean): Unit = {
    bypassThreadCheck = bypassCheck
  }

  /**
   * Creates a wrapped callback to be executed synchronously on the calling request thread or asynchronously
   * on an arbitrary request thread.
   * NOTE: this function must be originally called from a request thread.
   * @param asyncCompletionCallback A callback method that we intend to call from the current thread or in another
   *                                thread after an asynchronous action completes. The RequestLocal passed in must
   *                                belong to the request handler thread that is executing the callback.
   * @param requestLocal The RequestLocal for the current request handler thread in case we need to execute the callback
   *                     function synchronously from the calling thread.
   * @return Wrapped callback will either immediately execute `asyncCompletionCallback` or schedule it on an arbitrary request thread
   *         depending on where it is called
   */
  def wrapAsyncCallback[T](asyncCompletionCallback: (RequestLocal, T) => Unit, requestLocal: RequestLocal): T => Unit = {
    val requestChannel = threadRequestChannel.get()
    val currentRequest = threadCurrentRequest.get()
    if (requestChannel == null || currentRequest == null) {
      if (!bypassThreadCheck)
        throw new IllegalStateException("Attempted to reschedule to request handler thread from non-request handler thread.")
      t => asyncCompletionCallback(requestLocal, t)
    } else {
      t => {
        if (threadCurrentRequest.get() == currentRequest) {
          // If the callback is actually executed on the same request thread, we can directly execute
          // it without re-scheduling it.
          asyncCompletionCallback(requestLocal, t)
        } else {
          // The requestChannel and request are captured in this lambda, so when it's executed on the callback thread
          // we can re-schedule the original callback on a request thread and update the metrics accordingly.
          requestChannel.sendCallbackRequest(RequestChannel.CallbackRequest(newRequestLocal => asyncCompletionCallback(newRequestLocal, t), currentRequest))
        }
      }
    }
  }
}

/**
 * A thread that answers kafka requests.
 */
class KafkaRequestHandler(
  id: Int,
  brokerId: Int,
  val aggregateIdleMeter: Meter,
  val totalHandlerThreads: AtomicInteger,
  val requestChannel: RequestChannel,
  apis: ApiRequestHandler,
  time: Time,
  nodeName: String = "broker"
) extends Runnable with Logging {
  this.logIdent = s"[Kafka Request Handler $id on ${nodeName.capitalize} $brokerId], "
  private val shutdownComplete = new CountDownLatch(1)
  private val requestLocal = RequestLocal.withThreadConfinedCaching
  @volatile private var stopped = false

  def run(): Unit = {
    threadRequestChannel.set(requestChannel)
    while (!stopped) {
      // We use a single meter for aggregate idle percentage for the thread pool.
      // Since meter is calculated as total_recorded_value / time_window and
      // time_window is independent of the number of threads, each recorded idle
      // time should be discounted by # threads.
      val startSelectTime = time.nanoseconds

      val req = requestChannel.receiveRequest(300)
      val endTime = time.nanoseconds
      val idleTime = endTime - startSelectTime
      aggregateIdleMeter.mark(idleTime / totalHandlerThreads.get)

      req match {
        case RequestChannel.ShutdownRequest =>
          debug(s"Kafka request handler $id on broker $brokerId received shut down command")
          completeShutdown()
          return

        case callback: RequestChannel.CallbackRequest =>
          val originalRequest = callback.originalRequest
          try {

            // If we've already executed a callback for this request, reset the times and subtract the callback time from the 
            // new dequeue time. This will allow calculation of multiple callback times.
            // Otherwise, set dequeue time to now.
            if (originalRequest.callbackRequestDequeueTimeNanos.isDefined) {
              val prevCallbacksTimeNanos = originalRequest.callbackRequestCompleteTimeNanos.getOrElse(0L) - originalRequest.callbackRequestDequeueTimeNanos.getOrElse(0L)
              originalRequest.callbackRequestCompleteTimeNanos = None
              originalRequest.callbackRequestDequeueTimeNanos = Some(time.nanoseconds() - prevCallbacksTimeNanos)
            } else {
              originalRequest.callbackRequestDequeueTimeNanos = Some(time.nanoseconds())
            }
            
            threadCurrentRequest.set(originalRequest)
            callback.fun(requestLocal)
          } catch {
            case e: FatalExitError =>
              completeShutdown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            // When handling requests, we try to complete actions after, so we should try to do so here as well.
            apis.tryCompleteActions()
            if (originalRequest.callbackRequestCompleteTimeNanos.isEmpty)
              originalRequest.callbackRequestCompleteTimeNanos = Some(time.nanoseconds())
            threadCurrentRequest.remove()
          }

        case request: RequestChannel.Request =>
          try {
            request.requestDequeueTimeNanos = endTime
            trace(s"Kafka request handler $id on broker $brokerId handling request $request")
            threadCurrentRequest.set(request)
            apis.handle(request, requestLocal)
          } catch {
            case e: FatalExitError =>
              completeShutdown()
              Exit.exit(e.statusCode)
            case e: Throwable => error("Exception when handling request", e)
          } finally {
            threadCurrentRequest.remove()
            request.releaseBuffer()
          }

        case RequestChannel.WakeupRequest => 
          // We should handle this in receiveRequest by polling callbackQueue.
          warn("Received a wakeup request outside of typical usage.")

        case null => // continue
      }
    }
    completeShutdown()
  }

  private def completeShutdown(): Unit = {
    requestLocal.close()
    threadRequestChannel.remove()
    shutdownComplete.countDown()
  }

  def stop(): Unit = {
    stopped = true
  }

  def initiateShutdown(): Unit = requestChannel.sendShutdownRequest()

  def awaitShutdown(): Unit = shutdownComplete.await()

}

class KafkaRequestHandlerPool(
  val brokerId: Int,
  val requestChannel: RequestChannel,
  val apis: ApiRequestHandler,
  time: Time,
  numThreads: Int,
  requestHandlerAvgIdleMetricName: String,
  logAndThreadNamePrefix : String,
  nodeName: String = "broker"
) extends Logging {
  private val metricsGroup = new KafkaMetricsGroup(this.getClass)

  val threadPoolSize: AtomicInteger = new AtomicInteger(numThreads)
  /* a meter to track the average free capacity of the request handlers */
  private val aggregateIdleMeter = metricsGroup.newMeter(requestHandlerAvgIdleMetricName, "percent", TimeUnit.NANOSECONDS)

  this.logIdent = "[" + logAndThreadNamePrefix + " Kafka Request Handler on Broker " + brokerId + "], "
  val runnables = new mutable.ArrayBuffer[KafkaRequestHandler](numThreads)
  for (i <- 0 until numThreads) {
    createHandler(i)
  }

  def createHandler(id: Int): Unit = synchronized {
    runnables += new KafkaRequestHandler(id, brokerId, aggregateIdleMeter, threadPoolSize, requestChannel, apis, time, nodeName)
    KafkaThread.daemon(logAndThreadNamePrefix + "-kafka-request-handler-" + id, runnables(id)).start()
  }

  def resizeThreadPool(newSize: Int): Unit = synchronized {
    val currentSize = threadPoolSize.get
    info(s"Resizing request handler thread pool size from $currentSize to $newSize")
    if (newSize > currentSize) {
      for (i <- currentSize until newSize) {
        createHandler(i)
      }
    } else if (newSize < currentSize) {
      for (i <- 1 to (currentSize - newSize)) {
        runnables.remove(currentSize - i).stop()
      }
    }
    threadPoolSize.set(newSize)
  }

  def shutdown(): Unit = synchronized {
    info("shutting down")
    for (handler <- runnables)
      handler.initiateShutdown()
    for (handler <- runnables)
      handler.awaitShutdown()
    info("shut down completely")
  }
}
