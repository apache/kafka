/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package kafka.server

import java.io.IOException
import java.util
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledFuture, TimeUnit}

import org.apache.kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.metadata.{BrokerMetadataListener, FenceBrokerEvent, RegisterBrokerEvent}
import kafka.utils.{Logging, Scheduler}
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.message.{BrokerHeartbeatRequestData, BrokerRegistrationRequestData}
import org.apache.kafka.common.message.BrokerRegistrationRequestData.{FeatureCollection, ListenerCollection}
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{BrokerHeartbeatRequest, BrokerHeartbeatResponse, BrokerRegistrationRequest, BrokerRegistrationResponse}
import org.apache.kafka.common.utils.Time
import org.apache.kafka.{metadata => jmetadata}

import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

/**
 * Manages the broker lifecycle. Handles:
 * - Broker Registration
 * - Broker Heartbeats
 *
 * Explicit broker state transitions are performed by co-ordinating
 * with the controller through the heartbeats.
 *
 */
trait BrokerLifecycleManager {

  // Initiate broker registration and start the heartbeat scheduler loop
  def start(listeners: ListenerCollection, features: FeatureCollection): Unit

  // Enqueue a heartbeat request to be sent to the active controller
  // Specify the target state for the broker
  // - For the periodic heartbeat, this is always the current state of the controller
  // Note: This does not enforce any specific order of the state transitions. The target
  //       state is sent out as enqueued.
  // Returns a promise that indicates success if completed w/o exception
  def enqueue(state: jmetadata.BrokerState): Promise[Unit]

  // Current broker state
  def brokerState: jmetadata.BrokerState

  // Last successful heartbeat time
  def lastSuccessfulHeartbeatTime: Long

  // Stop the scheduler loop
  def stop(): Unit
}

/**
 * Implements the BrokerLifecycleManager trait. Uses a concurrent queue to process state changes/notifications.
 * Also responsible for maintaining the broker state based on the response from the controller.
 * Note: We don't start sending heartbeats out until a state change is requested from NOT_RUNNING -> *
 * At startup, the default state being NOT_RUNNING, the broker will NOT attempt to communicate
 * w/ the active controller until the Broker Registration is complete.
 *
 * @param config                   - Kafka config used for configuring the relevant heartbeat timeouts
 * @param controllerChannelManager - Channel to interact with the active controller
 * @param scheduler                - The scheduler for scheduling the heartbeat tasks on
 * @param time                     - Default time provider
 * @param brokerID                 - This broker's ID
 * @param rack                     - The rack the broker is hosted on
 * @param metadataOffset           - The last committed/processed metadata offset provider for this broker
 * @param brokerEpoch              - This broker's current epoch provider
 */
class BrokerLifecycleManagerImpl(val brokerMetadataListener: BrokerMetadataListener, val config: KafkaConfig, val controllerChannelManager: BrokerToControllerChannelManager, val scheduler: Scheduler, val time: Time, val brokerID: Int, val rack: String, val metadataOffset: () => Long, val brokerEpoch: () => Long) extends BrokerLifecycleManager with Logging with KafkaMetricsGroup {

  // Request queue
  private val requestQueue: util.Queue[(jmetadata.BrokerState, Promise[Unit])] = new ConcurrentLinkedQueue[(jmetadata.BrokerState, Promise[Unit])]()

  // Scheduler task
  private var schedulerTask: Option[ScheduledFuture[_]] = None

  // Broker states - Current and Target/Next
  private var currentState: jmetadata.BrokerState = _
  private val pendingHeartbeat = new AtomicBoolean(false)

  // Metrics - Histogram of broker heartbeat request/response time
  // FIXME: Tags
  private val heartbeatResponseTime = newHistogram(
    name = "BrokerHeartbeatResponseTimeMs",
    biased = true,
    Map("request" -> "BrokerHeartBeat")
  )
  private var _lastSuccessfulHeartbeatTime: Long = 0 // nanoseconds

  /**
   * Attempts to register the broker
   * - On success, schedules a periodic heartbeat task
   * - On failure, throws an exception which can be conditionally retried
   *
   * @param listeners - List of endpoints configured for this broker
   * @param features - List of features supported by this broker
   * @throws org.apache.kafka.common.errors.AuthenticationException
   *         - Transport layer authentication errors
   * @throws org.apache.kafka.common.errors.UnsupportedVersionException
   *         - Broker version not supported
   * @throws java.io.IOException
   *         - Disconnected client/Invalid response from the controller
   * @throws java.lang.InterruptedException
   *         - Registration wait was interrupted
   * @throws java.util.concurrent.TimeoutException
   *         - Registration wait timed out (max wait: registration.lease.timeout.ms)
   * @throws org.apache.kafka.common.errors.DuplicateBrokerRegistrationException
   *         - Duplicate broker ID during registration
   * @throws org.apache.kafka.common.KafkaException
   *         - Generic catch all for unknown/unhandled exception(s)
   *
   */
  override def start(listeners: ListenerCollection, features: FeatureCollection): Unit = {
    // FIXME: Handle broker registration inconsistencies where the controller successfully registers the broker but the
    //        broker times-out/fails on the RPC. Retrying today, would lead to a DuplicateBrokerRegistrationException
    currentState = jmetadata.BrokerState.NOT_RUNNING

    // Initiate broker registration
    val brokerRegistrationData = new BrokerRegistrationRequestData()
      .setBrokerId(brokerID)
      .setFeatures(features)
      .setListeners(listeners)
      .setRack(rack)
    val promise = Promise[Unit]()

    def responseHandler(response: ClientResponse) = {
      validateResponse(response) match {
        case None =>
          // Check for API errors
          val body = response.responseBody().asInstanceOf[BrokerRegistrationResponse].data
          Errors.forCode(body.errorCode()) match {
            case Errors.DUPLICATE_BROKER_REGISTRATION =>
              currentState = jmetadata.BrokerState.NOT_RUNNING
              promise.tryFailure(Errors.DUPLICATE_BROKER_REGISTRATION.exception())
            case Errors.NONE =>
              // TODO: Is this the correct next state?
              currentState = jmetadata.BrokerState.RECOVERING_FROM_UNCLEAN_SHUTDOWN
              // Registration success; notify the BrokerMetadataListener
              brokerMetadataListener.put(RegisterBrokerEvent(body.brokerEpoch))
              promise.trySuccess(())
            case _ =>
              // Unhandled error
              currentState = jmetadata.BrokerState.NOT_RUNNING
              promise.tryFailure(Errors.forCode(body.errorCode()).exception())
          }
        case Some(throwable) =>
          currentState = jmetadata.BrokerState.NOT_RUNNING
          promise.tryFailure(throwable)
      }
    }
    currentState = jmetadata.BrokerState.REGISTERING
    controllerChannelManager.sendRequest(new BrokerRegistrationRequest.Builder(brokerRegistrationData), responseHandler)

    // Wait for broker registration
    // Note: We want to deliberately block here since the rest of the broker startup process
    //       is dependent on the registration succeeding first
    // TODO: Maybe set a lower timeout?
    Await.result(promise.future, Duration(config.registrationLeaseTimeoutMs.longValue(), MILLISECONDS))

    // Broker registration successful; Schedule heartbeats
    schedulerTask = Some(scheduler.schedule(
      "send-broker-heartbeat",
      processHeartbeatRequests,
      config.registrationHeartbeatIntervalMs.longValue(),
      config.registrationHeartbeatIntervalMs.longValue(),
      TimeUnit.MILLISECONDS)
    )
  }

  /**
   * Enqueue a state change request for the target state
   *
   * @param state - Target state requested
   * @return Promise[Unit] - To wait for success/failure of the state change
   *
   */
  override def enqueue(state: jmetadata.BrokerState): Promise[Unit] = {
    // TODO: Ignore requests if requested state is the same as the current state?
    val promise = Promise[Unit]()
    requestQueue.add((state, promise))
    promise
  }

  /**
   * Stop the heartbeat scheduler
   *
   */
  override def stop(): Unit = {
    // TODO: BrokerShutdown state change
    schedulerTask foreach {
      task => task.cancel(true)
    }
    requestQueue.clear()
  }

  /**
   * Generic response validator
   *   - Checks for common transport errors
   *
   */
  private def validateResponse(response: ClientResponse): Option[Throwable] = {
    // Check for any transport errors
    if (response.authenticationException() != null) {
      Some(response.authenticationException())
    } else if (response.versionMismatch() != null) {
      Some(response.versionMismatch())
    } else if (response.wasDisconnected()) {
      Some(new IOException("Client was disconnected"))
    } else if (!response.hasResponse) {
      Some(new IOException("No response found"))
    } else {
      None
    }
  }

  /**
   * Task loop to be scheduled
   *   - Schedule another heartbeat if no heartbeat is pending/in-flight AND time since last heartbeat >=
   *     registration.heartbeat.interval.ms
   *   - If time since last heartbeat > registration.lease.timeout.ms, FENCE the broker
   *
   */
  private def processHeartbeatRequests(): Unit = {
    // TODO: Do we want to allow some sort preemption to prioritize critical state changes?
    // TODO: Ensure RPC timeout < heartbeat interval timeout (or at the very least < registration lease timeout)

    // Ensure that there are no outstanding state change requests
    if (pendingHeartbeat.compareAndSet(false, true)) {
      // No pending heartbeat in-flight. We have a few things to check during this iteration
      // Check when the last heartbeat was successful
      // - If < registration.heartbeat.interval.ms, no-op
      // - Else, check for any pending state changes that have been queued
      //   - Attempt a heartbeat w/ the requested state change
      // - No state change requests queued
      //   - If > registration.lease.timeout.ms (We have fallen way behind and it's best to fence ourselves here)
      //     - Fence ourselves and attempt a state change from FENCED -> ACTIVE in the next iteration
      //   - If > registration.heartbeat.interval.ms
      //     - Attempt another heartbeat w/ targetState = currentState
      //
      val timeSinceLastHeartbeat = TimeUnit.NANOSECONDS.toMillis(time.nanoseconds - lastSuccessfulHeartbeatTime)
      // NOTE: We still have to ensure the last heartbeat was sent at least w/ a gap of registration.heartbeat.interval.ms
      //       even though the task is scheduled at intervals registration.heartbeat.interval.ms because of
      //       scheduler ticks being batched in some cases where another task hogs the scheduler's runtime.
      //       We're not real-time here and so this accounts for two task runs occurring almost immediately one after
      //       the other
      if (timeSinceLastHeartbeat < config.registrationHeartbeatIntervalMs) {
        // No-op
        pendingHeartbeat.compareAndSet(true, false)
        return
      }

      // Check for any pending state changes that have been queued
      var state = requestQueue.poll
      if (state == null) {
        // No state change requests queued
        if (timeSinceLastHeartbeat > config.registrationLeaseTimeoutMs) {
          error(s"Last successful heartbeat was $timeSinceLastHeartbeat ms ago")
          // Fence ourselves; notify the BrokerMetadataListener
          currentState = jmetadata.BrokerState.FENCED
          brokerMetadataListener.put(FenceBrokerEvent(brokerEpoch()))
          // FIXME: What is the preferred action here? Do we wait for an external actor queue a state change
          //       request?
          pendingHeartbeat.compareAndSet(true, false)
          return
        } else if (timeSinceLastHeartbeat >= config.registrationHeartbeatIntervalMs) {
          // Attempt another heartbeat w/ targetState = currentState
          state = (currentState, Promise[Unit]())
        }
      }
      sendHeartbeat(state)
    }
  }

  /**
   * Send heartbeat request to the active controller
   *
   * @param requestState - Tuple of the target state and the associated pending promise
   *
   */
  private def sendHeartbeat(requestState: (jmetadata.BrokerState, Promise[Unit])): Unit = {

    val sendTime = time.nanoseconds

    // Construct broker heartbeat request
    def request: BrokerHeartbeatRequestData = {
      new BrokerHeartbeatRequestData()
        .setBrokerEpoch(brokerEpoch())
        .setBrokerId(brokerID)
        .setCurrentMetadataOffset(metadataOffset())
        .setCurrentState(currentState.value())
        .setTargetState(requestState._1.value())
    }

    def responseHandler(response: ClientResponse): Unit = {
      // Check for any transport errors
      validateResponse(response) match {
        case None =>
          // Extract API response
          val body = response.responseBody().asInstanceOf[BrokerHeartbeatResponse]
          handleBrokerHeartbeatResponse(body) match {
            case None =>
              // Update metrics
              heartbeatResponseTime.update(time.nanoseconds - sendTime)
              // Report success
              requestState._2.trySuccess(())
            case Some(errorMsg) => requestState._2.tryFailure(new KafkaException(errorMsg.toString))
          }
        case Some(throwable) =>
          requestState._2.tryFailure(throwable)
      }
      pendingHeartbeat.compareAndSet(true, false)
    }

    debug(s"Sending BrokerHeartbeatRequest to controller $request")
    controllerChannelManager.sendRequest(new BrokerHeartbeatRequest.Builder(request), responseHandler)
  }

  /**
   * Handles a heartbeat response to determine success/failure of the in-flight state change request
   *
   * @param response - From the active controller
   * @return
   */
  private def handleBrokerHeartbeatResponse(response: BrokerHeartbeatResponse): Option[Errors] = {
    if (response.data().errorCode() != 0) {
      val errorMsg = Errors.forCode(response.data().errorCode())
      error(s"Broker heartbeat failure: $errorMsg")
      Some(errorMsg)
    } else {
      currentState = jmetadata.BrokerState.fromValue(response.data().nextState())
      _lastSuccessfulHeartbeatTime = time.nanoseconds
      None
    }
  }

  /**
   * Exports current broker state
   *
   * @return
   */
  override def brokerState: jmetadata.BrokerState = currentState

  /**
   * Last successful heartbeat time in nanoseconds; using the JVM's high-resolution timer
   *   - NOT wall-clock time
   */
  override def lastSuccessfulHeartbeatTime: Long = _lastSuccessfulHeartbeatTime
}