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

import java.util
import java.util.concurrent.TimeUnit.MILLISECONDS
import kafka.utils.Logging
import org.apache.kafka.clients.ClientResponse
import org.apache.kafka.common.Uuid
import org.apache.kafka.common.message.ControllerRegistrationRequestData
import org.apache.kafka.common.protocol.Errors
import org.apache.kafka.common.requests.{ControllerRegistrationRequest, ControllerRegistrationResponse}
import org.apache.kafka.metadata.{ListenerInfo, VersionRange}
import org.apache.kafka.common.utils.{ExponentialBackoff, LogContext, Time}
import org.apache.kafka.image.loader.LoaderManifest
import org.apache.kafka.image.{MetadataDelta, MetadataImage}
import org.apache.kafka.image.publisher.MetadataPublisher
import org.apache.kafka.queue.EventQueue.DeadlineFunction
import org.apache.kafka.queue.{EventQueue, KafkaEventQueue}
import org.apache.kafka.server.{ControllerRequestCompletionHandler, NodeToControllerChannelManager}
import org.apache.kafka.server.common.MetadataVersion

import scala.jdk.CollectionConverters._

/**
 * The controller registration manager handles registering this controller with the controller
 * quorum. This support was added by KIP-919, and requires a metadata version of 3.7 or higher.
 *
 * This code uses an event queue paradigm. Modifications get translated into events, which
 * are placed on the queue to be processed sequentially. As described in the JavaDoc for
 * each variable, most mutable state can be accessed only from that event queue thread.
 */
class ControllerRegistrationManager(
  val nodeId: Int,
  val clusterId: String,
  val time: Time,
  val threadNamePrefix: String,
  val supportedFeatures: util.Map[String, VersionRange],
  val zkMigrationEnabled: Boolean,
  val incarnationId: Uuid,
  val listenerInfo: ListenerInfo,
  val resendExponentialBackoff: ExponentialBackoff = new ExponentialBackoff(100, 2, 120000L, 0.02)
) extends Logging with MetadataPublisher {
  override def name(): String = "ControllerRegistrationManager"

  private def logPrefix(): String = {
    val builder = new StringBuilder("[ControllerRegistrationManager")
    builder.append(" id=").append(nodeId)
    builder.append(" incarnation=").append(incarnationId)
    builder.append("] ")
    builder.toString()
  }

  val logContext = new LogContext(logPrefix())

  this.logIdent = logContext.logPrefix()

  /**
   * True if there is a pending RPC. Only read or written from the event queue thread.
   */
  var pendingRpc = false

  /**
   * The number of RPCs that we successfully sent.
   * Only read or written from the event queue thread.
   */
  var successfulRpcs = 0L

  /**
   * The number of RPCs that we failed to send, or got back a failure response for. This is
   * cleared after a success. Only read or written from the event queue thread.
   */
  var failedRpcs = 0L

  /**
   * The current metadata version that is in effect. Only read or written from the event queue thread.
   */
  private var metadataVersion: MetadataVersion = MetadataVersion.MINIMUM_KRAFT_VERSION

  /**
   * True if we're registered. Only read or written from the event queue thread.
   */
  var registeredInLog: Boolean = false

  /**
   * The channel manager, or null if this manager has not been started yet.  This variable
   * can only be read or written from the event queue thread.
   */
  private var _channelManager: NodeToControllerChannelManager = _

  /**
   * The event queue.
   */
  private[server] val eventQueue = new KafkaEventQueue(time,
    logContext,
    threadNamePrefix + "registration-manager-",
    new ShutdownEvent())

  private class ShutdownEvent extends EventQueue.Event {
    override def run(): Unit = {
      try {
        info(s"shutting down.")
        if (_channelManager != null) {
          _channelManager.shutdown()
          _channelManager = null
        }
      } catch {
        case t: Throwable => error("ControllerRegistrationManager.stop error", t)
      }
    }
  }

  /**
   * Start the ControllerRegistrationManager.
   *
   * @param channelManager                The channel manager to use.
   */
  def start(channelManager: NodeToControllerChannelManager): Unit = {
    eventQueue.append(() => {
      try {
        info(s"initialized channel manager.")
        _channelManager = channelManager
        maybeSendControllerRegistration()
      } catch {
        case t: Throwable => error("start error", t)
      }
    })
  }

  /**
   * Start shutting down the ControllerRegistrationManager, but do not block.
   */
  def beginShutdown(): Unit = {
    eventQueue.beginShutdown("beginShutdown")
  }

  /**
   * Shut down the ControllerRegistrationManager and block until all threads are joined.
   */
  override def close(): Unit = {
    beginShutdown()
    eventQueue.close()
  }

  override def onMetadataUpdate(
    delta: MetadataDelta,
    newImage: MetadataImage,
    manifest: LoaderManifest
  ): Unit = {
    if (delta.featuresDelta() != null ||
        (delta.clusterDelta() != null && delta.clusterDelta().changedControllers().containsKey(nodeId))) {
      eventQueue.append(new MetadataUpdateEvent(delta, newImage))
    }
  }

  private class MetadataUpdateEvent(
    delta: MetadataDelta,
    newImage: MetadataImage
  ) extends EventQueue.Event {
    override def run(): Unit = {
      try {
        if (delta.featuresDelta() != null) {
          metadataVersion = newImage.features().metadataVersion()
        }
        if (delta.clusterDelta() != null) {
          if (delta.clusterDelta().changedControllers().containsKey(nodeId)) {
            val curRegistration = newImage.cluster().controllers().get(nodeId)
            if (curRegistration == null) {
              info(s"Registration removed for this node ID.")
              registeredInLog = false
            } else if (!curRegistration.incarnationId().equals(incarnationId)) {
              info(s"Found registration for ${curRegistration.incarnationId()} instead of our incarnation.")
              registeredInLog = false
            } else {
              info(s"Our registration has been persisted to the metadata log.")
              registeredInLog = true
            }
          }
        }
        maybeSendControllerRegistration()
      } catch {
        case t: Throwable => error("onMetadataUpdate error", t)
      }
    }
  }

  private def maybeSendControllerRegistration(): Unit = {
    if (registeredInLog) {
      debug("maybeSendControllerRegistration: controller is already registered.")
    } else if (_channelManager == null) {
      debug("maybeSendControllerRegistration: cannot register yet because the channel manager has " +
          "not been initialized.")
    } else if (!metadataVersion.isControllerRegistrationSupported) {
      info("maybeSendControllerRegistration: cannot register yet because the metadata version is " +
          s"still $metadataVersion, which does not support KIP-919 controller registration.")
    } else if (pendingRpc) {
      info("maybeSendControllerRegistration: waiting for the previous RPC to complete.")
    } else {
      sendControllerRegistration()
    }
  }

  private def sendControllerRegistration(): Unit = {
    val features = new ControllerRegistrationRequestData.FeatureCollection()
    supportedFeatures.asScala.foreach {
      case (name, range) => features.add(new ControllerRegistrationRequestData.Feature().
        setName(name).
        setMinSupportedVersion(range.min()).
        setMaxSupportedVersion(range.max()))
    }
    val data = new ControllerRegistrationRequestData().
      setControllerId(nodeId).
      setFeatures(features).
      setIncarnationId(incarnationId).
      setListeners(listenerInfo.toControllerRegistrationRequest).
      setZkMigrationReady(zkMigrationEnabled)

    info(s"sendControllerRegistration: attempting to send $data")
    _channelManager.sendRequest(new ControllerRegistrationRequest.Builder(data),
      new RegistrationResponseHandler())
    pendingRpc = true
  }

  private class RegistrationResponseHandler extends ControllerRequestCompletionHandler {
    override def onComplete(response: ClientResponse): Unit = {
      pendingRpc = false
      if (response.authenticationException() != null) {
        error(s"RegistrationResponseHandler: authentication error", response.authenticationException())
        scheduleNextCommunicationAfterFailure()
      } else if (response.versionMismatch() != null) {
        error(s"RegistrationResponseHandler: unsupported API version error", response.versionMismatch())
        scheduleNextCommunicationAfterFailure()
      } else if (response.responseBody() == null) {
        error(s"RegistrationResponseHandler: unknown error")
        scheduleNextCommunicationAfterFailure()
      } else if (!response.responseBody().isInstanceOf[ControllerRegistrationResponse]) {
        error(s"RegistrationResponseHandler: invalid response type error")
        scheduleNextCommunicationAfterFailure()
      } else {
        val message = response.responseBody().asInstanceOf[ControllerRegistrationResponse]
        val errorCode = Errors.forCode(message.data().errorCode())
        if (errorCode == Errors.NONE) {
          successfulRpcs = successfulRpcs + 1
          failedRpcs = 0
          info(s"RegistrationResponseHandler: controller acknowledged ControllerRegistrationRequest.")
        } else {
          info(s"RegistrationResponseHandler: controller returned error $errorCode " +
            s"(${message.data().errorMessage()})")
          scheduleNextCommunicationAfterFailure()
        }
      }
    }

    override def onTimeout(): Unit = {
      error(s"RegistrationResponseHandler: channel manager timed out before sending the request.")
      scheduleNextCommunicationAfterFailure()
    }
  }

  private def scheduleNextCommunicationAfterFailure(): Unit = {
    val delayMs = resendExponentialBackoff.backoff(failedRpcs)
    failedRpcs = failedRpcs + 1
    scheduleNextCommunication(delayMs)
  }

  private def scheduleNextCommunication(intervalMs: Long): Unit = {
    trace(s"Scheduling next communication at $intervalMs ms from now.")
    val deadlineNs = time.nanoseconds() + MILLISECONDS.toNanos(intervalMs)
    eventQueue.scheduleDeferred("communication",
      new DeadlineFunction(deadlineNs),
      () => maybeSendControllerRegistration())
  }
}
