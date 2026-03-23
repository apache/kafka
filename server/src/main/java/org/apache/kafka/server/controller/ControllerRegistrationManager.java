/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.server.controller;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.ControllerRegistrationRequestData;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ControllerRegistrationRequest;
import org.apache.kafka.common.requests.ControllerRegistrationResponse;
import org.apache.kafka.common.utils.ExponentialBackoff;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.image.MetadataDelta;
import org.apache.kafka.image.MetadataImage;
import org.apache.kafka.image.loader.LoaderManifest;
import org.apache.kafka.image.publisher.MetadataPublisher;
import org.apache.kafka.metadata.ControllerRegistration;
import org.apache.kafka.metadata.ListenerInfo;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.MetadataVersion;
import org.apache.kafka.server.common.NodeToControllerChannelManager;

import org.slf4j.Logger;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * The controller registration manager handles registering this controller with the controller
 * quorum. This support was added by KIP-919, and requires a metadata version of 3.7 or higher.
 *
 * This code uses an event queue paradigm. Modifications get translated into events, which
 * are placed on the queue to be processed sequentially. As described in the JavaDoc for
 * each variable, most mutable state can be accessed only from that event queue thread.
 */
public class ControllerRegistrationManager implements MetadataPublisher {
    
    private final Logger logger;
    private final int nodeId;
    private final Time time;
    private final Map<String, VersionRange> supportedFeatures;
    private final Uuid incarnationId;
    private final ListenerInfo listenerInfo;
    private final ExponentialBackoff resendExponentialBackoff;

    /**
     * The event queue.
     */
    private final KafkaEventQueue eventQueue;

    /**
     * True if there is a pending RPC. Only read or written from the event queue thread.
     */
    private boolean pendingRpc = false;

    /**
     * The number of RPCs that we successfully sent.
     * Only read or written from the event queue thread.
     */
    private long successfulRpcs = 0L;

    /**
     * The number of RPCs that we failed to send, or got back a failure response for. This is
     * cleared after a success. Only read or written from the event queue thread.
     */
    private long failedRpcs = 0L;

    /**
     * The current metadata version that is in effect. Only read or written from the event queue thread.
     */
    private Optional<MetadataVersion> metadataVersion = Optional.empty();

    /**
     * True if we're registered. Only read or written from the event queue thread.
     */
    private boolean registeredInLog = false;

    /**
     * The channel manager, or null if this manager has not been started yet. This variable
     * can only be read or written from the event queue thread.
     */
    private NodeToControllerChannelManager channelManager;

    public ControllerRegistrationManager(
            int nodeId,
            Time time,
            String threadNamePrefix,
            Map<String, VersionRange> supportedFeatures,
            Uuid incarnationId,
            ListenerInfo listenerInfo) {
        this(nodeId, time, threadNamePrefix, supportedFeatures, incarnationId, listenerInfo, new ExponentialBackoff(100, 2, 120000L, 0.02));
    }

    public ControllerRegistrationManager(
            int nodeId,
            Time time,
            String threadNamePrefix,
            Map<String, VersionRange> supportedFeatures,
            Uuid incarnationId,
            ListenerInfo listenerInfo,
            ExponentialBackoff resendExponentialBackoff
    )  {
        this.nodeId = nodeId;
        this.time = time;
        this.supportedFeatures = supportedFeatures;
        this.incarnationId = incarnationId;
        this.listenerInfo = listenerInfo;
        this.resendExponentialBackoff = resendExponentialBackoff;
        LogContext logContext = new LogContext("[ControllerRegistrationManager" +
                " id=" + this.nodeId +
                " incarnation=" + this.incarnationId +
                "] ");
        this.logger = logContext.logger(ControllerRegistrationManager.class);
        this.eventQueue = new KafkaEventQueue(time,
                logContext,
                threadNamePrefix + "registration-manager-",
                new ShutdownEvent());
    }
    
    @Override 
    public String name() {
        return "ControllerRegistrationManager";
    }

    private class ShutdownEvent implements EventQueue.Event {
        
        @Override 
        public void run() {
            try {
                logger.info("shutting down.");
                if (channelManager != null) {
                    channelManager.shutdown();
                    channelManager = null;
                }
            } catch (Throwable t) {
                logger.error("ControllerRegistrationManager.stop error", t);
            }
        }
    }

    /**
     * Start the ControllerRegistrationManager.
     *
     * @param channelManager The channel manager to use.
     */
    public void start(NodeToControllerChannelManager channelManager) {
        eventQueue.append(() -> {
            try {
                logger.info("initialized channel manager.");
                this.channelManager = channelManager;
                maybeSendControllerRegistration();
            } catch (Throwable t) {
                logger.error("start error", t);
            }
        });
    }

    /**
     * Start shutting down the ControllerRegistrationManager, but do not block.
     */
    void beginShutdown() {
        eventQueue.beginShutdown("beginShutdown");
    }

    /**
     * Shut down the ControllerRegistrationManager and block until all threads are joined.
     */
    @Override 
    public void close() throws Exception {
        beginShutdown();
        eventQueue.close();
    }

    @Override 
    public void onMetadataUpdate(MetadataDelta delta, MetadataImage newImage, LoaderManifest manifest) {
        if (delta.featuresDelta() != null ||
                (delta.clusterDelta() != null && delta.clusterDelta().changedControllers().containsKey(nodeId))) {
            eventQueue.append(new MetadataUpdateEvent(delta, newImage));
        }
    }

    private class MetadataUpdateEvent implements EventQueue.Event {

        private final MetadataDelta delta;
        private final MetadataImage newImage;

        MetadataUpdateEvent(MetadataDelta delta, MetadataImage newImage) {
            this.delta = delta;
            this.newImage = newImage;
        }
            
        @Override 
        public void run() {
            try {
                if (delta.featuresDelta() != null) {
                    metadataVersion = newImage.features().metadataVersion();
                }
                if (delta.clusterDelta() != null) {
                    if (delta.clusterDelta().changedControllers().containsKey(nodeId)) {
                        ControllerRegistration curRegistration = newImage.cluster().controllers().get(nodeId);
                        if (curRegistration == null) {
                            logger.info("Registration removed for this node ID.");
                            registeredInLog = false;
                        } else if (!curRegistration.incarnationId().equals(incarnationId)) {
                            logger.info("Found registration for {} instead of our incarnation.", curRegistration.incarnationId());
                            registeredInLog = false;
                        } else {
                            logger.info("Our registration has been persisted to the metadata log.");
                            registeredInLog = true;
                        }
                    }
                }
                maybeSendControllerRegistration();
            } catch (Throwable t) {
                logger.error("onMetadataUpdate error", t);
            }
        }
    }

    private void maybeSendControllerRegistration() {
        Optional<MetadataVersion> metadataVersion = this.metadataVersion;
        if (registeredInLog) {
            logger.debug("maybeSendControllerRegistration: controller is already registered.");
        } else if (channelManager == null) {
            logger.debug("maybeSendControllerRegistration: cannot register yet because the channel manager has not been initialized.");
        } else if (metadataVersion.isEmpty()) {
            logger.info("maybeSendControllerRegistration: cannot register yet because the metadata.version is not known yet.");
        } else if (!metadataVersion.get().isControllerRegistrationSupported()) {
            logger.info("maybeSendControllerRegistration: cannot register yet because the metadata.version is " +
                    "still {}, which does not support KIP-919 controller registration.", metadataVersion);
        } else if (pendingRpc) {
            logger.info("maybeSendControllerRegistration: waiting for the previous RPC to complete.");
        } else {
            sendControllerRegistration();
        }
    }

    private void sendControllerRegistration() {
        ControllerRegistrationRequestData.FeatureCollection features = new ControllerRegistrationRequestData.FeatureCollection();
        supportedFeatures.forEach((name, range) -> features.add(new ControllerRegistrationRequestData.Feature().
                setName(name).
                setMinSupportedVersion(range.min()).
                setMaxSupportedVersion(range.max())));
        ControllerRegistrationRequestData data = new ControllerRegistrationRequestData().
            setControllerId(nodeId).
            setFeatures(features).
            setIncarnationId(incarnationId).
            setListeners(listenerInfo.toControllerRegistrationRequest()).
            setZkMigrationReady(false);

        logger.info("sendControllerRegistration: attempting to send {}", data);
        channelManager.sendRequest(new ControllerRegistrationRequest.Builder(data),
                new RegistrationResponseHandler());
        pendingRpc = true;
    }

    private class RegistrationResponseHandler implements ControllerRequestCompletionHandler {
        
        @Override 
        public void onComplete(ClientResponse response) {
            eventQueue.append(new RequestCompleteEvent(response));
        }

        @Override
        public void onTimeout() {
            eventQueue.append(new RequestTimeoutEvent());
        }
    }

    private class RequestCompleteEvent implements EventQueue.Event {

        private final ClientResponse response;
        
        RequestCompleteEvent(ClientResponse response) {
            this.response = response;
        }
        
        @Override 
        public void run() {
            pendingRpc = false;
            if (response.authenticationException() != null) {
                logger.error("RegistrationResponseHandler: authentication error", response.authenticationException());
                scheduleNextCommunicationAfterFailure();
            } else if (response.versionMismatch() != null) {
                logger.error("RegistrationResponseHandler: unsupported API version error", response.versionMismatch());
                scheduleNextCommunicationAfterFailure();
            } else if (response.responseBody() == null) {
                logger.error("RegistrationResponseHandler: unknown error");
                scheduleNextCommunicationAfterFailure();
            } else if (!(response.responseBody() instanceof ControllerRegistrationResponse message)) {
                logger.error("RegistrationResponseHandler: invalid response type error");
                scheduleNextCommunicationAfterFailure();
            } else {
                Errors errorCode = Errors.forCode(message.data().errorCode());
                if (errorCode == Errors.NONE) {
                    successfulRpcs = successfulRpcs + 1;
                    failedRpcs = 0;
                    logger.info("RegistrationResponseHandler: controller acknowledged ControllerRegistrationRequest.");
                } else {
                    logger.info("RegistrationResponseHandler: controller returned error {} ({})", errorCode, message.data().errorMessage());
                    scheduleNextCommunicationAfterFailure();
                }
            }
        }
    }

    private class RequestTimeoutEvent implements EventQueue.Event {
        
        @Override 
        public void run() {
            pendingRpc = false;
            logger.error("RegistrationResponseHandler: channel manager timed out before sending the request.");
            scheduleNextCommunicationAfterFailure();
        }
    }

    private void scheduleNextCommunicationAfterFailure() {
        long delayMs = resendExponentialBackoff.backoff(failedRpcs);
        failedRpcs = failedRpcs + 1;
        scheduleNextCommunication(delayMs);
    }

    private void scheduleNextCommunication(long intervalMs) {
        logger.trace("Scheduling next communication at {} ms from now.", intervalMs);
        long deadlineNs = time.nanoseconds() + TimeUnit.MILLISECONDS.toNanos(intervalMs);
        eventQueue.scheduleDeferred("communication",
                new EventQueue.DeadlineFunction(deadlineNs),
                this::maybeSendControllerRegistration);
    }

    // Only for testing
    public KafkaEventQueue eventQueue() {
        return eventQueue;
    }

    // Only for testing
    public boolean registeredInLog() {
        return registeredInLog;
    }

    // Only for testing
    public boolean pendingRpc() {
        return pendingRpc;
    }

    // Only for testing
    public long successfulRpcs() {
        return successfulRpcs;
    }

    // Only for testing
    public long failedRpcs() {
        return failedRpcs;
    }

}
