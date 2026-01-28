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
package org.apache.kafka.server;

import org.apache.kafka.clients.ClientResponse;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.BrokerHeartbeatRequestData;
import org.apache.kafka.common.message.BrokerHeartbeatResponseData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData;
import org.apache.kafka.common.message.BrokerRegistrationRequestData.ListenerCollection;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.BrokerHeartbeatRequest;
import org.apache.kafka.common.requests.BrokerHeartbeatResponse;
import org.apache.kafka.common.requests.BrokerRegistrationRequest;
import org.apache.kafka.common.requests.BrokerRegistrationResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.metadata.BrokerState;
import org.apache.kafka.metadata.VersionRange;
import org.apache.kafka.queue.EventQueue;
import org.apache.kafka.queue.KafkaEventQueue;
import org.apache.kafka.server.common.ControllerRequestCompletionHandler;
import org.apache.kafka.server.common.NodeToControllerChannelManager;
import org.apache.kafka.server.config.AbstractKafkaConfig;

import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

/**
 * The broker lifecycle manager owns the broker state.
 *
 * Its inputs are messages passed in from other parts of the broker and from the
 * controller: requests to start up, or shut down, for example. Its output are the broker
 * state and various futures that can be used to wait for broker state transitions to
 * occur.
 *
 * The lifecycle manager handles registering the broker with the controller, as described
 * in KIP-631. After registration is complete, it handles sending periodic broker
 * heartbeats and processing the responses.
 *
 * This code uses an event queue paradigm. Modifications get translated into events, which
 * are placed on the queue to be processed sequentially. As described in the JavaDoc for
 * each variable, most mutable state can be accessed only from that event queue thread.
 * In some cases we expose a volatile variable which can be read from any thread, but only
 * written from the event queue thread.
 */
public class BrokerLifecycleManager {

    private final Logger logger;
    private final KafkaEventQueue eventQueue;
    private final AbstractKafkaConfig config;
    private final Time time;
    private final Set<Uuid> logDirs;
    private final Runnable shutdownHook;

    /**
     * The broker id.
     */
    private final int nodeId;

    /**
     * The broker rack, or null if there is no configured rack.
     */
    private final Optional<String> rack;

    /**
     * How long to wait for registration to succeed before failing the startup process.
     */
    private final long initialTimeoutNs;

    /**
     * The broker incarnation ID.  This ID uniquely identifies each time we start the broker
     */
    private final Uuid incarnationId = Uuid.randomUuid();

    /**
     * A future which is completed just as soon as the broker has caught up with the latest
     * metadata offset for the first time.
     */
    private final CompletableFuture<Void> initialCatchUpFuture = new CompletableFuture<>();

    /**
     * A future which is completed when the broker is unfenced for the first time.
     */
    private final CompletableFuture<Void> initialUnfenceFuture = new CompletableFuture<>();

    /**
     * A future which is completed when controlled shutdown is done.
     */
    private final CompletableFuture<Void> controlledShutdownFuture = new CompletableFuture<>();

    /**
     * The broker epoch, or -1 if the broker has not yet registered.  This variable can only
     * be written from the event queue thread.
     */
    private volatile long brokerEpoch = -1L;

    /**
     * The current broker state.  This variable can only be written from the event queue
     * thread.
     */
    private volatile BrokerState state = BrokerState.NOT_RUNNING;

    /**
     * A thread-safe callback function which gives this manager the current highest metadata
     * offset.  This variable can only be read or written from the event queue thread.
     */
    private Supplier<Long> highestMetadataOffsetProvider;

    /**
     * True only if we are ready to unfence the broker.  This variable can only be read or
     * written from the event queue thread.
     */
    private boolean readyToUnfence = false;

    /**
     * Map of accumulated offline directories. The value is true if the directory has been successfully communicated
     * to the Controller.
     * This variable can only be read or written from the event queue thread.
     */
    private Map<Uuid, Boolean> offlineDirs = new HashMap<>();

    /**
     * True if we sent an event queue to the active controller requesting controlled
     * shutdown.  This variable can only be read or written from the event queue thread.
     */
    private boolean gotControlledShutdownResponse = false;

    /**
     * Whether this broker is registered with the controller quorum.
     * This variable can only be read or written from the event queue thread.
     */
    private boolean registered = false;

    /**
     * True if a request has been sent and a response or timeout has not yet been processed.
     * This variable can only be read or written from the event queue thread.
     */
    private boolean communicationInFlight = false;

    /**
     * True if we should schedule the next communication immediately. This is used to delay
     * an immediate scheduling of a communication event if one is already in flight.
     * This variable can only be read or written from the event queue thread.
     */
    private boolean nextSchedulingShouldBeImmediate = false;

    /**
     * True if the initial registration succeeded.  This variable can only be read or
     * written from the event queue thread.
     */
    private boolean initialRegistrationSucceeded = false;

    /**
     * The cluster ID, or null if this manager has not been started yet.  This variable can
     * only be read or written from the event queue thread.
     */
    private String clusterId;

    /**
     * The listeners which this broker advertises.  This variable can only be read or
     * written from the event queue thread.
     */
    private ListenerCollection advertisedListeners;

    /**
     * The features supported by this broker.  This variable can only be read or written
     * from the event queue thread.
     */
    private Map<String, VersionRange> supportedFeatures;

    /**
     * The channel manager, or null if this manager has not been started yet.  This variable
     * can only be read or written from the event queue thread.
     */
    private NodeToControllerChannelManager channelManager;

    /**
     * The broker epoch from the previous run, or empty if the epoch is not found.
     */
    private volatile OptionalLong previousBrokerEpoch = OptionalLong.empty();

    public BrokerLifecycleManager(
            AbstractKafkaConfig config,
            Time time,
            String threadNamePrefix,
            Set<Uuid> logDirs) {
        this(config, time, threadNamePrefix, logDirs, () -> { });
    }

    public BrokerLifecycleManager(
            AbstractKafkaConfig config,
            Time time,
            String threadNamePrefix,
            Set<Uuid> logDirs,
            Runnable shutdownHook) {
        this.config = config;
        this.time = time;
        this.logDirs = logDirs;
        this.shutdownHook = shutdownHook;
        LogContext logContext = new LogContext("[BrokerLifecycleManager id=" + this.config.nodeId() + "] ");
        this.logger = logContext.logger(BrokerLifecycleManager.class);
        this.nodeId = config.nodeId();
        this.rack = config.rack();
        this.initialTimeoutNs = MILLISECONDS.toNanos(config.initialRegistrationTimeoutMs());
        this.eventQueue = new KafkaEventQueue(
                time,
                logContext,
                threadNamePrefix + "lifecycle-manager-",
                new ShutdownEvent());
    }

    /**
     * Start the BrokerLifecycleManager.
     *
     * @param highestMetadataOffsetProvider Provides the current highest metadata offset.
     * @param channelManager                The NodeToControllerChannelManager to use.
     * @param clusterId                     The cluster ID.
     * @param advertisedListeners           The advertised listeners for this broker.
     * @param supportedFeatures             The features for this broker.
     * @param previousBrokerEpoch           The broker epoch before the reboot.
     */
    public void start(Supplier<Long> highestMetadataOffsetProvider,
               NodeToControllerChannelManager channelManager,
               String clusterId,
               ListenerCollection advertisedListeners,
               Map<String, VersionRange> supportedFeatures,
               OptionalLong previousBrokerEpoch) {
        this.previousBrokerEpoch = previousBrokerEpoch;
        eventQueue.append(new StartupEvent(highestMetadataOffsetProvider,
                channelManager, clusterId, advertisedListeners, supportedFeatures));
    }

    public CompletableFuture<Void> setReadyToUnfence() {
        eventQueue.append(new SetReadyToUnfenceEvent());
        return initialUnfenceFuture;
    }

    /**
     * Propagate directory failures to the controller.
     *
     * @param directory The ID for the directory that failed.
     */
    public void propagateDirectoryFailure(Uuid directory, long timeout) {
        eventQueue.append(new OfflineDirEvent(directory));
        // If we can't communicate the offline directory to the controller, we should shut down.
        eventQueue.scheduleDeferred("offlineDirFailure",
                new EventQueue.DeadlineFunction(time.nanoseconds() + MILLISECONDS.toNanos(timeout)),
                new OfflineDirBrokerFailureEvent(directory));
    }

    public void resendBrokerRegistration() {
        eventQueue.append(new ResendBrokerRegistrationEvent());
    }

    private class ResendBrokerRegistrationEvent implements EventQueue.Event {
        @Override
        public void run() {
            registered = false;
            scheduleNextCommunicationImmediately();
        }
    }

    public long brokerEpoch() {
        return brokerEpoch;
    }

    public BrokerState state() {
        return state;
    }

    public CompletableFuture<Void> initialCatchUpFuture() {
        return initialCatchUpFuture;
    }

    public CompletableFuture<Void> initialUnfenceFuture() {
        return initialUnfenceFuture;
    }

    public CompletableFuture<Void> controlledShutdownFuture() {
        return controlledShutdownFuture;
    }

    public KafkaEventQueue eventQueue() {
        return eventQueue;
    }

    private class BeginControlledShutdownEvent implements EventQueue.Event {
        @Override
        public void run() {
            switch (state) {
                case PENDING_CONTROLLED_SHUTDOWN ->
                    logger.info("Attempted to enter pending controlled shutdown state, but we are already in that state.");
                case RUNNING -> {
                    logger.info("Beginning controlled shutdown.");
                    state = BrokerState.PENDING_CONTROLLED_SHUTDOWN;
                    // Send the next heartbeat immediately in order to let the controller
                    // begin processing the controlled shutdown as soon as possible.
                    scheduleNextCommunicationImmediately();
                }
                default -> {
                    logger.info("Skipping controlled shutdown because we are in state {}.", state);
                    beginShutdown();
                }
            }
        }
    }

    /**
     * Enter the controlled shutdown state if we are in RUNNING state.
     * Or, if we're not running, shut down immediately.
     */
    public void beginControlledShutdown() {
        eventQueue.append(new BeginControlledShutdownEvent());
    }

    /**
     * Start shutting down the BrokerLifecycleManager, but do not block.
     */
    public void beginShutdown() {
        eventQueue.beginShutdown("beginShutdown");
    }

    /**
     * Shut down the BrokerLifecycleManager and block until all threads are joined.
     */
    public void close() throws InterruptedException {
        beginShutdown();
        eventQueue.close();
    }

    private class SetReadyToUnfenceEvent implements EventQueue.Event {
        @Override
        public void run() {
            readyToUnfence = true;
            scheduleNextCommunicationImmediately();
        }
    }

    private class OfflineDirEvent implements EventQueue.Event {

        private final Uuid dir;

        OfflineDirEvent(Uuid dir) {
            this.dir = dir;
        }

        @Override
        public void run() {
            offlineDirs.put(dir, false);
            if (registered) {
                scheduleNextCommunicationImmediately();
            }
        }
    }

    private class OfflineDirBrokerFailureEvent implements EventQueue.Event {

        private final Uuid offlineDir;

        OfflineDirBrokerFailureEvent(Uuid offlineDir) {
            this.offlineDir = offlineDir;
        }

        @Override
        public void run() {
            if (!offlineDirs.getOrDefault(offlineDir, false)) {
                logger.error("Shutting down because couldn't communicate offline log dir {} with controllers", offlineDir);
                shutdownHook.run();
            }
        }
    }

    private class StartupEvent implements EventQueue.Event {

        private final Supplier<Long> highestMetadataOffsetProvider;
        private final NodeToControllerChannelManager channelManager;
        private final String clusterId;
        private final ListenerCollection advertisedListeners;
        private final Map<String, VersionRange> supportedFeatures;

        StartupEvent(Supplier<Long> highestMetadataOffsetProvider,
                     NodeToControllerChannelManager channelManager,
                     String clusterId,
                     ListenerCollection advertisedListeners,
                     Map<String, VersionRange> supportedFeatures) {
            this.highestMetadataOffsetProvider = highestMetadataOffsetProvider;
            this.channelManager = channelManager;
            this.clusterId = clusterId;
            this.advertisedListeners = advertisedListeners;
            this.supportedFeatures = supportedFeatures;
        }

        @Override
        public void run() {
            BrokerLifecycleManager.this.highestMetadataOffsetProvider = this.highestMetadataOffsetProvider;
            BrokerLifecycleManager.this.channelManager = channelManager;
            BrokerLifecycleManager.this.channelManager.start();
            state = BrokerState.STARTING;
            BrokerLifecycleManager.this.clusterId = clusterId;
            BrokerLifecycleManager.this.advertisedListeners = advertisedListeners.duplicate();
            BrokerLifecycleManager.this.supportedFeatures = Map.copyOf(supportedFeatures);
            eventQueue.scheduleDeferred("initialRegistrationTimeout",
                    new EventQueue.DeadlineFunction(time.nanoseconds() + initialTimeoutNs),
                    new RegistrationTimeoutEvent());
            sendBrokerRegistration();
            logger.info("Incarnation {} of broker {} in cluster {} is now STARTING.", incarnationId, nodeId, clusterId);
        }
    }

    private void sendBrokerRegistration() {
        BrokerRegistrationRequestData.FeatureCollection features = new BrokerRegistrationRequestData.FeatureCollection();
        supportedFeatures.forEach((name, range) ->
                features.add(new BrokerRegistrationRequestData.Feature()
                        .setName(name)
                        .setMinSupportedVersion(range.min())
                        .setMaxSupportedVersion(range.max()))
        );
        List<Uuid> sortedLogDirs = new ArrayList<>(logDirs);
        sortedLogDirs.sort(Uuid::compareTo);
        BrokerRegistrationRequestData data = new BrokerRegistrationRequestData()
            .setBrokerId(nodeId)
            .setIsMigratingZkBroker(false)
            .setClusterId(clusterId)
            .setFeatures(features)
            .setIncarnationId(incarnationId)
            .setListeners(advertisedListeners)
            .setRack(rack.orElse(null))
            .setPreviousBrokerEpoch(previousBrokerEpoch.orElse(-1L))
            .setLogDirs(sortedLogDirs);
        if (logger.isDebugEnabled()) {
            logger.debug("Sending broker registration {}", data);
        }
        channelManager.sendRequest(new BrokerRegistrationRequest.Builder(data),
                new BrokerRegistrationResponseHandler());
        communicationInFlight = true;
    }

    // the response handler is not invoked from the event handler thread,
    // so it is not safe to update state here, instead, schedule an event
    // to continue handling the response on the event handler thread
    private class BrokerRegistrationResponseHandler implements ControllerRequestCompletionHandler {
        @Override
        public void onComplete(ClientResponse response) {
            eventQueue.prepend(new BrokerRegistrationResponseEvent(response, false));
        }

        @Override
        public void onTimeout() {
            logger.info("Unable to register the broker because the RPC got timed out before it could be sent.");
            eventQueue.prepend(new BrokerRegistrationResponseEvent(null, true));
        }
    }

    private class BrokerRegistrationResponseEvent implements EventQueue.Event {

        private final ClientResponse response;
        private final boolean timedOut;

        BrokerRegistrationResponseEvent(ClientResponse response, boolean timedOut) {
            this.response = response;
            this.timedOut = timedOut;
        }

        @Override
        public void run() {
            communicationInFlight = false;
            if (timedOut) {
                scheduleNextCommunicationAfterFailure();
                return;
            }
            if (response.authenticationException() != null) {
                logger.error("Unable to register broker $nodeId because of an authentication exception.", response.authenticationException());
                scheduleNextCommunicationAfterFailure();
            } else if (response.versionMismatch() != null) {
                logger.error("Unable to register broker $nodeId because of an API version problem.", response.versionMismatch());
                scheduleNextCommunicationAfterFailure();
            } else if (response.responseBody() == null) {
                logger.warn("Unable to register broker {}.", nodeId);
                scheduleNextCommunicationAfterFailure();
            } else if (!(response.responseBody() instanceof BrokerRegistrationResponse message)) {
                logger.error("Unable to register broker {} because the controller returned an invalid response type.", nodeId);
                scheduleNextCommunicationAfterFailure();
            } else {
                Errors errorCode = Errors.forCode(message.data().errorCode());
                if (errorCode == Errors.NONE) {
                    brokerEpoch = message.data().brokerEpoch();
                    registered = true;
                    initialRegistrationSucceeded = true;
                    logger.info("Successfully registered broker {} with broker epoch {}", nodeId, brokerEpoch);
                    scheduleNextCommunicationImmediately(); // Immediately send a heartbeat
                } else {
                    logger.info("Unable to register broker {} because the controller returned error {}", nodeId, errorCode);
                    scheduleNextCommunicationAfterFailure();
                }
            }
        }
    }

    private void sendBrokerHeartbeat() {
        Long metadataOffset = highestMetadataOffsetProvider.get();
        BrokerHeartbeatRequestData data = new BrokerHeartbeatRequestData()
            .setBrokerEpoch(brokerEpoch)
            .setBrokerId(nodeId)
            .setCurrentMetadataOffset(metadataOffset)
            .setWantFence(!readyToUnfence)
            .setWantShutDown(state == BrokerState.PENDING_CONTROLLED_SHUTDOWN)
            .setOfflineLogDirs(new ArrayList<>(offlineDirs.keySet()));
        if (logger.isTraceEnabled()) {
            logger.trace("Sending broker heartbeat {}", data);
        }
        BrokerHeartbeatResponseHandler handler = new BrokerHeartbeatResponseHandler(offlineDirs.keySet());
        channelManager.sendRequest(new BrokerHeartbeatRequest.Builder(data), handler);
        communicationInFlight = true;
    }

    // the response handler is not invoked from the event handler thread,
    // so it is not safe to update state here, instead, schedule an event
    // to continue handling the response on the event handler thread
    private class BrokerHeartbeatResponseHandler implements ControllerRequestCompletionHandler {

        private final Iterable<Uuid> currentOfflineDirs;

        BrokerHeartbeatResponseHandler(Iterable<Uuid> currentOfflineDirs) {
            this.currentOfflineDirs = currentOfflineDirs;
        }

        @Override
        public void onComplete(ClientResponse response) {
            eventQueue.prepend(new BrokerHeartbeatResponseEvent(response, false, currentOfflineDirs));
        }

        @Override
        public void onTimeout() {
            logger.info("Unable to send a heartbeat because the RPC got timed out before it could be sent.");
            eventQueue.prepend(new BrokerHeartbeatResponseEvent(null, true, currentOfflineDirs));
        }
    }

    private class BrokerHeartbeatResponseEvent implements EventQueue.Event {

        private final ClientResponse response;
        private final boolean timedOut;
        private final Iterable<Uuid> currentOfflineDirs;

        BrokerHeartbeatResponseEvent(ClientResponse response, boolean timedOut, Iterable<Uuid> currentOfflineDirs) {
            this.response = response;
            this.timedOut = timedOut;
            this.currentOfflineDirs = currentOfflineDirs;
        }

        @Override
        public void run() {
            communicationInFlight = false;
            if (timedOut) {
                scheduleNextCommunicationAfterFailure();
                return;
            }
            if (response.authenticationException() != null) {
                logger.error("Unable to send broker heartbeat for {} because of an authentication exception.",
                        nodeId, response.authenticationException());
                scheduleNextCommunicationAfterFailure();
            } else if (response.versionMismatch() != null) {
                logger.error("Unable to send broker heartbeat for {} because of an API version problem.", nodeId, response.versionMismatch());
                scheduleNextCommunicationAfterFailure();
            } else if (response.responseBody() == null) {
                logger.warn("Unable to send broker heartbeat for {}. Retrying.", nodeId);
                scheduleNextCommunicationAfterFailure();
            } else if (!(response.responseBody() instanceof BrokerHeartbeatResponse message)) {
                logger.error("Unable to send broker heartbeat for {} because the controller returned an invalid response type.", nodeId);
                scheduleNextCommunicationAfterFailure();
            } else {
                Errors errorCode = Errors.forCode(message.data().errorCode());
                if (errorCode == Errors.NONE) {
                    BrokerHeartbeatResponseData responseData = message.data();
                    currentOfflineDirs.forEach(cur -> offlineDirs.put(cur, true));
                    switch (state) {
                        case STARTING -> {
                            if (responseData.isCaughtUp()) {
                                logger.info("The broker has caught up. Transitioning from STARTING to RECOVERY.");
                                state = BrokerState.RECOVERY;
                                initialCatchUpFuture.complete(null);
                            } else {
                                logger.debug("The broker is STARTING. Still waiting to catch up with cluster metadata.");
                            }
                            // Schedule the heartbeat after only 10 ms so that in the case where
                            // there is no recovery work to be done, we start up a bit quicker.
                            scheduleNextCommunication(NANOSECONDS.convert(10, MILLISECONDS));
                        }
                        case RECOVERY -> {
                            if (!responseData.isFenced()) {
                                logger.info("The broker has been unfenced. Transitioning from RECOVERY to RUNNING.");
                                initialUnfenceFuture.complete(null);
                                state = BrokerState.RUNNING;
                            } else {
                                logger.info("The broker is in RECOVERY.");
                            }
                            scheduleNextCommunicationAfterSuccess();
                        }
                        case RUNNING -> {
                            logger.debug("The broker is RUNNING. Processing heartbeat response.");
                            scheduleNextCommunicationAfterSuccess();
                        }
                        case PENDING_CONTROLLED_SHUTDOWN -> {
                            if (!responseData.shouldShutDown()) {
                                logger.info("The broker is in PENDING_CONTROLLED_SHUTDOWN state, still waiting " +
                                        "for the active controller.");
                                if (!gotControlledShutdownResponse) {
                                    // If this is the first pending controlled shutdown response we got,
                                    // schedule our next heartbeat a little bit sooner than we usually would.
                                    // In the case where controlled shutdown completes quickly, this will
                                    // speed things up a little bit.
                                    scheduleNextCommunication(NANOSECONDS.convert(50, MILLISECONDS));
                                } else {
                                    scheduleNextCommunicationAfterSuccess();
                                }
                            } else {
                                logger.info("The controller has asked us to exit controlled shutdown.");
                                beginShutdown();
                            }
                            gotControlledShutdownResponse = true;
                        }
                        case SHUTTING_DOWN -> logger.info("The broker is SHUTTING_DOWN. Ignoring heartbeat response.");
                        default -> {
                            logger.error("Unexpected broker state {}", state);
                            scheduleNextCommunicationAfterSuccess();
                        }
                    }
                } else {
                    logger.warn("Broker {} sent a heartbeat request but received error {}.", nodeId, errorCode);
                    scheduleNextCommunicationAfterFailure();
                }
            }
        }
    }

    private void scheduleNextCommunicationImmediately() {
        scheduleNextCommunication(0);
    }

    private void scheduleNextCommunicationAfterFailure() {
        nextSchedulingShouldBeImmediate = false; // never immediately reschedule after a failure
        scheduleNextCommunication(NANOSECONDS.convert(config.brokerHeartbeatIntervalMs(), MILLISECONDS));
    }

    private void scheduleNextCommunicationAfterSuccess() {
        scheduleNextCommunication(NANOSECONDS.convert(config.brokerHeartbeatIntervalMs(), MILLISECONDS));
    }

    private void scheduleNextCommunication(long intervalNs) {
        long adjustedIntervalNs = nextSchedulingShouldBeImmediate ? 0 : intervalNs;
        nextSchedulingShouldBeImmediate = false;
        logger.trace("Scheduling next communication at {}ms from now.", MILLISECONDS.convert(adjustedIntervalNs, NANOSECONDS));
        long deadlineNs = time.nanoseconds() + adjustedIntervalNs;
        eventQueue.scheduleDeferred("communication", new EventQueue.DeadlineFunction(deadlineNs), new CommunicationEvent());
    }

    private class RegistrationTimeoutEvent implements EventQueue.Event {
        @Override
        public void run() {
            if (!initialRegistrationSucceeded) {
                logger.error("Shutting down because we were unable to register with the controller quorum.");
                eventQueue.beginShutdown("registrationTimeout");
            }
        }
    }

    private class CommunicationEvent implements EventQueue.Event {
        @Override
        public void run() {
            if (communicationInFlight) {
                logger.trace("Delaying communication because there is already one in flight.");
                nextSchedulingShouldBeImmediate = true;
            } else if (registered) {
                sendBrokerHeartbeat();
            } else {
                sendBrokerRegistration();
            }
        }
    }

    private class ShutdownEvent implements EventQueue.Event {
        @Override
        public void run() throws InterruptedException {
            logger.info("Transitioning from {} to {}.", state, BrokerState.SHUTTING_DOWN);
            state = BrokerState.SHUTTING_DOWN;
            controlledShutdownFuture.complete(null);
            initialCatchUpFuture.cancel(false);
            initialUnfenceFuture.cancel(false);
            if (channelManager != null) {
                channelManager.shutdown();
                channelManager = null;
            }
        }
    }
}
