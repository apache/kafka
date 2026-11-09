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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.ApiVersions;
import org.apache.kafka.clients.ClientRequest;
import org.apache.kafka.clients.GroupRebalanceConfig;
import org.apache.kafka.clients.LeastLoadedNode;
import org.apache.kafka.clients.MockClient;
import org.apache.kafka.clients.consumer.CloseOptions;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.RebalanceConsumer;
import org.apache.kafka.clients.consumer.RebalanceListener;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.ApplicationEventProcessor;
import org.apache.kafka.clients.consumer.internals.events.ApplyAssignmentEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.AsyncPollEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEvent;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.events.CompletableApplicationEvent;
import org.apache.kafka.clients.consumer.internals.events.CompletableEventReaper;
import org.apache.kafka.clients.consumer.internals.events.ConsumerRebalanceListenerCallbackCompletedEvent;
import org.apache.kafka.clients.consumer.internals.events.ErrorEvent;
import org.apache.kafka.clients.consumer.internals.events.LeaveGroupOnCloseEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsAssignedEvent;
import org.apache.kafka.clients.consumer.internals.events.PartitionsRemovedEvent;
import org.apache.kafka.clients.consumer.internals.events.SyncCommitEvent;
import org.apache.kafka.clients.consumer.internals.events.TopicSubscriptionChangeEvent;
import org.apache.kafka.clients.consumer.internals.events.UnsubscribeEvent;
import org.apache.kafka.clients.consumer.internals.metrics.AsyncConsumerMetrics;
import org.apache.kafka.clients.consumer.internals.metrics.RebalanceCallbackMetricsManager;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.internals.ClusterResourceListeners;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.requests.MetadataResponse;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.internals.LogContext;

import java.lang.reflect.Field;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Supplier;

import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP;
import static org.apache.kafka.clients.consumer.internals.ConsumerUtils.CONSUMER_METRIC_GROUP_PREFIX;

/**
 * A consumer driven step by step by the heartbeat simulation.
 * <p>
 * The network thread side is the real code of the async consumer: the request managers built by
 * {@link RequestManagers#supplier}, the {@link ApplicationEventProcessor} and the
 * {@link NetworkClientDelegate}, over a {@link MockClient} whose requests the simulation routes
 * to its brokers. {@link #networkThreadRunOnce()} runs {@link ConsumerNetworkThread#runOnce()}
 * where the real consumer would loop in its own thread.
 * <p>
 * The application thread is modeled after {@link AsyncKafkaConsumer} because its blocking calls
 * cannot run on a single thread: {@code poll} (checkInflightPoll and processBackgroundEvents),
 * {@code subscribe}, {@code unsubscribe}, {@code commitSync}, {@code commitAsync} and
 * {@code close}. A blocking call becomes a wait that the simulation resumes on a later step of the
 * application thread.
 * <p>
 * The member id is set by the simulation instead of being drawn by the membership manager, so
 * that a seed reproduces the same run.
 */
public class SimConsumer {

    /**
     * Hooks into the simulation.
     */
    public interface Hooks {
        /** The application thread added an event: the real handler wakes up the network thread. */
        void wakeupNetworkThread(SimConsumer consumer);

        /** A rebalance callback started (assigned) or completed (revoked, lost). */
        void onCallback(SimConsumer consumer, String method, Set<TopicPartition> partitions);

        /** How long the next user callback runs. */
        long callbackDurationMs(SimConsumer consumer, String method);

        /** How many records poll returns for a fetchable partition. */
        int recordsPerPoll(SimConsumer consumer, TopicPartition partition);

        /**
         * The application thread applies the assignment of a reconciliation that is not current
         * anymore: the member is not reconciling or it rejoined the group since.
         */
        void onStaleAssignmentApplied(SimConsumer consumer, Set<TopicPartition> assigned, Set<TopicPartition> added, String reason);

        /** Poll returned records: the positions after them, by partition. */
        void onRecords(SimConsumer consumer, Map<TopicPartition, Long> positions);

        /** A commit call returned (commitSync) or completed (commitAsync), with an error or not. */
        void onCommitCompleted(SimConsumer consumer, String call, Map<TopicPartition, OffsetAndMetadata> offsets, Throwable error);
    }

    private final String name;
    private final Time time;
    private final Hooks hooks;
    private final MockClient client;
    private final ConsumerConfig config;
    private final ConsumerSubscriptionState subscriptions;
    private final ConsumerMetadata metadata;
    private final BlockingQueue<ApplicationEvent> applicationEventQueue;
    private final BackgroundEventHandler backgroundEventHandler;
    private final ConsumerNetworkThread networkThread;
    private final RequestManagers requestManagers;
    private final NetworkClientDelegate networkClientDelegate;
    private final ConsumerMembershipManager membershipManager;
    private final ConsumerRebalanceListenerInvoker rebalanceListenerInvoker;
    private final OffsetCommitCallbackInvoker offsetCommitCallbackInvoker;
    private final long defaultApiTimeoutMs;
    private final Set<ClientRequest> seenRequests = new HashSet<>();

    // State of the application thread.
    private AsyncPollEvent inflightPoll;
    private boolean newlySubmittedPoll;
    private CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> lastPendingAsyncCommit;
    private final Deque<BackgroundEvent> pendingBackgroundEvents = new ArrayDeque<>();
    private boolean skipAssignmentEvents;
    private KafkaException firstError;
    private Wait wait;
    private boolean lastWaitTimedOut;
    private Operation operation;
    private final List<String> errors = new ArrayList<>();

    // What the application observes through the member state listener of AsyncKafkaConsumer.
    private int applicationMemberEpoch = -1;
    private Set<TopicPartition> groupAssignmentSnapshot = Set.of();

    // The number of times the member left the group or was fenced, and its value when each
    // pending PartitionsAssignedEvent was queued.
    private int membershipResets;
    private final Map<BackgroundEvent, Integer> membershipResetsOfAssignmentEvents = new IdentityHashMap<>();

    // Partitions owned according to the rebalance callbacks: from the start of onPartitionsAssigned
    // until the end of onPartitionsRevoked or onPartitionsLost.
    private final Set<TopicPartition> ownedByCallbacks = new LinkedHashSet<>();
    private final Set<TopicPartition> releasing = new LinkedHashSet<>();

    private boolean closed;

    /**
     * A wait of the application thread: a future to complete or a time to reach.
     */
    private static final class Wait {
        final CompletableFuture<?> future;
        final long untilMs;
        final Runnable continuation;

        Wait(CompletableFuture<?> future, long untilMs, Runnable continuation) {
            this.future = future;
            this.untilMs = untilMs;
            this.continuation = continuation;
        }
    }

    /**
     * A call of the user into the consumer.
     */
    private abstract static class Operation {
        final String description;

        Operation(String description) {
            this.description = description;
        }

        /** Runs one step of the operation, returns true when the call returns. */
        abstract boolean step();
    }

    public SimConsumer(
        String name,
        String memberId,
        Time time,
        Map<String, Object> configs,
        MetadataResponse bootstrapMetadata,
        Hooks hooks
    ) {
        this.name = name;
        this.time = time;
        this.hooks = hooks;

        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, name);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.putAll(configs);
        this.config = new ConsumerConfig(props);
        this.defaultApiTimeoutMs = config.getInt(ConsumerConfig.DEFAULT_API_TIMEOUT_MS_CONFIG);

        LogContext logContext = new LogContext("[Consumer " + name + "] ");
        Metrics metrics = new Metrics(time);
        this.subscriptions = new ConsumerSubscriptionState(logContext, AutoOffsetResetStrategy.EARLIEST);
        this.metadata = new ConsumerMetadata(config, subscriptions, logContext, new ClusterResourceListeners());
        this.client = new MockClient(time, metadata) {
            // The metadata lists the nodes in a random order (Cluster shuffles them), so the first
            // one is not the same from one run to the next. The node with the lowest id that is
            // not backing off is chosen instead, so that a seed reproduces the same run.
            @Override
            public LeastLoadedNode leastLoadedNode(long now) {
                return metadata.fetch().nodes().stream()
                    .sorted(Comparator.comparingInt(Node::id))
                    .filter(node -> !connectionFailed(node))
                    .findFirst()
                    .map(node -> new LeastLoadedNode(node, true))
                    .orElse(new LeastLoadedNode(null, false));
            }
        };
        this.client.updateMetadata(bootstrapMetadata);

        // Mirrors the test constructor of AsyncKafkaConsumer that takes a KafkaClient.
        AsyncConsumerMetrics asyncConsumerMetrics = new AsyncConsumerMetrics(metrics, CONSUMER_METRIC_GROUP);
        this.applicationEventQueue = new LinkedBlockingQueue<>();
        this.backgroundEventHandler = new BackgroundEventHandler(
            new LinkedBlockingQueue<>() {
                @Override
                public boolean add(BackgroundEvent event) {
                    if (event.type() == BackgroundEvent.Type.PARTITIONS_ASSIGNED) {
                        membershipResetsOfAssignmentEvents.put(event, membershipResets);
                    }
                    return super.add(event);
                }
            },
            time,
            asyncConsumerMetrics
        );
        FetchBuffer fetchBuffer = new FetchBuffer(logContext);
        FetchMetricsManager fetchMetricsManager = new FetchMetricsManager(
            metrics,
            new FetchMetricsRegistry(CONSUMER_METRIC_GROUP_PREFIX)
        );
        PositionsValidator positionsValidator = new PositionsValidator(logContext, time, subscriptions, metadata);
        Supplier<NetworkClientDelegate> networkClientDelegateSupplier = NetworkClientDelegate.supplier(
            time,
            config,
            logContext,
            client,
            metadata,
            backgroundEventHandler,
            false,
            asyncConsumerMetrics
        );
        this.offsetCommitCallbackInvoker = new OffsetCommitCallbackInvoker(
            new ConsumerInterceptors<>(List.of(), metrics)
        );
        Supplier<RequestManagers> requestManagersSupplier = RequestManagers.supplier(
            time,
            logContext,
            backgroundEventHandler,
            metadata,
            subscriptions,
            fetchBuffer,
            config,
            new GroupRebalanceConfig(config, GroupRebalanceConfig.ProtocolType.CONSUMER),
            new ApiVersions(),
            fetchMetricsManager,
            networkClientDelegateSupplier,
            Optional.empty(),
            metrics,
            offsetCommitCallbackInvoker,
            new MemberStateListener() {
                // Mirrors the member state listener of AsyncKafkaConsumer.
                @Override
                public void onMemberEpochUpdated(Optional<Integer> memberEpoch, String memberId) {
                    applicationMemberEpoch = memberEpoch.orElse(-1);
                    // The epoch is empty when the member leaves the group or is fenced.
                    if (memberEpoch.isEmpty()) membershipResets++;
                }

                @Override
                public void onGroupAssignmentUpdated(Set<TopicPartition> partitions) {
                    groupAssignmentSnapshot = Set.copyOf(partitions);
                }
            },
            Optional.empty(),
            positionsValidator
        );
        Supplier<ApplicationEventProcessor> applicationEventProcessorSupplier = ApplicationEventProcessor.supplier(
            logContext,
            metadata,
            subscriptions,
            requestManagersSupplier
        );
        this.networkThread = new ConsumerNetworkThread(
            logContext,
            time,
            applicationEventQueue,
            new CompletableEventReaper(logContext),
            applicationEventProcessorSupplier,
            networkClientDelegateSupplier,
            requestManagersSupplier,
            asyncConsumerMetrics
        );
        this.networkThread.initializeResources();
        this.requestManagers = requestManagersSupplier.get();
        this.networkClientDelegate = networkClientDelegateSupplier.get();
        this.membershipManager = requestManagers.consumerMembershipManager.orElseThrow();
        setMemberId(membershipManager, memberId);

        this.rebalanceListenerInvoker = new ConsumerRebalanceListenerInvoker(
            logContext,
            subscriptions,
            time,
            new RebalanceCallbackMetricsManager(metrics)
        );
        this.subscriptions.setRebalanceListener(new RecordingListener(), new MockConsumer<>("earliest"));
    }

    /**
     * Replaces the member id that the membership manager drew with {@code Uuid.randomUuid()}. The
     * coordinator orders members by member id in places, so a seed reproduces its run only if the
     * member ids are drawn from the seed too. The field is final and has no setter.
     */
    private static void setMemberId(AbstractMembershipManager<?, ?> manager, String memberId) {
        try {
            Field field = AbstractMembershipManager.class.getDeclaredField("memberId");
            field.setAccessible(true);
            field.set(manager, memberId);
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    private final class RecordingListener implements RebalanceListener {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
            releasing.addAll(partitions);
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
            ownedByCallbacks.addAll(partitions);
            hooks.onCallback(SimConsumer.this, "onPartitionsAssigned", Set.copyOf(partitions));
        }

        @Override
        public void onPartitionsLost(Collection<TopicPartition> partitions, RebalanceConsumer consumer) {
            releasing.addAll(partitions);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Network thread.
    // ---------------------------------------------------------------------------------------------

    /**
     * Runs one iteration of the network thread loop. Unlike the real thread, which logs and
     * swallows them, exceptions propagate to the simulation.
     */
    public void networkThreadRunOnce() {
        if (closed) return;
        networkThread.runOnce();
    }

    public MockClient client() {
        return client;
    }

    /**
     * @return The requests sent by the network client delegate since the last call.
     */
    public List<ClientRequest> newRequests() {
        List<ClientRequest> result = new ArrayList<>();
        for (ClientRequest request : client.requests()) {
            if (seenRequests.add(request)) result.add(request);
        }
        seenRequests.retainAll(client.requests());
        return result;
    }

    public void updateMetadata(MetadataResponse response) {
        client.updateMetadata(response);
    }

    // ---------------------------------------------------------------------------------------------
    // Application thread.
    // ---------------------------------------------------------------------------------------------

    private void addApplicationEvent(ApplicationEvent event) {
        // ApplicationEventHandler#add
        event.setEnqueuedMs(time.milliseconds());
        applicationEventQueue.add(event);
        hooks.wakeupNetworkThread(this);
    }

    private <T> void addAndGet(CompletableApplicationEvent<T> event, Runnable continuation) {
        // ApplicationEventHandler#addAndGet blocks until the network thread completes the event.
        addApplicationEvent(event);
        waitFor(event.future(), event.deadlineMs(), continuation);
    }

    private void waitFor(CompletableFuture<?> future, long untilMs, Runnable continuation) {
        if (wait != null) throw new IllegalStateException("Already waiting");
        wait = new Wait(future, untilMs, continuation);
    }

    private void sleep(long durationMs, Runnable continuation) {
        waitFor(null, time.milliseconds() + durationMs, continuation);
    }

    /**
     * @return True if the application thread is in a call into the consumer.
     */
    public boolean inCall() {
        return operation != null;
    }

    public String currentCall() {
        return operation == null ? "none" : operation.description;
    }

    /**
     * Runs one step of the application thread: resumes a wait if it is over, then the current
     * call. Returns true if the call returned.
     */
    public boolean applicationThreadStep() {
        if (closed && operation == null) return true;
        if (wait != null) {
            if (wait.future != null) {
                if (!wait.future.isDone()) {
                    if (time.milliseconds() < wait.untilMs) return false;
                    // ConsumerUtils#getResult throws a TimeoutException.
                    Wait timedOut = wait;
                    wait = null;
                    lastWaitTimedOut = true;
                    timedOut.continuation.run();
                    lastWaitTimedOut = false;
                    if (wait != null) return false;
                    return finishStep();
                }
            } else if (time.milliseconds() < wait.untilMs) {
                return false;
            }
            Wait done = wait;
            wait = null;
            done.continuation.run();
            if (wait != null) return false;
        }
        return finishStep();
    }

    private boolean finishStep() {
        if (operation == null) return true;
        if (operation.step()) {
            operation = null;
            return true;
        }
        return false;
    }

    private void start(Operation op) {
        if (operation != null) throw new IllegalStateException("Call " + operation.description + " in progress");
        if (closed) throw new IllegalStateException("Closed");
        operation = op;
    }

    /**
     * consumer.subscribe(topics): AsyncKafkaConsumer#subscribeInternal.
     */
    public void subscribe(Set<String> topics) {
        start(new Operation("subscribe" + new TreeSet<>(topics)) {
            boolean started;
            boolean done;

            @Override
            boolean step() {
                if (!started) {
                    started = true;
                    addAndGet(
                        new TopicSubscriptionChangeEvent(new HashSet<>(topics), time.milliseconds() + defaultApiTimeoutMs),
                        () -> {
                            if (lastWaitTimedOut) recordError("subscribe", new KafkaException("Timed out"));
                            done = true;
                        }
                    );
                    return false;
                }
                return done;
            }
        });
    }

    /**
     * consumer.poll(timeout): AsyncKafkaConsumer#poll, repeated iterations of checkInflightPoll
     * until the timeout. The poll returns no records because the fetches return none, but the
     * positions of the fetchable partitions move forward as if the records were consumed.
     */
    public void poll(long timeoutMs) {
        long deadlineMs = time.milliseconds() + timeoutMs;
        start(new Operation("poll(" + timeoutMs + ")") {
            boolean firstPass = true;
            boolean iterating;

            @Override
            boolean step() {
                if (!iterating) {
                    // The do/while loop of poll: one iteration at least, then until the timer expires.
                    if (!firstPass && time.milliseconds() >= deadlineMs) {
                        consumeRecords();
                        return true;
                    }
                    if (!startCheckInflightPoll(firstPass, deadlineMs)) return true;
                    firstPass = false;
                    iterating = true;
                }
                if (!continueProcessBackgroundEvents()) return false;
                iterating = false;
                if (!finishCheckInflightPoll()) return true;
                // pollForFetches waits until the next step.
                if (time.milliseconds() >= deadlineMs) {
                    consumeRecords();
                    return true;
                }
                return false;
            }
        });
    }

    /**
     * First half of AsyncKafkaConsumer#checkInflightPoll, up to processBackgroundEvents.
     *
     * @return False if poll throws.
     */
    private boolean startCheckInflightPoll(boolean firstPass, long deadlineMs) {
        if (inflightPoll != null && (firstPass || inflightPoll.isComplete())) {
            if (!maybeClearPreviousInflightPoll()) return false;
        }
        newlySubmittedPoll = false;
        if (inflightPoll == null) {
            inflightPoll = new AsyncPollEvent(deadlineMs, time.milliseconds());
            newlySubmittedPoll = true;
            addApplicationEvent(inflightPoll);
        }
        offsetCommitCallbackInvoker.executeCallbacks();
        startProcessBackgroundEvents(false);
        return true;
    }

    /**
     * Second half of AsyncKafkaConsumer#checkInflightPoll.
     *
     * @return False if poll throws.
     */
    private boolean finishCheckInflightPoll() {
        if (firstError != null) {
            KafkaException error = firstError;
            firstError = null;
            inflightPoll = null;
            recordError("poll", error);
            return false;
        }
        if (inflightPoll != null) {
            if (inflightPoll.isComplete()) {
                Optional<KafkaException> error = inflightPoll.error();
                inflightPoll = null;
                if (error.isPresent()) {
                    recordError("poll", error.get());
                    return false;
                }
            } else if (!newlySubmittedPoll && inflightPoll.isExpired(time) && inflightPoll.isValidatePositionsComplete()) {
                inflightPoll = null;
            }
        }
        return true;
    }

    private boolean maybeClearPreviousInflightPoll() {
        if (inflightPoll.isComplete()) {
            Optional<KafkaException> error = inflightPoll.error();
            // The fetch buffer is always empty in the simulation.
            inflightPoll = null;
            if (error.isPresent()) {
                recordError("poll", error.get());
                return false;
            }
        } else if (inflightPoll.isExpired(time) && inflightPoll.isValidatePositionsComplete()) {
            inflightPoll = null;
        }
        return true;
    }

    private void startProcessBackgroundEvents(boolean skipAssignmentEvents) {
        this.skipAssignmentEvents = skipAssignmentEvents;
        pendingBackgroundEvents.addAll(backgroundEventHandler.drainEvents());
    }

    /**
     * AsyncKafkaConsumer#processBackgroundEvents(boolean) and its BackgroundEventProcessor,
     * resumable after the blocking addAndGet of the ApplyAssignmentEvent and after a callback.
     *
     * @return True when all the drained events are processed.
     */
    private boolean continueProcessBackgroundEvents() {
        while (!pendingBackgroundEvents.isEmpty()) {
            if (wait != null) return false;
            BackgroundEvent event = pendingBackgroundEvents.poll();
            try {
                Integer resetsWhenQueued = membershipResetsOfAssignmentEvents.remove(event);
                if (skipAssignmentEvents && event.type() == BackgroundEvent.Type.PARTITIONS_ASSIGNED) {
                    ((PartitionsAssignedEvent) event).future().completeExceptionally(
                        new KafkaException("Assignment event skipped because consumer is unsubscribing"));
                    continue;
                }
                switch (event.type()) {
                    case ERROR:
                        throw ((ErrorEvent) event).error();
                    case PARTITIONS_ASSIGNED:
                        processPartitionsAssigned((PartitionsAssignedEvent) event, resetsWhenQueued);
                        break;
                    case PARTITIONS_REMOVED:
                        PartitionsRemovedEvent removed = (PartitionsRemovedEvent) event;
                        invokeRebalanceCallbackAndNotifyBackgroundThread(
                            removed.methodName(), removed.partitions(), removed.future());
                        break;
                    default:
                        throw new IllegalArgumentException("Background event type " + event.type() + " was not expected");
                }
            } catch (Throwable t) {
                KafkaException e = ConsumerUtils.maybeWrapAsKafkaException(t);
                if (firstError == null) firstError = e;
            }
        }
        return wait == null;
    }

    private void processPartitionsAssigned(PartitionsAssignedEvent event, int resetsWhenQueued) {
        String staleReason = null;
        if (membershipManager.state() != MemberState.RECONCILING) {
            staleReason = "member is " + membershipManager.state();
        } else if (!membershipManager.reconciliationInProgress()) {
            staleReason = "no reconciliation in progress";
        } else if (membershipResets != resetsWhenQueued) {
            staleReason = "member left the group or was fenced and rejoined";
        }
        if (staleReason != null) {
            hooks.onStaleAssignmentApplied(this, event.assignedPartitions(), event.addedPartitions(), staleReason);
        }
        // applyNewAssignment: addAndGet(ApplyAssignmentEvent), then the callback.
        ApplyAssignmentEvent applyEvent = new ApplyAssignmentEvent(event.assignedPartitions(), event.addedPartitions());
        addAndGet(applyEvent, () -> {
            if (lastWaitTimedOut || applyEvent.future().isCompletedExceptionally()) {
                KafkaException error = new KafkaException("Failed to apply the new assignment");
                addApplicationEvent(new ConsumerRebalanceListenerCallbackCompletedEvent(
                    ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED, event.future(), Optional.of(error)));
                if (firstError == null) firstError = error;
                return;
            }
            if (!subscriptions.hasRebalanceListener()) {
                event.future().complete(null);
            } else {
                invokeRebalanceCallbackAndNotifyBackgroundThread(
                    ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED, event.addedPartitions(), event.future());
            }
        });
    }

    private void invokeRebalanceCallbackAndNotifyBackgroundThread(
        ConsumerRebalanceListenerMethodName methodName,
        SortedSet<TopicPartition> partitions,
        CompletableFuture<Void> future
    ) {
        ConsumerRebalanceListenerCallbackCompletedEvent invokedEvent = AsyncKafkaConsumer.invokeRebalanceCallbacks(
            rebalanceListenerInvoker,
            methodName,
            partitions,
            future
        );
        // The user code of the callback runs for a while before it returns.
        long durationMs = hooks.callbackDurationMs(this, methodName.fullyQualifiedMethodName());
        Runnable callbackReturns = () -> {
            if (methodName != ConsumerRebalanceListenerMethodName.ON_PARTITIONS_ASSIGNED) {
                Set<TopicPartition> released = new LinkedHashSet<>(releasing);
                ownedByCallbacks.removeAll(released);
                releasing.clear();
                hooks.onCallback(this, methodName == ConsumerRebalanceListenerMethodName.ON_PARTITIONS_REVOKED ?
                    "onPartitionsRevoked" : "onPartitionsLost", released);
            }
            addApplicationEvent(invokedEvent);
            if (invokedEvent.error().isPresent() && firstError == null) firstError = invokedEvent.error().get();
        };
        if (durationMs > 0) {
            sleep(durationMs, callbackReturns);
        } else {
            callbackReturns.run();
        }
    }

    private void consumeRecords() {
        Map<TopicPartition, Long> positions = new HashMap<>();
        for (TopicPartition tp : subscriptions.fetchablePartitions(tp -> true)) {
            ConsumerSubscriptionState.FetchPosition position = subscriptions.position(tp);
            if (position == null) continue;
            int records = hooks.recordsPerPoll(this, tp);
            if (records <= 0) continue;
            subscriptions.position(tp, new ConsumerSubscriptionState.FetchPosition(
                position.offset + records,
                position.offsetEpoch,
                position.currentLeader
            ));
            positions.put(tp, position.offset + records);
        }
        if (!positions.isEmpty()) hooks.onRecords(this, positions);
    }

    // ---------------------------------------------------------------------------------------------
    // Commits.
    // ---------------------------------------------------------------------------------------------

    /**
     * consumer.commitSync() or consumer.commitSync(offsets): AsyncKafkaConsumer#commitSync. The
     * application thread waits for the offsets to be ready, for the pending async commit, then
     * for the commit, without processing the background events.
     */
    public void commitSync(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets, long timeoutMs) {
        String call = offsets.isPresent() ? "commitSync(" + offsets.get() + ")" : "commitSync()";
        start(new Operation(call) {
            int phase;
            long deadlineMs;
            SyncCommitEvent event;

            @Override
            boolean step() {
                if (phase == 0) {
                    deadlineMs = time.milliseconds() + timeoutMs;
                    // AsyncKafkaConsumer#commit
                    offsetCommitCallbackInvoker.executeCallbacks();
                    if (offsets.isPresent() && offsets.get().isEmpty()) return true;
                    event = new SyncCommitEvent(offsets, deadlineMs);
                    addApplicationEvent(event);
                    phase = 1;
                    waitFor(event.offsetsReady(), time.milliseconds() + defaultApiTimeoutMs, () -> { });
                    return false;
                }
                if (phase == 1) {
                    phase = 2;
                    // awaitPendingAsyncCommitsAndExecuteCommitCallbacks
                    if (lastPendingAsyncCommit != null) {
                        CompletableFuture<Map<TopicPartition, OffsetAndMetadata>> pending = lastPendingAsyncCommit;
                        waitFor(pending, deadlineMs, () -> {
                            if (pending.isDone()) lastPendingAsyncCommit = null;
                            offsetCommitCallbackInvoker.executeCallbacks();
                        });
                        return false;
                    }
                    offsetCommitCallbackInvoker.executeCallbacks();
                }
                if (phase == 2) {
                    phase = 3;
                    waitFor(event.future(), deadlineMs, () -> { });
                    return false;
                }
                Throwable error = null;
                if (!event.future().isDone()) {
                    error = new TimeoutException("commitSync timed out");
                } else if (event.future().isCompletedExceptionally()) {
                    try {
                        event.future().join();
                    } catch (Exception e) {
                        error = e.getCause() != null ? e.getCause() : e;
                    }
                }
                hooks.onCommitCompleted(SimConsumer.this, "commitSync", offsets.orElse(Map.of()), error);
                return true;
            }
        });
    }

    /**
     * consumer.commitAsync() or consumer.commitAsync(offsets, callback): AsyncKafkaConsumer#commitAsync.
     * The application thread waits for the offsets to be ready; the result is reported when the
     * commit completes.
     */
    public void commitAsync(Optional<Map<TopicPartition, OffsetAndMetadata>> offsets) {
        String call = offsets.isPresent() ? "commitAsync(" + offsets.get() + ")" : "commitAsync()";
        start(new Operation(call) {
            AsyncCommitEvent event;

            @Override
            boolean step() {
                if (event == null) {
                    offsetCommitCallbackInvoker.executeCallbacks();
                    if (offsets.isPresent() && offsets.get().isEmpty()) return true;
                    event = new AsyncCommitEvent(offsets);
                    addApplicationEvent(event);
                    waitFor(event.offsetsReady(), time.milliseconds() + defaultApiTimeoutMs, () -> { });
                    return false;
                }
                lastPendingAsyncCommit = event.future().whenComplete((committed, error) -> {
                    Throwable cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
                    hooks.onCommitCompleted(SimConsumer.this, "commitAsync", offsets.orElse(committed == null ? Map.of() : committed), cause);
                });
                return true;
            }
        });
    }

    /**
     * consumer.unsubscribe(): AsyncKafkaConsumer#unsubscribe, waiting for the UnsubscribeEvent
     * while processing the background events without the assignment events.
     */
    public void unsubscribe() {
        start(new Operation("unsubscribe") {
            UnsubscribeEvent event;
            long deadlineMs;

            @Override
            boolean step() {
                if (event == null) {
                    deadlineMs = time.milliseconds() + defaultApiTimeoutMs;
                    event = new UnsubscribeEvent(deadlineMs);
                    addApplicationEvent(event);
                }
                return processBackgroundEventsUntil(event.future(), deadlineMs);
            }
        });
    }

    /**
     * AsyncKafkaConsumer#processBackgroundEvents(Future, Timer, Predicate, boolean) with the
     * assignment events skipped. Errors are ignored like unsubscribe and close ignore them.
     */
    private boolean processBackgroundEventsUntil(CompletableFuture<?> future, long deadlineMs) {
        if (pendingBackgroundEvents.isEmpty()) startProcessBackgroundEvents(true);
        if (!continueProcessBackgroundEvents()) return false;
        firstError = null;
        return future.isDone() || time.milliseconds() >= deadlineMs;
    }

    /**
     * consumer.close(): the group part of AsyncKafkaConsumer#close. The rebalance callbacks run
     * on the application thread, then the LeaveGroupOnCloseEvent, then the network thread
     * cleanup sends the leave heartbeat if it is still pending.
     */
    public void close(CloseOptions.GroupMembershipOperation membershipOperation, long timeoutMs) {
        start(new Operation("close(" + membershipOperation + ")") {
            int phase;
            long deadlineMs;
            LeaveGroupOnCloseEvent leaveEvent;

            @Override
            boolean step() {
                if (phase == 0) {
                    deadlineMs = time.milliseconds() + timeoutMs;
                    phase = 1;
                    if (runRebalanceCallbacksOnClose()) return false;
                }
                if (phase == 1) {
                    phase = 2;
                    leaveEvent = new LeaveGroupOnCloseEvent(deadlineMs, membershipOperation);
                    addApplicationEvent(leaveEvent);
                }
                if (phase == 2) {
                    // ApplicationEventHandler#addAndGet: the background events are not processed.
                    if (!leaveEvent.future().isDone() && time.milliseconds() < deadlineMs) return false;
                    phase = 3;
                }
                // ConsumerNetworkThread#cleanup
                ConsumerNetworkThread.runAtClose(requestManagers.entries(), networkClientDelegate, time.milliseconds());
                networkClientDelegate.poll(0, time.milliseconds());
                closed = true;
                return true;
            }
        });
    }

    /**
     * AsyncKafkaConsumer#runRebalanceCallbacksOnClose.
     *
     * @return True if the callback is running.
     */
    private boolean runRebalanceCallbacksOnClose() {
        Set<TopicPartition> assignedPartitions = groupAssignmentSnapshot;
        if (assignedPartitions.isEmpty()) return false;
        SortedSet<TopicPartition> droppedPartitions = new TreeSet<>(AbstractMembershipManager.TOPIC_PARTITION_COMPARATOR);
        droppedPartitions.addAll(assignedPartitions);
        String method;
        if (applicationMemberEpoch > 0) {
            rebalanceListenerInvoker.invokePartitionsRevoked(droppedPartitions);
            method = "onPartitionsRevoked";
        } else {
            rebalanceListenerInvoker.invokePartitionsLost(droppedPartitions);
            method = "onPartitionsLost";
        }
        long durationMs = hooks.callbackDurationMs(this, method);
        Runnable callbackReturns = () -> {
            Set<TopicPartition> released = new LinkedHashSet<>(releasing);
            ownedByCallbacks.removeAll(released);
            releasing.clear();
            hooks.onCallback(this, method, released);
        };
        if (durationMs > 0) {
            sleep(durationMs, callbackReturns);
            return true;
        }
        callbackReturns.run();
        return false;
    }

    /**
     * The process stops: no more requests or callbacks.
     */
    public void crash() {
        closed = true;
        operation = null;
        wait = null;
    }

    private void recordError(String call, KafkaException error) {
        errors.add(call + ": " + error.getClass().getSimpleName() + ": " + error.getMessage());
    }

    // ---------------------------------------------------------------------------------------------
    // Observations.
    // ---------------------------------------------------------------------------------------------

    public String name() {
        return name;
    }

    public boolean isClosed() {
        return closed;
    }

    public String memberId() {
        return membershipManager.memberId();
    }

    public Optional<String> instanceId() {
        return membershipManager.groupInstanceId();
    }

    public int memberEpoch() {
        return membershipManager.memberEpoch();
    }

    public MemberState state() {
        return membershipManager.state();
    }

    public Map<Uuid, SortedSet<Integer>> currentAssignment() {
        return membershipManager.currentAssignment().partitions;
    }

    public boolean reconciliationInProgress() {
        return membershipManager.reconciliationInProgress();
    }

    public Set<TopicPartition> assignedPartitions() {
        return subscriptions.assignedPartitions();
    }

    public Set<TopicPartition> ownedByCallbacks() {
        return Set.copyOf(ownedByCallbacks);
    }

    public Set<String> subscription() {
        return subscriptions.subscription();
    }

    /**
     * @return The errors thrown to the application since the last call.
     */
    public List<String> drainErrors() {
        List<String> drained = List.copyOf(errors);
        errors.clear();
        return drained;
    }

    public boolean waiting() {
        return wait != null;
    }

    public String networkState() {
        return "coordinator=" + requestManagers.coordinatorRequestManager.flatMap(CoordinatorRequestManager::coordinator)
            .map(n -> n.idString()).orElse("none") + " inflight=" + client.requests().size() +
            " unsent=" + networkClientDelegate.unsentRequests().size() + " pending-bg=" + pendingBackgroundEvents.size() +
            " call=" + currentCall() + " waiting=" + (wait != null);
    }

    @Override
    public String toString() {
        return name + "(" + memberId().substring(0, 4) + ", epoch=" + memberEpoch() + ", " + state() +
            ", assigned=" + assignedPartitions() + ", owned=" + ownedByCallbacks + ")";
    }
}
