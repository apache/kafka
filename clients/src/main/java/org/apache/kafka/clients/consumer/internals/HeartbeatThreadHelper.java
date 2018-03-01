package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

// TODO come up with a better name
public class HeartbeatThreadHelper {
    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";
    private final LogContext logContext;
    private final Logger log;
    private final String groupId;
    private final Heartbeat heartbeat;
    private final AbstractCoordinator coordinator;
    protected final Time time;
    protected final long retryBackoffMs;
    private HeartbeatThread heartbeatThread = null;

    public HeartbeatThreadHelper(LogContext logContext,
                                 String groupId,
                                 Heartbeat heartbeat,
                                 AbstractCoordinator coordinator,
                                 Time time,
                                 long retryBackoffMs) {
        this.logContext = logContext;
        this.log = logContext.logger(AbstractCoordinator.class);
        this.groupId = groupId;
        this.heartbeat = heartbeat;
        this.coordinator = coordinator;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
    }

    private class HeartbeatThread extends KafkaThread {
        private boolean enabled = false;
        private boolean closed = false;

        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        HeartbeatThread() {
            super(HEARTBEAT_THREAD_PREFIX + (groupId.isEmpty() ? "" : " | " + groupId), true);
        }

        public void enable() {
            log.debug("Enabling heartbeat thread");
            synchronized (coordinator) {
                this.enabled = true;
                heartbeat.resetTimeouts(time.milliseconds());
                coordinator.notify();
            }
        }

        public void disable() {
            log.debug("Disabling heartbeat thread");
            synchronized (coordinator) {
                this.enabled = false;
            }
        }

        public void close() {
            synchronized (coordinator) {
                this.closed = true;
                coordinator.notify();
            }
        }

        boolean hasFailed() {
            return failed.get() != null;
        }

        RuntimeException failureCause() {
            return failed.get();
        }

        @Override
        public void run() {
            try {
                log.debug("Heartbeat thread started");
                while (true) {
                    synchronized (coordinator) {
                        if (closed)
                            return;

                        if (!enabled) {
                            coordinator.wait();
                            continue;
                        }
                        if (!coordinator.isStable()) {
                            // the group is not stable (perhaps because we left the group or because the coordinator
                            // kicked us out), so disable heartbeats and wait for the main thread to rejoin.
                            disable();
                            continue;
                        }

                        coordinator.client.pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinator.coordinatorUnknown()) {
                            if (coordinator.findCoordinatorFuture() != null || coordinator.lookupCoordinator().failed())
                                // the immediate future check ensures that we backoff properly in the case that no
                                // brokers are available to connect to.
                                coordinator.wait(retryBackoffMs);
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            // the session timeout has expired without seeing a successful heartbeat, so we should
                            // probably make sure the coordinator is still healthy.
                            coordinator.markCoordinatorUnknown();
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            // the poll timeout has expired, which means that the foreground thread has stalled
                            // in between calls to poll(), so we explicitly leave the group.
                            coordinator.maybeLeaveGroup();
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            coordinator.wait(retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);

                            coordinator.sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (coordinator) {
                                        heartbeat.receiveHeartbeat(time.milliseconds());
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (coordinator) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat(time.milliseconds());
                                        } else {
                                            heartbeat.failHeartbeat();

                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            coordinator.notify();
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            } catch (AuthenticationException e) {
                log.error("An authentication error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (GroupAuthorizationException e) {
                log.error("A group authorization error occurred in the heartbeat thread", e);
                this.failed.set(e);
            } catch (InterruptedException | InterruptException e) {
                Thread.interrupted();
                log.error("Unexpected interrupt received in heartbeat thread", e);
                this.failed.set(new RuntimeException(e));
            } catch (Throwable e) {
                log.error("Heartbeat thread failed due to unexpected error", e);
                if (e instanceof RuntimeException)
                    this.failed.set((RuntimeException) e);
                else
                    this.failed.set(new RuntimeException(e));
            } finally {
                log.debug("Heartbeat thread has closed");
            }
        }
    }

    public void pollHeartbeat(long now) {
        if (heartbeatThread != null) {
            if (heartbeatThread.hasFailed()) {
                // set the heartbeat thread to null and raise an exception. If the user catches it,
                // the next call to ensureActiveGroup() will spawn a new heartbeat thread.
                RuntimeException cause = heartbeatThread.failureCause();
                heartbeatThread = null;
                throw cause;
            }
            // Awake the heartbeat thread if needed
            if (heartbeat.shouldHeartbeat(now)) {
                notify();
            }
            heartbeat.poll(now);
        }
    }

    public void startHeartbeatThreadIfNeeded() {
        if (heartbeatThread == null) {
            heartbeatThread = new HeartbeatThread();
            heartbeatThread.start();
        }
    }

    public void disableHeartbeatThread() {
        if (heartbeatThread != null)
            heartbeatThread.disable();
    }

    public void enableHeartbeatThread() {
        if (heartbeatThread != null)
            heartbeatThread.enable();
    }

    public void closeHeartbeatThread() {
        HeartbeatThread thread = null;
        synchronized (coordinator) {
            if (heartbeatThread == null)
                return;
            heartbeatThread.close();
            thread = heartbeatThread;
            heartbeatThread = null;
        }
        try {
            thread.join();
        } catch (InterruptedException e) {
            log.warn("Interrupted while waiting for consumer heartbeat thread to close");
            throw new InterruptException(e);
        }
    }
}
