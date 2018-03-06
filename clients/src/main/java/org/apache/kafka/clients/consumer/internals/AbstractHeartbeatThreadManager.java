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

import org.apache.kafka.common.errors.AuthenticationException;
import org.apache.kafka.common.errors.GroupAuthorizationException;
import org.apache.kafka.common.errors.InterruptException;
import org.apache.kafka.common.errors.RebalanceInProgressException;
import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.slf4j.Logger;

import java.util.concurrent.atomic.AtomicReference;

public abstract class AbstractHeartbeatThreadManager implements HeartbeatThreadManager {

    public static final String HEARTBEAT_THREAD_PREFIX = "kafka-coordinator-heartbeat-thread";
    private final Logger log;
    private final String heartbeatThreadName;
    private final Heartbeat heartbeat;
    private final Object lock;
    protected final Time time;
    protected final long retryBackoffMs;
    private HeartbeatThread heartbeatThread = null;

    public AbstractHeartbeatThreadManager(LogContext logContext,
                                          String groupId,
                                          Heartbeat heartbeat,
                                          Object lock,
                                          Time time,
                                          long retryBackoffMs) {
        this.log = logContext.logger(AbstractHeartbeatThreadManager.class);
        this.heartbeatThreadName = HEARTBEAT_THREAD_PREFIX + (groupId.isEmpty() ? "" : " | " + groupId);
        this.heartbeat = heartbeat;
        this.lock = lock;
        this.time = time;
        this.retryBackoffMs = retryBackoffMs;
    }

    private class HeartbeatThread extends KafkaThread {
        private boolean enabled = false;
        private boolean closed = false;

        private AtomicReference<RuntimeException> failed = new AtomicReference<>(null);

        HeartbeatThread() {
            super(heartbeatThreadName, true);
        }

        private void enable() {
            log.debug("Enabling heartbeat thread");
            this.enabled = true;
            heartbeat.resetTimeouts(time.milliseconds());
            lock.notify();
        }

        private void disable() {
            log.debug("Disabling heartbeat thread");
            this.enabled = false;
        }

        private boolean disabled() {
            return !enabled;
        }

        private void close() {
            this.closed = true;
            lock.notify();
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
                    synchronized (lock) {
                        if (closed)
                            return;

                        if (disabled()) {
                            lock.wait();
                            continue;
                        }

                        onHeartbeatThreadWakeup();
                        if (disabled()) {
                            continue;
                        }

                        pollNoWakeup();
                        long now = time.milliseconds();

                        if (coordinatorUnknown()) {
                            if (coordinatorUnavailable())
                                lock.wait(retryBackoffMs);
                        } else if (heartbeat.sessionTimeoutExpired(now)) {
                            onSessionTimeoutExpired();
                        } else if (heartbeat.pollTimeoutExpired(now)) {
                            onPollTimeoutExpired();
                        } else if (!heartbeat.shouldHeartbeat(now)) {
                            // poll again after waiting for the retry backoff in case the heartbeat failed or the
                            // coordinator disconnected
                            lock.wait(retryBackoffMs);
                        } else {
                            heartbeat.sentHeartbeat(now);

                            sendHeartbeatRequest().addListener(new RequestFutureListener<Void>() {
                                @Override
                                public void onSuccess(Void value) {
                                    synchronized (lock) {
                                        heartbeat.receiveHeartbeat(time.milliseconds());
                                    }
                                }

                                @Override
                                public void onFailure(RuntimeException e) {
                                    synchronized (lock) {
                                        if (e instanceof RebalanceInProgressException) {
                                            // it is valid to continue heartbeating while the group is rebalancing. This
                                            // ensures that the coordinator keeps the member in the group for as long
                                            // as the duration of the rebalance timeout. If we stop sending heartbeats,
                                            // however, then the session timeout may expire before we can rejoin.
                                            heartbeat.receiveHeartbeat(time.milliseconds());
                                        } else {
                                            heartbeat.failHeartbeat();

                                            // wake up the thread if it's sleeping to reschedule the heartbeat
                                            lock.notify();
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

    @Override
    public void pollHeartbeat(long now) {
        synchronized (lock) {
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
                    lock.notify();
                }
                heartbeat.poll(now);
            }
        }
    }

    @Override
    public void startHeartbeatThreadIfNeeded() {
        synchronized (lock) {
            if (heartbeatThread == null) {
                heartbeatThread = new HeartbeatThread();
                heartbeatThread.start();
            }
        }
    }

    @Override
    public void disableHeartbeatThread() {
        synchronized (lock) {
            if (heartbeatThread != null)
                heartbeatThread.disable();
        }
    }

    @Override
    public void enableHeartbeatThread() {
        synchronized (lock) {
            if (heartbeatThread != null)
                heartbeatThread.enable();
        }
    }

    @Override
    public void closeHeartbeatThread() {
        HeartbeatThread thread;
        synchronized (lock) {
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

    protected abstract void pollNoWakeup();

    protected abstract boolean coordinatorUnavailable();

    protected abstract boolean coordinatorUnknown();

    public abstract RequestFuture<Void> sendHeartbeatRequest();

    protected abstract void onHeartbeatThreadWakeup();

    protected abstract void onSessionTimeoutExpired();

    protected abstract void onPollTimeoutExpired();
}