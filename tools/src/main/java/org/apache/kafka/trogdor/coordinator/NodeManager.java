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

package org.apache.kafka.trogdor.coordinator;

import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.trogdor.agent.AgentClient;
import org.apache.kafka.trogdor.common.Node;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.fault.DoneState;
import org.apache.kafka.trogdor.fault.Fault;
import org.apache.kafka.trogdor.fault.SendingState;
import org.apache.kafka.trogdor.rest.AgentStatusResponse;
import org.apache.kafka.trogdor.rest.CreateAgentFaultRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

class NodeManager {
    private static final Logger log = LoggerFactory.getLogger(NodeManager.class);

    /**
     * The Time object used to fetch the current time.
     */
    private final Time time;

    /**
     * The node which is being managed.
     */
    private final Node node;

    /**
     * The client for the node being managed.
     */
    private final AgentClient client;

    /**
     * The maximum amount of time to go without contacting the node.
     */
    private final long heartbeatMs;

    /**
     * True if the NodeManager is shutting down.  Protected by the queueLock.
     */
    private boolean shutdown = false;

    /**
     * The Node Manager runnable.
     */
    private final NodeManagerRunnable runnable;

    /**
     * The Node Manager thread.
     */
    private final KafkaThread thread;

    /**
     * The lock protecting the NodeManager fields.
     */
    private final Lock lock = new ReentrantLock();

    /**
     * The condition variable used to wake the thread when it is waiting for a
     * queue or shutdown change.
     */
    private final Condition cond = lock.newCondition();

    /**
     * A queue of faults which should be sent to this node.  Protected by the lock.
     */
    private final List<Fault> faultQueue = new ArrayList<>();

    /**
     * The last time we successfully contacted the node.  Protected by the lock.
     */
    private long lastContactMs = 0;

    /**
     * The current status of this node.
     */
    public static class NodeStatus {
        private final String nodeName;
        private final long lastContactMs;

        NodeStatus(String nodeName, long lastContactMs) {
            this.nodeName = nodeName;
            this.lastContactMs = lastContactMs;
        }

        public String nodeName() {
            return nodeName;
        }

        public long lastContactMs() {
            return lastContactMs;
        }
    }

    class NodeManagerRunnable implements Runnable {
        @Override
        public void run() {
            try {
                Fault fault = null;
                long lastCommAttemptMs = 0;
                while (true) {
                    long now = time.milliseconds();
                    if (fault != null) {
                        lastCommAttemptMs = now;
                        if (sendFault(now, fault)) {
                            fault = null;
                        }
                    }
                    long nextCommAttemptMs = lastCommAttemptMs + heartbeatMs;
                    if (now < nextCommAttemptMs) {
                        lastCommAttemptMs = now;
                        sendHeartbeat(now);
                    }
                    long waitMs = Math.max(0L, nextCommAttemptMs - now);
                    lock.lock();
                    try {
                        if (shutdown) {
                            return;
                        }
                        try {
                            cond.await(waitMs, TimeUnit.MILLISECONDS);
                        } catch (InterruptedException e) {
                            log.info("{}: NodeManagerRunnable got InterruptedException", node.name());
                            Thread.currentThread().interrupt();
                        }
                        if (fault == null) {
                            if (!faultQueue.isEmpty()) {
                                fault = faultQueue.remove(0);
                            }
                        }
                    } finally {
                        lock.unlock();
                    }
                }
            } catch (Throwable e) {
                log.warn("{}: exiting NodeManagerRunnable with exception", node.name(), e);
            } finally {
            }
        }
    }

    NodeManager(Time time, Node node) {
        this.time = time;
        this.node = node;
        this.client = new AgentClient(node.hostname(), Node.Util.getTrogdorAgentPort(node));
        this.heartbeatMs = Node.Util.getIntConfig(node,
                Platform.Config.TROGDOR_COORDINATOR_HEARTBEAT_MS,
                Platform.Config.TROGDOR_COORDINATOR_HEARTBEAT_MS_DEFAULT);
        this.runnable = new NodeManagerRunnable();
        this.thread = new KafkaThread("NodeManagerThread(" + node.name() + ")", runnable, false);
        this.thread.start();
    }

    private boolean sendFault(long now, Fault fault) {
        try {
            client.putFault(new CreateAgentFaultRequest(fault.id(), fault.spec()));
        } catch (Exception e) {
            log.warn("{}: error sending fault to {}.", node.name(), client.target(), e);
            return false;
        }
        lock.lock();
        try {
            lastContactMs = now;
        } finally {
            lock.unlock();
        }
        SendingState state = (SendingState) fault.state();
        if (state.completeSend(node.name())) {
            fault.setState(new DoneState(now, ""));
        }
        return true;
    }

    private void sendHeartbeat(long now) {
        AgentStatusResponse status = null;
        try {
            status = client.getStatus();
        } catch (Exception e) {
            log.warn("{}: error sending heartbeat to {}.", node.name(), client.target(), e);
            return;
        }
        lock.lock();
        try {
            lastContactMs = now;
        } finally {
            lock.unlock();
        }
        log.debug("{}: got heartbeat status {}.", node.name(), status);
    }

    public void beginShutdown() {
        lock.lock();
        try {
            if (shutdown)
                return;
            log.trace("{}: beginning shutdown.", node.name());
            shutdown = true;
            cond.signalAll();
        } finally {
            lock.unlock();
        }
    }

    public void waitForShutdown() {
        log.trace("waiting for NodeManager({}) shutdown.", node.name());
        try {
            thread.join();
        } catch (InterruptedException e) {
            log.error("{}: Interrupted while waiting for thread shutdown", node.name(), e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Get the current status of this node.
     *
     * @return                  The node status.
     */
    public NodeStatus status() {
        lock.lock();
        try {
            return new NodeStatus(node.name(), lastContactMs);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Enqueue a new fault.
     *
     * @param fault             The fault to enqueue.
     */
    public void enqueueFault(Fault fault) {
        lock.lock();
        try {
            log.trace("{}: added {} to fault queue.", node.name(), fault);
            faultQueue.add(fault);
            cond.signalAll();
        } finally {
            lock.unlock();
        }
    }
};
