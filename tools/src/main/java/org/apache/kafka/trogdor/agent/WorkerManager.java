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

package org.apache.kafka.trogdor.agent;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.internals.KafkaFutureImpl;
import org.apache.kafka.common.utils.Scheduler;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.common.ThreadUtils;
import org.apache.kafka.trogdor.rest.WorkerDone;
import org.apache.kafka.trogdor.rest.WorkerRunning;
import org.apache.kafka.trogdor.rest.WorkerStarting;
import org.apache.kafka.trogdor.rest.WorkerStopping;
import org.apache.kafka.trogdor.rest.WorkerState;
import org.apache.kafka.trogdor.task.AgentWorkerStatusTracker;
import org.apache.kafka.trogdor.task.TaskSpec;
import org.apache.kafka.trogdor.task.TaskWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public final class WorkerManager {
    private static final Logger log = LoggerFactory.getLogger(WorkerManager.class);

    /**
     * The platform to use.
     */
    private final Platform platform;

    /**
     * The name of this node.
     */
    private final String nodeName;

    /**
     * The scheduler to use.
     */
    private final Scheduler scheduler;

    /**
     * The clock to use.
     */
    private final Time time;

    /**
     * A map of task IDs to Work objects.
     */
    private final Map<String, Worker> workers;

    /**
     * An ExecutorService used to schedule events in the future.
     */
    private final ScheduledExecutorService stateChangeExecutor;

    /**
     * An ExecutorService used to clean up TaskWorkers.
     */
    private final ScheduledExecutorService workerCleanupExecutor;

    /**
     * An ExecutorService to help with shutting down.
     */
    private final ScheduledExecutorService shutdownExecutor;

    /**
     * The shutdown manager.
     */
    private final ShutdownManager shutdownManager = new ShutdownManager();

    /**
     * The shutdown manager handles shutting down gracefully.
     *
     * We can shut down gracefully only when all the references handed out
     * by the ShutdownManager has been closed, and the shutdown bit has
     * been set.  RPC operations hold a reference for the duration of their
     * execution, and so do Workers which have not been shut down.
     * This prevents us from shutting down in the middle of an RPC, or with
     * workers which are still running.
     */
    static class ShutdownManager {
        private boolean shutdown = false;
        private long refCount = 0;

        class Reference implements AutoCloseable {
            AtomicBoolean closed = new AtomicBoolean(false);

            @Override
            public void close() {
                if (closed.compareAndSet(false, true)) {
                    synchronized (ShutdownManager.this) {
                        refCount--;
                        if (shutdown && (refCount == 0)) {
                            ShutdownManager.this.notifyAll();
                        }
                    }
                }
            }
        }

        synchronized Reference takeReference() {
            if (shutdown) {
                throw new KafkaException("WorkerManager is shut down.");
            }
            refCount++;
            return new Reference();
        }

        synchronized boolean shutdown() {
            if (shutdown) {
                return false;
            }
            shutdown = true;
            return true;
        }

        synchronized void waitForQuiescence() throws InterruptedException {
            while ((!shutdown) || (refCount > 0)) {
                wait();
            }
        }
    }

    WorkerManager(Platform platform, Scheduler scheduler) {
        this.platform = platform;
        this.nodeName = platform.curNode().name();
        this.scheduler = scheduler;
        this.time = scheduler.time();
        this.workers = new HashMap<>();
        this.stateChangeExecutor = Executors.newSingleThreadScheduledExecutor(
                ThreadUtils.createThreadFactory("WorkerManagerStateThread", false));
        this.workerCleanupExecutor = Executors.newScheduledThreadPool(1,
            ThreadUtils.createThreadFactory("WorkerCleanupThread%d", false));
        this.shutdownExecutor = Executors.newScheduledThreadPool(0,
            ThreadUtils.createThreadFactory("WorkerManagerShutdownThread%d", false));
    }

    enum State {
        STARTING,
        CANCELLING,
        RUNNING,
        STOPPING,
        DONE,
    }

    /**
     * A worker which is being tracked.
     */
    class Worker {
        /**
         * The task ID.
         */
        private final String id;

        /**
         * The task specification.
         */
        private final TaskSpec spec;

        /**
         * The work which this worker is performing.
         */
        private final TaskWorker taskWorker;

        /**
         * The worker status.
         */
        private final AgentWorkerStatusTracker status = new AgentWorkerStatusTracker();

        /**
         * The time when this task was started.
         */
        private final long startedMs;

        /**
         * The work state.
         */
        private State state = State.STARTING;

        /**
         * The time when this task was completed, or -1 if it has not been.
         */
        private long doneMs = -1;

        /**
         * The worker error.
         */
        private String error = "";

        /**
         * If there is a task timeout scheduled, this is a future which can
         * be used to cancel it.
         */
        private Future<TaskSpec> timeoutFuture = null;

        /**
         * A shutdown manager reference which will keep the WorkerManager
         * alive for as long as this worker is alive.
         */
        private ShutdownManager.Reference reference;

        Worker(String id, TaskSpec spec, long now) {
            this.id = id;
            this.spec = spec;
            this.taskWorker = spec.newTaskWorker(id);
            this.startedMs = now;
            this.reference = shutdownManager.takeReference();
        }

        String id() {
            return id;
        }

        TaskSpec spec() {
            return spec;
        }

        WorkerState state() {
            switch (state) {
                case STARTING:
                    return new WorkerStarting(spec);
                case RUNNING:
                    return new WorkerRunning(spec, startedMs, status.get());
                case CANCELLING:
                case STOPPING:
                    return new WorkerStopping(spec, startedMs, status.get());
                case DONE:
                    return new WorkerDone(spec, startedMs, doneMs, status.get(), error);
            }
            throw new RuntimeException("unreachable");
        }

        void transitionToRunning() {
            state = State.RUNNING;
            timeoutFuture = scheduler.schedule(stateChangeExecutor,
                new StopWorker(id), spec.durationMs());
        }

        void transitionToStopping() {
            state = State.STOPPING;
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
                timeoutFuture = null;
            }
            workerCleanupExecutor.submit(new CleanupWorker(this));
        }

        void transitionToDone() {
            state = State.DONE;
            doneMs = time.milliseconds();
            if (reference != null) {
                reference.close();
                reference = null;
            }
        }
    }

    public void createWorker(final String id, TaskSpec spec) throws Exception {
        try (ShutdownManager.Reference ref = shutdownManager.takeReference()) {
            final Worker worker = stateChangeExecutor.
                submit(new CreateWorker(id, spec, time.milliseconds())).get();
            if (worker == null) {
                log.info("{}: Ignoring request to create worker {}, because there is already " +
                    "a worker with that id.", nodeName, id);
                return;
            }
            KafkaFutureImpl<String> haltFuture = new KafkaFutureImpl<>();
            haltFuture.thenApply(new KafkaFuture.BaseFunction<String, Void>() {
                @Override
                public Void apply(String errorString) {
                    if (errorString == null)
                        errorString = "";
                    if (errorString.isEmpty()) {
                        log.info("{}: Worker {} is halting.", nodeName, id);
                    } else {
                        log.info("{}: Worker {} is halting with error {}", nodeName, id, errorString);
                    }
                    stateChangeExecutor.submit(
                        new HandleWorkerHalting(worker, errorString, false));
                    return null;
                }
            });
            try {
                worker.taskWorker.start(platform, worker.status, haltFuture);
            } catch (Exception e) {
                log.info("{}: Worker {} start() exception", nodeName, id, e);
                stateChangeExecutor.submit(new HandleWorkerHalting(worker,
                    "worker.start() exception: " + Utils.stackTrace(e), true));
            }
            stateChangeExecutor.submit(new FinishCreatingWorker(worker));
        }
    }

    /**
     * Handles a request to create a new worker.  Processed by the state change thread.
     */
    class CreateWorker implements Callable<Worker> {
        private final String id;
        private final TaskSpec spec;
        private final long now;

        CreateWorker(String id, TaskSpec spec, long now) {
            this.id = id;
            this.spec = spec;
            this.now = now;
        }

        @Override
        public Worker call() throws Exception {
            Worker worker = workers.get(id);
            if (worker != null) {
                log.info("{}: Task ID {} is already in use.", nodeName, id);
                return null;
            }
            worker = new Worker(id, spec, now);
            workers.put(id, worker);
            log.info("{}: Created a new worker for task {} with spec {}", nodeName, id, spec);
            return worker;
        }
    }

    /**
     * Finish creating a Worker.  Processed by the state change thread.
     */
    class FinishCreatingWorker implements Callable<Void> {
        private final Worker worker;

        FinishCreatingWorker(Worker worker) {
            this.worker = worker;
        }

        @Override
        public Void call() throws Exception {
            switch (worker.state) {
                case CANCELLING:
                    log.info("{}: Worker {} was cancelled while it was starting up.  " +
                        "Transitioning to STOPPING.", nodeName, worker.id);
                    worker.transitionToStopping();
                    break;
                case STARTING:
                    log.info("{}: Worker {} is now RUNNING.  Scheduled to stop in {} ms.",
                        nodeName, worker.id, worker.spec.durationMs());
                    worker.transitionToRunning();
                    break;
                default:
                    break;
            }
            return null;
        }
    }

    /**
     * Handles a worker halting.  Processed by the state change thread.
     */
    class HandleWorkerHalting implements Callable<Void> {
        private final Worker worker;
        private final String failure;
        private final boolean startupHalt;

        HandleWorkerHalting(Worker worker, String failure, boolean startupHalt) {
            this.worker = worker;
            this.failure = failure;
            this.startupHalt = startupHalt;
        }

        @Override
        public Void call() throws Exception {
            if (worker.error.isEmpty()) {
                worker.error = failure;
            }
            String verb = (worker.error.isEmpty()) ? "halting" :
                "halting with error [" + worker.error + "]";
            switch (worker.state) {
                case STARTING:
                    if (startupHalt) {
                        log.info("{}: Worker {} {} during startup.  Transitioning to DONE.",
                            nodeName, worker.id, verb);
                        worker.transitionToDone();
                    } else {
                        log.info("{}: Worker {} {} during startup.  Transitioning to CANCELLING.",
                            nodeName, worker.id, verb);
                        worker.state = State.CANCELLING;
                    }
                    break;
                case CANCELLING:
                    log.info("{}: Cancelling worker {} {}.  ",
                            nodeName, worker.id, verb);
                    break;
                case RUNNING:
                    log.info("{}: Running worker {} {}.  Transitioning to STOPPING.",
                        nodeName, worker.id, verb);
                    worker.transitionToStopping();
                    break;
                case STOPPING:
                    log.info("{}: Stopping worker {} {}.", nodeName, worker.id, verb);
                    break;
                case DONE:
                    log.info("{}: Can't halt worker {} because it is already DONE.",
                        nodeName, worker.id);
                    break;
            }
            return null;
        }
    }

    /**
     * Transitions a worker to WorkerDone.  Processed by the state change thread.
     */
    static class CompleteWorker implements Callable<Void> {
        private final Worker worker;

        private final String failure;

        CompleteWorker(Worker worker, String failure) {
            this.worker = worker;
            this.failure = failure;
        }

        @Override
        public Void call() throws Exception {
            if (worker.error.isEmpty() && !failure.isEmpty()) {
                worker.error = failure;
            }
            worker.transitionToDone();
            return null;
        }
    }

    public TaskSpec stopWorker(String id) throws Exception {
        try (ShutdownManager.Reference ref = shutdownManager.takeReference()) {
            TaskSpec taskSpec = stateChangeExecutor.submit(new StopWorker(id)).get();
            if (taskSpec == null) {
                throw new KafkaException("No task found with id " + id);
            }
            return taskSpec;
        }
    }

    /**
     * Stops a worker.  Processed by the state change thread.
     */
    class StopWorker implements Callable<TaskSpec> {
        private final String id;

        StopWorker(String id) {
            this.id = id;
        }

        @Override
        public TaskSpec call() throws Exception {
            Worker worker = workers.get(id);
            if (worker == null) {
                return null;
            }
            switch (worker.state) {
                case STARTING:
                    log.info("{}: Cancelling worker {} during its startup process.",
                        nodeName, id);
                    worker.state = State.CANCELLING;
                    break;
                case CANCELLING:
                    log.info("{}: Can't stop worker {}, because it is already being " +
                        "cancelled.", nodeName, id);
                    break;
                case RUNNING:
                    log.info("{}: Stopping running worker {}.", nodeName, id);
                    worker.transitionToStopping();
                    break;
                case STOPPING:
                    log.info("{}: Can't stop worker {}, because it is already " +
                            "stopping.", nodeName, id);
                    break;
                case DONE:
                    log.debug("{}: Can't stop worker {}, because it is already done.",
                        nodeName, id);
                    break;
            }
            return worker.spec();
        }
    }

    /**
     * Cleans up the resources associated with a worker.  Processed by the worker
     * cleanup thread pool.
     */
    class CleanupWorker implements Callable<Void> {
        private final Worker worker;

        CleanupWorker(Worker worker) {
            this.worker = worker;
        }

        @Override
        public Void call() throws Exception {
            String failure = "";
            try {
                worker.taskWorker.stop(platform);
            } catch (Exception exception) {
                log.error("{}: worker.stop() exception", nodeName, exception);
                failure = exception.getMessage();
            }
            stateChangeExecutor.submit(new CompleteWorker(worker, failure));
            return null;
        }
    }

    public TreeMap<String, WorkerState> workerStates() throws Exception {
        try (ShutdownManager.Reference ref = shutdownManager.takeReference()) {
            return stateChangeExecutor.submit(new GetWorkerStates()).get();
        }
    }

    class GetWorkerStates implements Callable<TreeMap<String, WorkerState>> {
        @Override
        public TreeMap<String, WorkerState> call() throws Exception {
            TreeMap<String, WorkerState> workerMap = new TreeMap<>();
            for (Worker worker : workers.values()) {
                workerMap.put(worker.id(), worker.state());
            }
            return workerMap;
        }
    }

    public void beginShutdown() throws Exception {
        if (shutdownManager.shutdown()) {
            shutdownExecutor.submit(new Shutdown());
        }
    }

    public void waitForShutdown() throws Exception {
        while (!shutdownExecutor.isShutdown()) {
            shutdownExecutor.awaitTermination(1, TimeUnit.DAYS);
        }
    }

    class Shutdown implements Callable<Void> {
        @Override
        public Void call() throws Exception {
            log.info("{}: Shutting down WorkerManager.", platform.curNode().name());
            for (Worker worker : workers.values()) {
                stateChangeExecutor.submit(new StopWorker(worker.id));
            }
            shutdownManager.waitForQuiescence();
            workerCleanupExecutor.shutdownNow();
            stateChangeExecutor.shutdownNow();
            workerCleanupExecutor.awaitTermination(1, TimeUnit.DAYS);
            stateChangeExecutor.awaitTermination(1, TimeUnit.DAYS);
            shutdownExecutor.shutdown();
            return null;
        }
    }
}
