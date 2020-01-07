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
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.trogdor.common.Platform;
import org.apache.kafka.trogdor.rest.RequestConflictException;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
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
    private final Map<Long, Worker> workers;

    /**
     * An ExecutorService used to schedule events in the future.
     */
    private final ScheduledExecutorService stateChangeExecutor;

    /**
     * An ExecutorService used to clean up TaskWorkers.
     */
    private final ExecutorService workerCleanupExecutor;

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
            if (refCount == 0) {
                this.notifyAll();
            }
            return true;
        }

        synchronized void waitForQuiescence() throws InterruptedException {
            while ((!shutdown) || (refCount > 0)) {
                this.wait();
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
        this.workerCleanupExecutor = Executors.newCachedThreadPool(
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
         * The worker ID.
         */
        private final long workerId;

        /**
         * The task ID.
         */
        private final String taskId;

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
        private Future<Void> timeoutFuture = null;

        /**
         * A future which is completed when the task transitions to DONE state.
         */
        private KafkaFutureImpl<String> doneFuture = null;

        /**
         * A shutdown manager reference which will keep the WorkerManager
         * alive for as long as this worker is alive.
         */
        private ShutdownManager.Reference reference;

        /**
         * Whether we should destroy the records of this worker once it stops.
         */
        private boolean mustDestroy = false;

        Worker(long workerId, String taskId, TaskSpec spec, long now) {
            this.workerId = workerId;
            this.taskId = taskId;
            this.spec = spec;
            this.taskWorker = spec.newTaskWorker(taskId);
            this.startedMs = now;
            this.reference = shutdownManager.takeReference();
        }

        long workerId() {
            return workerId;
        }

        String taskId() {
            return taskId;
        }

        TaskSpec spec() {
            return spec;
        }

        WorkerState state() {
            switch (state) {
                case STARTING:
                    return new WorkerStarting(taskId, spec);
                case RUNNING:
                    return new WorkerRunning(taskId, spec, startedMs, status.get());
                case CANCELLING:
                case STOPPING:
                    return new WorkerStopping(taskId, spec, startedMs, status.get());
                case DONE:
                    return new WorkerDone(taskId, spec, startedMs, doneMs, status.get(), error);
            }
            throw new RuntimeException("unreachable");
        }

        void transitionToRunning() {
            state = State.RUNNING;
            timeoutFuture = scheduler.schedule(stateChangeExecutor,
                new StopWorker(workerId, false),
                Math.max(0, spec.endMs() - time.milliseconds()));
        }

        Future<Void> transitionToStopping() {
            state = State.STOPPING;
            if (timeoutFuture != null) {
                timeoutFuture.cancel(false);
                timeoutFuture = null;
            }
            return workerCleanupExecutor.submit(new HaltWorker(this));
        }

        void transitionToDone() {
            state = State.DONE;
            doneMs = time.milliseconds();
            if (reference != null) {
                reference.close();
                reference = null;
            }
            doneFuture.complete(error);
        }

        @Override
        public String toString() {
            return String.format("%s_%d", taskId, workerId);
        }
    }

    public KafkaFuture<String> createWorker(long workerId, String taskId, TaskSpec spec) throws Throwable {
        try (ShutdownManager.Reference ref = shutdownManager.takeReference()) {
            final Worker worker = stateChangeExecutor.
                submit(new CreateWorker(workerId, taskId, spec, time.milliseconds())).get();
            if (worker.doneFuture != null) {
                log.info("{}: Ignoring request to create worker {}, because there is already " +
                    "a worker with that id.", nodeName, workerId);
                return worker.doneFuture;
            }
            worker.doneFuture = new KafkaFutureImpl<>();
            if (worker.spec.endMs() <= time.milliseconds()) {
                log.info("{}: Will not run worker {} as it has expired.", nodeName, worker);
                stateChangeExecutor.submit(new HandleWorkerHalting(worker,
                    "worker expired", true));
                return worker.doneFuture;
            }
            KafkaFutureImpl<String> haltFuture = new KafkaFutureImpl<>();
            haltFuture.thenApply((KafkaFuture.BaseFunction<String, Void>) errorString -> {
                if (errorString == null)
                    errorString = "";
                if (errorString.isEmpty()) {
                    log.info("{}: Worker {} is halting.", nodeName, worker);
                } else {
                    log.info("{}: Worker {} is halting with error {}",
                        nodeName, worker, errorString);
                }
                stateChangeExecutor.submit(
                    new HandleWorkerHalting(worker, errorString, false));
                return null;
            });
            try {
                worker.taskWorker.start(platform, worker.status, haltFuture);
            } catch (Exception e) {
                log.info("{}: Worker {} start() exception", nodeName, worker, e);
                stateChangeExecutor.submit(new HandleWorkerHalting(worker,
                    "worker.start() exception: " + Utils.stackTrace(e), true));
            }
            stateChangeExecutor.submit(new FinishCreatingWorker(worker));
            return worker.doneFuture;
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RequestConflictException) {
                log.info("{}: request conflict while creating worker {} for task {} with spec {}.",
                    nodeName, workerId, taskId, spec);
            } else {
                log.info("{}: Error creating worker {} for task {} with spec {}",
                    nodeName, workerId, taskId, spec, e);
            }
            throw e.getCause();
        }
    }

    /**
     * Handles a request to create a new worker.  Processed by the state change thread.
     */
    class CreateWorker implements Callable<Worker> {
        private final long workerId;
        private final String taskId;
        private final TaskSpec spec;
        private final long now;

        CreateWorker(long workerId, String taskId, TaskSpec spec, long now) {
            this.workerId = workerId;
            this.taskId = taskId;
            this.spec = spec;
            this.now = now;
        }

        @Override
        public Worker call() throws Exception {
            try {
                Worker worker = workers.get(workerId);
                if (worker != null) {
                    if (!worker.taskId().equals(taskId)) {
                        throw new RequestConflictException("There is already a worker ID " + workerId +
                            " with a different task ID.");
                    } else if (!worker.spec().equals(spec)) {
                        throw new RequestConflictException("There is already a worker ID " + workerId +
                            " with a different task spec.");
                    } else {
                        return worker;
                    }
                }
                worker = new Worker(workerId, taskId, spec, now);
                workers.put(workerId, worker);
                log.info("{}: Created worker {} with spec {}", nodeName, worker, spec);
                return worker;
            } catch (Exception e) {
                log.info("{}: unable to create worker {} for task {}, with spec {}",
                    nodeName, workerId, taskId, spec, e);
                throw e;
            }
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
                        "Transitioning to STOPPING.", nodeName, worker);
                    worker.transitionToStopping();
                    break;
                case STARTING:
                    log.info("{}: Worker {} is now RUNNING.  Scheduled to stop in {} ms.",
                        nodeName, worker, worker.spec.durationMs());
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
                            nodeName, worker, verb);
                        worker.transitionToDone();
                    } else {
                        log.info("{}: Worker {} {} during startup.  Transitioning to CANCELLING.",
                            nodeName, worker, verb);
                        worker.state = State.CANCELLING;
                    }
                    break;
                case CANCELLING:
                    log.info("{}: Cancelling worker {} {}.  ",
                            nodeName, worker, verb);
                    break;
                case RUNNING:
                    log.info("{}: Running worker {} {}.  Transitioning to STOPPING.",
                        nodeName, worker, verb);
                    worker.transitionToStopping();
                    break;
                case STOPPING:
                    log.info("{}: Stopping worker {} {}.", nodeName, worker, verb);
                    break;
                case DONE:
                    log.info("{}: Can't halt worker {} because it is already DONE.",
                        nodeName, worker);
                    break;
            }
            return null;
        }
    }

    /**
     * Transitions a worker to WorkerDone.  Processed by the state change thread.
     */
    class CompleteWorker implements Callable<Void> {
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
            if (worker.mustDestroy) {
                log.info("{}: destroying worker {} with error {}",
                    nodeName, worker, worker.error);
                workers.remove(worker.workerId);
            } else {
                log.info("{}: completed worker {} with error {}",
                    nodeName, worker, worker.error);
            }
            return null;
        }
    }

    public void stopWorker(long workerId, boolean mustDestroy) throws Throwable {
        try (ShutdownManager.Reference ref = shutdownManager.takeReference()) {
            stateChangeExecutor.submit(new StopWorker(workerId, mustDestroy)).get();
        } catch (ExecutionException e) {
            throw e.getCause();
        }
    }

    /**
     * Stops a worker.  Processed by the state change thread.
     */
    class StopWorker implements Callable<Void> {
        private final long workerId;
        private final boolean mustDestroy;

        StopWorker(long workerId, boolean mustDestroy) {
            this.workerId = workerId;
            this.mustDestroy = mustDestroy;
        }

        @Override
        public Void call() throws Exception {
            Worker worker = workers.get(workerId);
            if (worker == null) {
                log.info("{}: Can't stop worker {} because there is no worker with that ID.",
                    nodeName, workerId);
                return null;
            }
            if (mustDestroy) {
                worker.mustDestroy = true;
            }
            switch (worker.state) {
                case STARTING:
                    log.info("{}: Cancelling worker {} during its startup process.",
                        nodeName, worker);
                    worker.state = State.CANCELLING;
                    break;
                case CANCELLING:
                    log.info("{}: Can't stop worker {}, because it is already being " +
                        "cancelled.", nodeName, worker);
                    break;
                case RUNNING:
                    log.info("{}: Stopping running worker {}.", nodeName, worker);
                    worker.transitionToStopping();
                    break;
                case STOPPING:
                    log.info("{}: Can't stop worker {}, because it is already " +
                            "stopping.", nodeName, worker);
                    break;
                case DONE:
                    if (worker.mustDestroy) {
                        log.info("{}: destroying worker {} with error {}",
                            nodeName, worker, worker.error);
                        workers.remove(worker.workerId);
                    } else {
                        log.debug("{}: Can't stop worker {}, because it is already done.",
                            nodeName, worker);
                    }
                    break;
            }
            return null;
        }
    }

    /**
     * Cleans up the resources associated with a worker.  Processed by the worker
     * cleanup thread pool.
     */
    class HaltWorker implements Callable<Void> {
        private final Worker worker;

        HaltWorker(Worker worker) {
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

    public TreeMap<Long, WorkerState> workerStates() throws Exception {
        try (ShutdownManager.Reference ref = shutdownManager.takeReference()) {
            return stateChangeExecutor.submit(new GetWorkerStates()).get();
        }
    }

    class GetWorkerStates implements Callable<TreeMap<Long, WorkerState>> {
        @Override
        public TreeMap<Long, WorkerState> call() throws Exception {
            TreeMap<Long, WorkerState> workerMap = new TreeMap<>();
            for (Worker worker : workers.values()) {
                workerMap.put(worker.workerId(), worker.state());
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
            log.info("{}: Shutting down WorkerManager.", nodeName);
            try {
                stateChangeExecutor.submit(new DestroyAllWorkers()).get();
                log.info("{}: Waiting for shutdownManager quiescence...", nodeName);
                shutdownManager.waitForQuiescence();
                workerCleanupExecutor.shutdownNow();
                stateChangeExecutor.shutdownNow();
                log.info("{}: Waiting for workerCleanupExecutor to terminate...", nodeName);
                workerCleanupExecutor.awaitTermination(1, TimeUnit.DAYS);
                log.info("{}: Waiting for stateChangeExecutor to terminate...", nodeName);
                stateChangeExecutor.awaitTermination(1, TimeUnit.DAYS);
                log.info("{}: Shutting down shutdownExecutor.", nodeName);
                shutdownExecutor.shutdown();
            } catch (Exception e) {
                log.info("{}: Caught exception while shutting down WorkerManager", nodeName, e);
                throw e;
            }
            return null;
        }
    }

    /**
     * Begins the process of destroying all workers.  Processed by the state change thread.
     */
    class DestroyAllWorkers implements Callable<Void> {
        @Override
        public Void call() throws Exception {
            log.info("{}: Destroying all workers.", nodeName);

            // StopWorker may remove elements from the set of worker IDs.  That might generate
            // a ConcurrentModificationException if we were iterating over the worker ID
            // set directly.  Therefore, we make a copy of the worker IDs here and iterate
            // over that instead.
            //
            // Note that there is no possible way that more worker IDs can be added while this
            // callable is running, because the state change executor is single-threaded.
            ArrayList<Long> workerIds = new ArrayList<>(workers.keySet());

            for (long workerId : workerIds) {
                try {
                    new StopWorker(workerId, true).call();
                } catch (Exception e) {
                    log.error("Failed to stop worker {}", workerId, e);
                }
            }
            return null;
        }
    }

}
