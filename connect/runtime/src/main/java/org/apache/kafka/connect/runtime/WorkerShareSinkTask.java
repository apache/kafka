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
package org.apache.kafka.connect.runtime;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ShareConsumer;
import org.apache.kafka.common.internals.Plugin;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.common.utils.internals.KafkaThread;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.runtime.errors.ErrorHandlingMetrics;
import org.apache.kafka.connect.runtime.errors.ErrorReporter;
import org.apache.kafka.connect.runtime.errors.RetryWithToleranceOperator;
import org.apache.kafka.connect.runtime.isolation.LoaderSwap;
import org.apache.kafka.connect.sink.ShareSinkTask;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.ClusterConfigState;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.storage.HeaderConverter;
import org.apache.kafka.connect.storage.StatusBackingStore;
import org.apache.kafka.connect.util.ConnectorTaskId;
import org.apache.kafka.connect.util.LoggingContext;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.apache.kafka.connect.runtime.WorkerConfig.TOPIC_TRACKING_ENABLE_CONFIG;

final class WorkerShareSinkTask extends WorkerTask<ConsumerRecord<byte[], byte[]>, SinkRecord> {
    private final ShareSinkTask task;
    private final ClusterConfigState configState;
    private final Plugin<Converter> keyConverter;
    private final Plugin<Converter> valueConverter;
    private final Plugin<HeaderConverter> headerConverter;
    private final Supplier<ShareConsumer<byte[], byte[]>> consumerFactory;
    private final ShareSinkConsumerController.Settings controllerSettings;
    private final long cancelGraceMs;
    private final ShareSinkExecutionRegistry.Reservation reservation;
    private final SinkRecordConverter recordConverter;
    private final Object lifecycle = new Object();
    private final AtomicBoolean metricsRemoved = new AtomicBoolean();
    private final AtomicReference<Throwable> cleanupError = new AtomicReference<>();
    private final String version;
    private Map<String, String> taskConfig;
    private WorkerShareSinkTaskContext context;
    private volatile Generation current;
    private Thread pluginThread;
    private CountDownLatch activeCall;
    private boolean runEntered;
    private boolean pluginExited;
    private boolean pluginCleanupComplete;
    private int owners;
    private boolean ownersClean = true;
    private long generationSequence;
    private ShareSinkDelivery preparedDelivery;
    private ShareSinkConsumerController.AttemptId preparedAttempt;

    WorkerShareSinkTask(ConnectorTaskId id, ShareSinkTask task,
                        TaskStatus.Listener statusListener, TargetState initialState,
                        Resources resources,
                        Supplier<ShareConsumer<byte[], byte[]>> consumerFactory,
                        ShareSinkConsumerController.Settings controllerSettings, long cancelGraceMs,
                        ClassLoader loader, Time time,
                        ShareSinkExecutionRegistry.Reservation reservation) {
        super(id, statusListener, initialState, loader, resources.connectMetrics, resources.errorMetrics,
                resources.retryWithToleranceOperator, resources.transformationChain, resources.reporters,
                time, resources.statusBackingStore, resources.pluginsMetadata, resources.loaderSwapper);
        this.task = task;
        this.configState = resources.configState;
        this.keyConverter = resources.keyConverter;
        this.valueConverter = resources.valueConverter;
        this.headerConverter = resources.headerConverter;
        this.consumerFactory = consumerFactory;
        this.controllerSettings = controllerSettings;
        this.cancelGraceMs = cancelGraceMs;
        this.reservation = reservation;
        this.version = task.version();
        this.recordConverter = new SinkRecordConverter(id, keyConverter, valueConverter, headerConverter,
                transformationChain, retryWithToleranceOperator, resources.loaderSwapper,
                resources.workerConfig.getBoolean(TOPIC_TRACKING_ENABLE_CONFIG) ? this::recordActiveTopic : topic -> { });
    }

    @Override
    public void initialize(TaskConfig config) {
        taskConfig = Map.copyOf(config.originalsStrings());
        context = new WorkerShareSinkTaskContext(() -> {
            Map<String, String> latest = configState.taskConfig(id);
            return latest == null ? taskConfig : latest;
        }, pluginMetrics);
    }

    @Override
    protected void initializeAndStart() {
        task.initialize(context);
        task.start(taskConfig);
    }

    @Override
    public void run() {
        synchronized (lifecycle) {
            if (runEntered) {
                return;
            }
            runEntered = true;
            pluginThread = Thread.currentThread();
        }
        try {
            super.run();
        } finally {
            recordPluginExit();
        }
    }

    @Override
    protected void execute() {
        try {
            while (!isStopping() && !isCancelled()) {
                if (!resumeIfPaused()) {
                    return;
                }
                Generation generation = current;
                if (generation == null) {
                    generation = startGeneration();
                    if (generation == null) {
                        continue;
                    }
                }
                ShareSinkConsumerController.Unit unit = generation.controller.awaitUnit();
                if (unit == null) {
                    if (generation.draining || isStopping()) {
                        retireGeneration(true);
                        continue;
                    }
                    throw controllerFailure(generation);
                }
                if (isStopping() || shouldPause()) {
                    generation.controller.requestStop();
                    continue;
                }
                processUnit(generation, unit);
            }
        } catch (InterruptedException e) {
            if (!isStopping() && !isCancelled()) {
                Generation generation = current;
                throw generation == null ? new ConnectException("Share task interrupted", e) : controllerFailure(generation);
            }
            Thread.currentThread().interrupt();
        }
    }

    private boolean resumeIfPaused() throws InterruptedException {
        if (!shouldPause()) {
            return true;
        }
        retireGeneration(true);
        onPause();
        if (!awaitUnpause()) {
            return false;
        }
        onResume();
        preparedDelivery = null;
        preparedAttempt = null;
        return true;
    }

    private void processUnit(Generation generation, ShareSinkConsumerController.Unit unit) {
        CountDownLatch call = beginCall(generation);
        try {
            if (preparedAttempt == null || preparedAttempt.generation() != unit.id().generation()
                    || preparedAttempt.unit() != unit.id().unit()) {
                preparedDelivery = new ShareSinkDelivery(unit.records(), recordConverter::convertForShare);
                preparedAttempt = unit.id();
            }
            ShareSinkDelivery.CompletedDelivery completed = preparedDelivery.complete(task);
            endCall(call);
            context.drainHints();
            if (!generation.controller.publishResult(unit, completed)) {
                throw controllerFailure(generation);
            }
            preparedDelivery = null;
            preparedAttempt = null;
        } catch (RetriableException e) {
            endCall(call);
            long delay = context.drainHints().timeoutMs();
            if (!generation.controller.publishRetry(unit, delay < 0 ? 100 : delay)) {
                throw controllerFailure(generation);
            }
        } catch (RuntimeException e) {
            endCall(call);
            generation.controller.publishFailure(unit, e);
            throw e;
        } finally {
            endCall(call);
        }
    }

    private CountDownLatch beginCall(Generation generation) {
        synchronized (lifecycle) {
            if (generation.controller.failure().isPresent() || isCancelled()) {
                throw controllerFailure(generation);
            }
            activeCall = new CountDownLatch(1);
            return activeCall;
        }
    }

    private void endCall(CountDownLatch call) {
        synchronized (lifecycle) {
            call.countDown();
            if (activeCall == call) {
                activeCall = null;
            }
        }
    }

    private Generation startGeneration() {
        Generation generation = new Generation(++generationSequence);
        synchronized (lifecycle) {
            if (isStopping() || isCancelled() || shouldPause()) {
                return null;
            }
            current = generation;
            owners++;
        }
        try {
            generation.thread.start();
        } catch (Throwable e) {
            synchronized (lifecycle) {
                owners--;
                current = null;
            }
            throw e;
        }
        return generation;
    }

    private ConnectException controllerFailure(Generation generation) {
        return new ConnectException("Share task lost its consumer generation",
                generation.controller.failure().orElse(null));
    }

    private void controllerFailed(Generation generation) {
        reservation.markOrphan();
        synchronized (this) {
            onFailure(controllerFailure(generation));
            super.cancel();
        }
        synchronized (lifecycle) {
            generation.cancelledCall = activeCall;
            if (activeCall != null && pluginThread != null) {
                pluginThread.interrupt();
            }
        }
    }

    private void retireGeneration(boolean requireExit) throws InterruptedException {
        Generation generation = current;
        if (generation == null) {
            return;
        }
        generation.draining = true;
        generation.controller.requestDrain();
        if (!generation.controller.awaitStopped(Duration.ofMillis(cancelGraceMs))) {
            generation.controller.requestStop();
            reservation.markOrphan();
            if (requireExit) {
                throw new ConnectException("Share consumer cleanup did not finish before its grace period");
            }
            return;
        }
        if (!generation.controller.cleanupComplete()) {
            reservation.markOrphan();
            if (requireExit) {
                throw controllerFailure(generation);
            }
        }
        current = null;
    }

    @Override
    public void stop() {
        super.stop();
        Generation generation = current;
        if (generation != null) {
            generation.draining = true;
            generation.controller.requestDrain();
        }
    }

    @Override
    public void cancel() {
        super.stop();
        super.cancel();
        reservation.markOrphan();
        Generation generation = current;
        if (generation != null) {
            generation.controller.requestStop();
        }
        synchronized (lifecycle) {
            if (activeCall != null && pluginThread != null) {
                pluginThread.interrupt();
            }
        }
    }

    @Override
    public void transitionTo(TargetState state) {
        super.transitionTo(state);
        Generation generation = current;
        if (state == TargetState.PAUSED && generation != null) {
            generation.draining = true;
            generation.controller.requestDrain();
        }
    }

    @Override
    protected void close() {
        try {
            retireGeneration(false);
        } catch (InterruptedException e) {
            Generation generation = current;
            if (generation != null) {
                generation.controller.requestStop();
            }
            reservation.markOrphan();
            Thread.currentThread().interrupt();
        } finally {
            Utils.closeQuietly(task::stop, "share sink task", cleanupError);
            Utils.closeAllQuietly(cleanupError, "share sink plugins", headerConverter, keyConverter, valueConverter, pluginMetrics);
        }
    }

    @Override
    void doClose() {
        boolean interrupted = Thread.interrupted();
        try {
            close();
        } finally {
            Utils.closeAllQuietly(cleanupError, "share processing resources", transformationChain, retryWithToleranceOperator);
            synchronized (lifecycle) {
                pluginCleanupComplete = cleanupError.get() == null;
            }
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
        if (cleanupError.get() != null) {
            throw new ConnectException("Share task plugin cleanup did not complete", cleanupError.get());
        }
    }

    ShareSinkExecutionRegistry.Reservation reservation() {
        return reservation;
    }

    void abortBeforeStart() {
        synchronized (lifecycle) {
            if (runEntered) {
                cancel();
                return;
            }
            runEntered = true;
        }
        try {
            doClose();
        } finally {
            recordPluginExit();
        }
    }

    private void recordPluginExit() {
        synchronized (lifecycle) {
            pluginExited = true;
            pluginThread = null;
        }
        reservation.pluginExited(pluginCleanupComplete);
        reportControllerExit();
    }

    private void reportControllerExit() {
        boolean exited;
        boolean clean;
        synchronized (lifecycle) {
            exited = pluginExited && owners == 0;
            clean = ownersClean;
        }
        if (exited) {
            reservation.controllerExited(clean);
        }
    }

    @Override
    public boolean awaitStop(long timeoutMs) {
        try {
            reservation.released().toCompletableFuture().get(Math.max(0, timeoutMs), TimeUnit.MILLISECONDS);
            return true;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (ExecutionException | TimeoutException e) {
            return false;
        }
    }

    @Override
    public void removeMetrics() {
        synchronized (lifecycle) {
            if (!pluginExited || owners != 0 || !pluginCleanupComplete || !ownersClean) {
                return;
            }
        }
        if (metricsRemoved.compareAndSet(false, true)) {
            super.removeMetrics();
        }
    }

    @Override
    protected String taskVersion() {
        return version;
    }

    record Resources(WorkerConfig workerConfig, ClusterConfigState configState, ConnectMetrics connectMetrics,
                     Plugin<Converter> keyConverter, Plugin<Converter> valueConverter, ErrorHandlingMetrics errorMetrics,
                     Plugin<HeaderConverter> headerConverter,
                     TransformationChain<ConsumerRecord<byte[], byte[]>, SinkRecord> transformationChain,
                     RetryWithToleranceOperator<ConsumerRecord<byte[], byte[]>> retryWithToleranceOperator,
                     StatusBackingStore statusBackingStore,
                     Supplier<List<ErrorReporter<ConsumerRecord<byte[], byte[]>>>> reporters,
                     TaskPluginsMetadata pluginsMetadata, Function<ClassLoader, LoaderSwap> loaderSwapper) {
    }

    private final class Generation {
        private final ShareSinkConsumerController controller;
        private final Thread thread;
        private volatile boolean draining;
        private volatile CountDownLatch cancelledCall;

        private Generation(long sequence) {
            controller = new ShareSinkConsumerController(consumerFactory, SinkConnectorConfig.parseTopicsList(taskConfig),
                    reservation.incarnation(), sequence, controllerSettings, time, () -> controllerFailed(this));
            thread = KafkaThread.nonDaemon("share-consumer-" + id + "-" + sequence, this::runOwner);
        }

        private void runOwner() {
            boolean clean = false;
            try (LoggingContext loggingContext = LoggingContext.forTask(id);
                    LoaderSwap swap = pluginLoaderSwapper.apply(loader)) {
                controller.run();
                clean = controller.cleanupComplete();
                CountDownLatch call = cancelledCall;
                if (call != null && !call.await(cancelGraceMs, TimeUnit.MILLISECONDS)) {
                    reservation.markOrphan();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                reservation.markOrphan();
            } finally {
                synchronized (lifecycle) {
                    ownersClean &= clean;
                    owners--;
                }
                if (!clean) {
                    reservation.markOrphan();
                }
                reportControllerExit();
            }
        }
    }
}
