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
package org.apache.kafka.common.utils;

import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Mock fatal procedures for use in testing environments where the application should not terminate the JVM.
 * <p>Use factory methods {@link #disallowFatal()} ()}, {@link #forExit(Procedure)},
 * {@link #forExitAndHalt(Procedure, Procedure)}, and {@link #forHalt(Procedure)}
 * to construct test-safe alternatives for {@link Exit#system()}.
 * <p>Instances of this class throw from {@link #exit(int, String)} and {@link #halt(int, String)}, killing the calling
 * thread, but keeping the JVM alive. Call {@link #close()} at the end of the test to run hooks and assertions.
 */
public class MockExit extends Exit implements AutoCloseable {

    @FunctionalInterface
    public interface Procedure extends Exit.Procedure {
        void execute(int statusCode, String message);
        static Procedure noop() {
            return (statusCode, message) -> { };
        }
    }

    @FunctionalInterface
    public interface ShutdownHookAdder extends Exit.ShutdownHookAdder {
        void addShutdownHook(String name, Runnable runnable);

        static ShutdownHookAdder noop() {
            return (name, runnable) -> { };
        }
    }

    private static final long SHUTDOWN_HOOKS_MILLIS = TimeUnit.SECONDS.toMillis(30);

    /**
     * Fail the test after any fatal procedure is called.
     * This should be the default for any tests expecting happy-path execution.
     */
    public static MockExit disallowFatal() {
        return new MockExit(Procedure.noop(), Procedure.noop(), ShutdownHookAdder.noop(), true);
    }

    /**
     * Allow any fatal procedure to be called during correct execution
     * @param exitProcedure Procedure to run for {@link #exit(int, String)}
     * @param haltProcedure Procedure to run for {@link #halt(int, String)}
     */
    public static MockExit forExitAndHalt(Procedure exitProcedure, Procedure haltProcedure) {
        return new MockExit(exitProcedure, haltProcedure, ShutdownHookAdder.noop(), false);
    }

    /**
     * Expect the exit procedure to be called
     * @param exitProcedure Procedure to run for {@link #exit(int, String)}
     */
    public static MockExit forExit(Procedure exitProcedure) {
        return new MockExit(exitProcedure, Procedure.noop(), ShutdownHookAdder.noop(), false);
    }

    /**
     * Expect the halt procedure to be called
     * @param haltProcedure Procedure to run for {@link #halt(int, String)}
     */
    public static MockExit forHalt(Procedure haltProcedure) {
        return new MockExit(Procedure.noop(), haltProcedure, ShutdownHookAdder.noop(), false);
    }

    private final AtomicReference<Throwable> firstFatalCall;
    private final Procedure exitProcedure;
    private final Procedure haltProcedure;
    // Must synchronize on this to access.
    private IdentityHashMap<Thread, Thread> shutdownHooks;
    private final ShutdownHookAdder shutdownHookAdder;
    private final boolean failIfFatal;

    private MockExit(
            Procedure exitProcedure,
            Procedure haltProcedure,
            ShutdownHookAdder shutdownHookAdder,
            boolean failIfFatal
    ) {
        this.exitProcedure = Objects.requireNonNull(exitProcedure, "Exit Procedure may not be null");
        this.haltProcedure = Objects.requireNonNull(haltProcedure, "Halt Procedure may not be null");
        this.shutdownHookAdder = Objects.requireNonNull(shutdownHookAdder, "ShutdownHookAdder may noy be null");
        this.firstFatalCall = new AtomicReference<>();
        this.shutdownHooks = new IdentityHashMap<>();
        this.failIfFatal = failIfFatal;
    }


    @Override
    public void exitOrThrow(int statusCode, String message) {
        runFatalProcedure(exitProcedure, statusCode, message, "exit");
    }

    @Override
    public void haltOrThrow(int statusCode, String message) {
        runFatalProcedure(haltProcedure, statusCode, message, "halt");
    }

    private void runFatalProcedure(Procedure instanceProcedure, int statusCode, String message, String name) {
        try {
            instanceProcedure.execute(statusCode, message);
            throw new MockFatalProcedureError(name, statusCode, message);
        } catch (Throwable t) {
            firstFatalCall.compareAndSet(null, t);
            throw t;
        }
    }

    @Override
    public void addShutdownRunnable(String name, Runnable runnable) {
        trackShutdownHook(name, runnable);
        shutdownHookAdder.addShutdownHook(name, runnable);
    }

    /**
     * Run assertions about fatal procedure calls, and shutdown hooks previously registered.
     */
    @Override
    public void close() {
        try {
            runAsserts();
        } finally {
            runHooks();
        }
    }

    private synchronized void trackShutdownHook(String name, Runnable runnable) {
        if (shutdownHooks == null) {
            throw new IllegalStateException("MockExit already closed");
        }
        Thread t = (name != null) ? KafkaThread.nonDaemon(name, runnable) : new Thread(runnable);
        shutdownHooks.put(t, t);
    }

    private synchronized Set<Thread> clearShutdownHooks() {
        if (shutdownHooks == null) {
            throw new IllegalStateException("MockExit already closed");
        }
        Set<Thread> threads = shutdownHooks.keySet();
        shutdownHooks = null;
        return threads;
    }

    private void runHooks() {
        Set<Thread> hooks = clearShutdownHooks();
        for (Thread t : hooks) {
            t.start();
        }
        Timer timer = Time.SYSTEM.timer(SHUTDOWN_HOOKS_MILLIS);
        for (Thread t : hooks) {
            try {
                t.join(timer.remainingMs());
            } catch (InterruptedException ignored) {
            }
            timer.update();
            if (timer.isExpired()) {
                break;
            }
        }
    }

    private void runAsserts() {
        Throwable t = firstFatalCall.get();
        if (failIfFatal && t != null) {
            throw new AssertionError("Expected fatal procedure to not be called", t);
        }
    }

    /**
     * Error thrown at the end of mock fatal procedures instead of killing the process.
     * This is a fallback for {@link Procedure} instances which do not throw exceptions themselves.
     */
    public static class MockFatalProcedureError extends Error {

        public MockFatalProcedureError(String name, int statusCode, String message) {
            super(String.format("%s(%d, %s) called while using mocked fatal procedures", name, statusCode, message));
        }
    }
}
