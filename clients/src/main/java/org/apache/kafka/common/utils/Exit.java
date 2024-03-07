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

/**
 * Internal class that should be used instead of `System.exit()` and `Runtime.getRuntime().halt()` so that tests can
 * easily change the behaviour.
 */
public abstract class Exit {

    @FunctionalInterface
    public interface Procedure {
        void execute(int statusCode, String message);
    }

    @FunctionalInterface
    public interface ShutdownHookAdder {
        void addShutdownHook(String name, Runnable runnable);
    }

    private static final Procedure DEFAULT_HALT_PROCEDURE = (statusCode, message) -> Runtime.getRuntime().halt(statusCode);

    private static final Procedure DEFAULT_EXIT_PROCEDURE = (statusCode, message) -> System.exit(statusCode);

    private static final ShutdownHookAdder DEFAULT_SHUTDOWN_HOOK_ADDER = (name, runnable) -> {
        if (name != null)
            Runtime.getRuntime().addShutdownHook(KafkaThread.nonDaemon(name, runnable));
        else
            Runtime.getRuntime().addShutdownHook(new Thread(runnable));
    };

    private static final Procedure NOOP_HALT_PROCEDURE = (statusCode, message) -> {
        throw new IllegalStateException("Halt called after resetting procedures; possible race condition present in test");
    };

    private static final Procedure NOOP_EXIT_PROCEDURE = (statusCode, message) -> {
        throw new IllegalStateException("Exit called after resetting procedures; possible race condition present in test");
    };

    private static volatile Procedure exitProcedure = DEFAULT_EXIT_PROCEDURE;
    private static volatile Procedure haltProcedure = DEFAULT_HALT_PROCEDURE;
    private static volatile ShutdownHookAdder shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER;

    /**
     * Use of this method is discouraged, and should be replaced with {@link #exitOrThrow(int)}
     */
    public static void exit(int statusCode) {
        exit(statusCode, null);
    }

    /**
     * Use of this method is discouraged, and should be replaced with {@link #exitOrThrow(int, String)}
     */
    public static void exit(int statusCode, String message) {
        exitProcedure.execute(statusCode, message);
    }

    /**
     * Use of this method is discouraged, and should be replaced with {@link #haltOrThrow(int)}
     */
    public static void halt(int statusCode) {
        halt(statusCode, null);
    }

    /**
     * Use of this method is discouraged, and should be replaced with {@link #haltOrThrow(int, String)}
     */
    public static void halt(int statusCode, String message) {
        haltProcedure.execute(statusCode, message);
    }

    /**
     * Use of this method is discouraged, and should be replaced with {@link #addShutdownRunnable(String, Runnable)}}
     */
    public static void addShutdownHook(String name, Runnable runnable) {
        shutdownHookAdder.addShutdownHook(name, runnable);
    }

    /**
     * For testing only, do not call in main code.
     */
    public static void setExitProcedure(Procedure procedure) {
        exitProcedure = procedure;
    }

    /**
     * For testing only, do not call in main code.
     */
    public static void setHaltProcedure(Procedure procedure) {
        haltProcedure = procedure;
    }

    /**
     * For testing only, do not call in main code.
     */
    public static void setShutdownHookAdder(ShutdownHookAdder shutdownHookAdder) {
        Exit.shutdownHookAdder = shutdownHookAdder;
    }

    /**
     * For testing only, do not call in main code.
     * <p>Clears the procedure set in {@link #setExitProcedure(Procedure)}, but does not restore system default behavior of exiting the JVM.
     */
    public static void resetExitProcedure() {
        exitProcedure = NOOP_EXIT_PROCEDURE;
    }

    /**
     * For testing only, do not call in main code.
     * <p>Clears the procedure set in {@link #setHaltProcedure(Procedure)}, but does not restore system default behavior of exiting the JVM.
     */
    public static void resetHaltProcedure() {
        haltProcedure = NOOP_HALT_PROCEDURE;
    }

    /**
     * For testing only, do not call in main code.
     * <p>Restores the system default shutdown hook behavior.
     */
    public static void resetShutdownHookAdder() {
        shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER;
    }

    /**
     * @return the default system exit behavior. Using this grants the ability to stop the JVM at any time.
     */
    public static Exit system() {
        return SystemExit.SYSTEM;
    }

    /**
     * @return an immutable reference to exit behavior that is active at the time this method is evaluated.
     * <p>This may grant the ability to stop the JVM at any time if the static exit behavior has not been changed.
     * <p>Note: changes to the static exit behavior made after this method will not apply to the returned object.
     * This is used as a temporary shim between the mutable-static and immutable-instance forms of the Exit class.
     * This is intended to be called as early as possible on the same thread that calls
     * {@link #setExitProcedure(Procedure)}, {@link #setHaltProcedure(Procedure)},
     * or {@link #addShutdownHook(String, Runnable)} to avoid race conditions.
     */
    public static Exit staticContext() {
        Procedure exitProcedure = Exit.exitProcedure;
        Procedure haltProcedure = Exit.haltProcedure;
        ShutdownHookAdder shutdownHookAdder = Exit.shutdownHookAdder;
        if (exitProcedure != DEFAULT_EXIT_PROCEDURE
                || haltProcedure != DEFAULT_HALT_PROCEDURE
                || shutdownHookAdder != DEFAULT_SHUTDOWN_HOOK_ADDER
        ) {
            // Static exit is mocked
            return new StaticContext(exitProcedure, haltProcedure, shutdownHookAdder);
        } else {
            // No mocks are present, use system procedures. The singleton is used to enable reference equality checks.
            return system();
        }
    }

    /**
     * @see #exitOrThrow(int, String)
     */
    public void exitOrThrow(int statusCode) {
        exitOrThrow(statusCode, null);
    }

    /**
     * Terminate the running Java Virtual Machine, or throw an exception if this is not possible.
     * <p>By default, this behaves like {@link Runtime#exit(int)}.
     * @param message Human-readable termination message to aid in debugging, maybe null.
     * @throws Error If termination has been replaced by mocked behavior
     *
     * @see Runtime#exit
     * @see #haltOrThrow
     * @see #addShutdownRunnable
     */
    public abstract void exitOrThrow(int statusCode, String message);

    /**
     * @see #haltOrThrow(int, String)
     */
    public void haltOrThrow(int statusCode) {
        haltOrThrow(statusCode, null);
    }

    /**
     * Terminate the running Java Virtual Machine, or throw an exception if this is not possible.
     * <p>By default, this behaves like {@link Runtime#halt(int)}.
     * @param message Human-readable termination message to aid in debugging, maybe null
     * @throws Error If termination has been replaced by mocked behavior
     *
     * @see Runtime#halt
     * @see #exitOrThrow
     * @see #addShutdownRunnable
     */
    public abstract void haltOrThrow(int statusCode, String message);

    /**
     * <p>By default, this behaves like {@link Runtime#addShutdownHook(Thread)}.
     * @param name The name of the thread executing the runnable, maybe null.
     * @param runnable The operation that should take place at shutdown, non-null.
     * @see Runtime#addShutdownHook
     * @see #exitOrThrow
     * @see #haltOrThrow
     */
    public abstract void addShutdownRunnable(String name, Runnable runnable);

    private static final class SystemExit extends Exit {

        private static final Exit SYSTEM = new SystemExit();

        private SystemExit() {
        }

        @Override
        public void exitOrThrow(int statusCode, String message) {
            System.exit(statusCode);
        }

        @Override
        public void haltOrThrow(int statusCode, String message) {
            Runtime.getRuntime().halt(statusCode);
        }

        @Override
        public void addShutdownRunnable(String name, Runnable runnable) {
            if (name != null)
                Runtime.getRuntime().addShutdownHook(KafkaThread.nonDaemon(name, runnable));
            else
                Runtime.getRuntime().addShutdownHook(new Thread(runnable));
        }
    }

    private static final class StaticContext extends Exit {

        private final Procedure exitProcedure;
        private final Procedure haltProcedure;
        private final ShutdownHookAdder shutdownHookAdder;

        private StaticContext(Procedure exitProcedure, Procedure haltProcedure, ShutdownHookAdder shutdownHookAdder) {
            this.exitProcedure = exitProcedure;
            this.haltProcedure = haltProcedure;
            this.shutdownHookAdder = shutdownHookAdder;
        }

        @Override
        public void exitOrThrow(int statusCode, String message) {
            exitProcedure.execute(statusCode, message);
        }

        @Override
        public void haltOrThrow(int statusCode, String message) {
            haltProcedure.execute(statusCode, message);
        }

        @Override
        public void addShutdownRunnable(String name, Runnable runnable) {
            shutdownHookAdder.addShutdownHook(name, runnable);
        }
    }
}
