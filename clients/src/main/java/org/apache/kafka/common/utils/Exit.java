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
public class Exit {

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

    private volatile static Procedure exitProcedure = DEFAULT_EXIT_PROCEDURE;
    private volatile static Procedure haltProcedure = DEFAULT_HALT_PROCEDURE;
    private volatile static ShutdownHookAdder shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER;

    public static void exit(int statusCode) {
        exit(statusCode, null);
    }

    public static void exit(int statusCode, String message) {
        exitProcedure.execute(statusCode, message);
    }

    public static void halt(int statusCode) {
        halt(statusCode, null);
    }

    public static void halt(int statusCode, String message) {
        haltProcedure.execute(statusCode, message);
    }

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
}
