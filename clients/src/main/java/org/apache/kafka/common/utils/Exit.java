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

    public interface Procedure {
        void execute(int statusCode, String message);
    }

    public interface ShutdownHookAdder {
        void addShutdownHook(String name, Runnable runnable);
    }

    private static final Procedure DEFAULT_HALT_PROCEDURE = new Procedure() {
        @Override
        public void execute(int statusCode, String message) {
            Runtime.getRuntime().halt(statusCode);
        }
    };

    private static final Procedure DEFAULT_EXIT_PROCEDURE = new Procedure() {
        @Override
        public void execute(int statusCode, String message) {
            System.exit(statusCode);
        }
    };

    private static final ShutdownHookAdder DEFAULT_SHUTDOWN_HOOK_ADDER = new ShutdownHookAdder() {
        @Override
        public void addShutdownHook(String name, Runnable runnable) {
            if (name != null)
                Runtime.getRuntime().addShutdownHook(KafkaThread.nonDaemon(name, runnable));
            else
                Runtime.getRuntime().addShutdownHook(new Thread(runnable));
        }
    };

    // The procedures to use if no custom procedure has been installed via setExitProcedure/setHaltProcedure
    private volatile static Procedure fallbackExitProcedure = DEFAULT_EXIT_PROCEDURE;
    private volatile static Procedure fallbackHaltProcedure = DEFAULT_HALT_PROCEDURE;

    // Use InheritableThreadLocal so that all threads use the same custom procedure(s), but
    // once new test cases are started, leaked threads from prior tests don't have those custom procedures
    // overwritten by the new cases, and don't accidentally invoke the custom procedures for the new cases
    private static final InheritableThreadLocal<ProcedureWithFallback> EXIT_PROCEDURE = new InheritableThreadLocal<ProcedureWithFallback>() {
        @Override
        protected ProcedureWithFallback initialValue() {
            return ProcedureWithFallback.forExit();
        }
    };
    private static final InheritableThreadLocal<ProcedureWithFallback> HALT_PROCEDURE = new InheritableThreadLocal<ProcedureWithFallback>() {
        @Override
        protected ProcedureWithFallback initialValue() {
            return ProcedureWithFallback.forHalt();
        }
    };
    private volatile static ShutdownHookAdder shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER;

    public static void exit(int statusCode) {
        exit(statusCode, null);
    }

    public static void exit(int statusCode, String message) {
        EXIT_PROCEDURE.get().procedure().execute(statusCode, message);
    }

    public static void halt(int statusCode) {
        halt(statusCode, null);
    }

    public static void halt(int statusCode, String message) {
        HALT_PROCEDURE.get().procedure().execute(statusCode, message);
    }

    public static void addShutdownHook(String name, Runnable runnable) {
        shutdownHookAdder.addShutdownHook(name, runnable);
    }

    public static void setFallbackExitProcedure(Procedure procedure) {
        fallbackExitProcedure = procedure;
        EXIT_PROCEDURE.get().setFallback(procedure);
    }

    public static void setFallbackHaltProcedure(Procedure procedure) {
        fallbackHaltProcedure = procedure;
        HALT_PROCEDURE.get().setFallback(procedure);
    }

    public static void setExitProcedure(Procedure procedure) {
        EXIT_PROCEDURE.get().setCustom(procedure);
    }

    public static void setHaltProcedure(Procedure procedure) {
        HALT_PROCEDURE.get().setCustom(procedure);
    }

    public static void setShutdownHookAdder(ShutdownHookAdder shutdownHookAdder) {
        Exit.shutdownHookAdder = shutdownHookAdder;
    }

    public static void resetExitProcedure() {
        EXIT_PROCEDURE.get().useFallback();
        EXIT_PROCEDURE.set(ProcedureWithFallback.forExit());
    }

    public static void resetHaltProcedure() {
        HALT_PROCEDURE.get().useFallback();
        HALT_PROCEDURE.set(ProcedureWithFallback.forHalt());
    }

    public static void resetShutdownHookAdder() {
        shutdownHookAdder = DEFAULT_SHUTDOWN_HOOK_ADDER;
    }

    private static class ProcedureWithFallback {
        private volatile Procedure fallback;
        private volatile Procedure custom;
        private volatile boolean useCustom;

        public static ProcedureWithFallback forExit() {
            return new ProcedureWithFallback(fallbackExitProcedure);
        }

        public static ProcedureWithFallback forHalt() {
            return new ProcedureWithFallback(fallbackHaltProcedure);
        }

        private ProcedureWithFallback(Procedure fallbackProcedure) {
            this.fallback = fallbackProcedure;
            this.custom = null;
            this.useCustom = true;
        }

        public void setCustom(Procedure procedure) {
            this.custom = procedure;
        }

        public void setFallback(Procedure procedure) {
            this.fallback = procedure;
        }

        // Disable any custom procedures and force the fallback to be used; should be invoked
        // once a test case has completed, in order to prevent leaked threads from that test case
        // from calling the custom procedure
        public void useFallback() {
            useCustom = false;
        }

        public Procedure procedure() {
            return custom != null && useCustom ? custom : fallback;
        }
    }
}
