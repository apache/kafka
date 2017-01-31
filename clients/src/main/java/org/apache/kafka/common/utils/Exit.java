package org.apache.kafka.common.utils;

/**
 * Internal class that should be used instead of `Exit.exit()` and `Runtime.getRuntime().halt()` so that tests can
 * easily change the behaviour.
 */
public class Exit {

    public interface Procedure {
        void execute(int statusCode, String message);
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
            Exit.exit(statusCode);
        }
    };

    private volatile static Procedure exitProcedure = DEFAULT_EXIT_PROCEDURE;
    private volatile static Procedure haltProcedure = DEFAULT_HALT_PROCEDURE;

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

    public static void setExitProcedure(Procedure procedure) {
        exitProcedure = procedure;
    }

    public static void setHaltProcedure(Procedure procedure) {
        haltProcedure = procedure;
    }

    public static void resetExitProcedure() {
        exitProcedure = DEFAULT_EXIT_PROCEDURE;
    }

    public static void resetHaltProcedure() {
        haltProcedure = DEFAULT_HALT_PROCEDURE;
    }

}
