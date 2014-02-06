package kafka.common.utils;

/**
 * A wrapper for Thread that sets things up nicely
 */
public class KafkaThread extends Thread {

    public KafkaThread(String name, Runnable runnable, boolean daemon) {
        super(runnable, name);
        setDaemon(daemon);
        setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                e.printStackTrace();
            }
        });
    }

}
