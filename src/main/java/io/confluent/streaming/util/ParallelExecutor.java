package io.confluent.streaming.util;

import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A lightweight parallel executor
 */
public class ParallelExecutor {

  /**
   * A parallel task must implement this interface
   */
  public interface Task {
    /**
     * Executes a task
     * @return boolean true if the task are ready for next execution
     */
    boolean process();
  }

  private final WorkerThread[] workerThreads;
  private final AtomicInteger taskIndex = new AtomicInteger(0);
  private volatile ArrayList<? extends Task> tasks = new ArrayList<>();
  private volatile CountDownLatch latch;
  private volatile boolean readyForNextExecution = true;
  private volatile boolean running = true;
  private volatile Exception exception;

  public ParallelExecutor(int parallelDegree) {
    parallelDegree = Math.max(parallelDegree, 1);
    workerThreads = new WorkerThread[parallelDegree - 1];
    for (int i = 0; i < workerThreads.length; i++) {
      workerThreads[i] = new WorkerThread();
      workerThreads[i].start();
    }
  }

  /**
   * Executes tasks in parallel. While this method is executing, other execute call will be blocked.
   * @param tasks a list of tasks executed in parallel
   * @return boolean true if all tasks are ready for next execution
   * @throws Exception an exception thrown by a failed task
   */
  public boolean execute(ArrayList<? extends Task> tasks) throws Exception {
    synchronized (this) {
      try {
        int numTasks = tasks.size();
        exception = null;
        readyForNextExecution = true;
        if (numTasks > 0) {
          this.tasks = tasks;
          this.latch = new CountDownLatch(numTasks);

          taskIndex.set(numTasks);
          wakeUpWorkers(Math.min(numTasks - 1, workerThreads.length));

          // the calling thread also picks up tasks
          if (taskIndex.get() > 0) doProcess();

          while (true) {
            try {
              latch.await();
              break;
            } catch (InterruptedException ex) {
              Thread.interrupted();
            }
          }
        }
        if (exception != null) throw exception;
      }
      finally {
        this.tasks = null;
        this.latch = null;
        this.exception = null;
      }
      return readyForNextExecution;
    }
  }

  /**
   * Shuts this parallel executor down
   */
  public void shutdown() {
    synchronized (this) {
      running = false;
      // wake up all workers
      wakeUpWorkers(workerThreads.length);
    }
  }

  private void doProcess() {
    int index = taskIndex.decrementAndGet();
    if (index >= 0) {
      try {
        if (!tasks.get(index).process())
          this.readyForNextExecution = false;
      }
      catch (Exception ex) {
        exception = ex;
      }
      finally {
        latch.countDown();
      }
    }
  }

  private void wakeUpWorkers(int numWorkers) {
    for (int i = 0; i < numWorkers; i++)
      LockSupport.unpark(workerThreads[i]);
  }

  private class WorkerThread extends Thread {

    WorkerThread() {
      super();
      setDaemon(true);
    }

    @Override
    public void run() {
      while (running) {
        if (taskIndex.get() > 0) {
          doProcess();
        }
        else {
          // no more work. park this thread.
          LockSupport.park();
          Thread.interrupted();
        }
      }
    }
  }

}
