package io.confluent.streaming.util;

import org.junit.Test;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ParallelExecutorTest {

  @Test
  public void testExecutingShortTaskList() throws Exception {
    ParallelExecutor parallelExecutor = new ParallelExecutor(10);
    ArrayList<TestTask> taskList = new ArrayList<>();
    Counter counter = new Counter();

    for (int i = 0; i < 5; i++) {
      taskList.add(new TestTask());
    }

    parallelExecutor.execute(taskList, counter);

    for (TestTask task : taskList) {
      assertEquals(task.executionCount, 1);
    }
    assertEquals(counter.get(), taskList.size());

    parallelExecutor.execute(taskList, counter);

    for (TestTask task : taskList) {
      assertEquals(task.executionCount, 2);
    }
    assertEquals(counter.get(), taskList.size() * 2);
  }

  @Test
  public void testExecutingLongTaskList() throws Exception {
    ParallelExecutor parallelExecutor = new ParallelExecutor(10);
    ArrayList<TestTask> taskList = new ArrayList<>();
    Counter counter = new Counter();

    for (int i = 0; i < 20; i++) {
      taskList.add(new TestTask());
    }

    parallelExecutor.execute(taskList, counter);

    for (TestTask task : taskList) {
      assertEquals(task.executionCount, 1);
    }
    assertEquals(counter.get(), taskList.size());

    parallelExecutor.execute(taskList, counter);

    for (TestTask task : taskList) {
      assertEquals(task.executionCount, 2);
    }
    assertEquals(counter.get(), taskList.size() * 2);
  }

  @Test
  public void testException() {
    ParallelExecutor parallelExecutor = new ParallelExecutor(10);
    ArrayList<TestTask> taskList = new ArrayList<>();
    Counter counter = new Counter();

    for (int i = 0; i < 20; i++) {
      if (i == 15) {
        taskList.add(new TestTask() {
          @Override
          public void process(Object context) {
            throw new TestException();
          }
        });
      }
      else {
        taskList.add(new TestTask());
      }
    }

    Exception exception = null;
    try {
      parallelExecutor.execute(taskList, counter);
    }
    catch (Exception ex) {
      exception = ex;
    }

    assertEquals(counter.get(), taskList.size() - 1);
    assertFalse(exception == null);
    assertTrue(exception instanceof TestException);
  }

  private static class TestTask implements ParallelExecutor.Task {
    public volatile int executionCount = 0;

    @Override
    public void process(Object context) {
      Counter counter = (Counter) context;
      try {
        Thread.sleep(20);
        executionCount++;
      }
      catch (Exception ex) {
        // ignore
      }
      counter.incr();
    }
  }

  private static class Counter {
    private AtomicInteger totalExecutionCount = new AtomicInteger();

    public void incr() {
      totalExecutionCount.incrementAndGet();
    }

    public int get() {
      return totalExecutionCount.get();
    }
  }

  private static class TestException extends RuntimeException {
  }
}
