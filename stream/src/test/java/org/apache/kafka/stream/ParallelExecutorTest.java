package org.apache.kafka.stream;

import org.apache.kafka.stream.util.ParallelExecutor;
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
    AtomicInteger counter = new AtomicInteger(0);

    for (int i = 0; i < 5; i++) {
      taskList.add(new TestTask(counter));
    }

    parallelExecutor.execute(taskList);

    for (TestTask task : taskList) {
      assertEquals(task.executionCount, 1);
    }
    assertEquals(counter.get(), taskList.size());

    parallelExecutor.execute(taskList);

    for (TestTask task : taskList) {
      assertEquals(task.executionCount, 2);
    }
    assertEquals(counter.get(), taskList.size() * 2);
  }

  @Test
  public void testExecutingLongTaskList() throws Exception {
    ParallelExecutor parallelExecutor = new ParallelExecutor(10);
    ArrayList<TestTask> taskList = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger(0);

    for (int i = 0; i < 20; i++) {
      taskList.add(new TestTask(counter));
    }

    parallelExecutor.execute(taskList);

    for (TestTask task : taskList) {
      assertEquals(task.executionCount, 1);
    }
    assertEquals(counter.get(), taskList.size());

    parallelExecutor.execute(taskList);

    for (TestTask task : taskList) {
      assertEquals(task.executionCount, 2);
    }
    assertEquals(counter.get(), taskList.size() * 2);
  }

  @Test
  public void testException() {
    ParallelExecutor parallelExecutor = new ParallelExecutor(10);
    ArrayList<TestTask> taskList = new ArrayList<>();
    AtomicInteger counter = new AtomicInteger(0);

    for (int i = 0; i < 20; i++) {
      if (i == 15) {
        taskList.add(new TestTask(counter) {
          @Override
          public boolean process() {
            throw new TestException();
          }
        });
      }
      else {
        taskList.add(new TestTask(counter));
      }
    }

    Exception exception = null;
    try {
      parallelExecutor.execute(taskList);
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
    private AtomicInteger counter;

    TestTask(AtomicInteger counter) {
      this.counter = counter;
    }

    @Override
    public boolean process() {
      try {
        Thread.sleep(20);
        executionCount++;
      }
      catch (Exception ex) {
        // ignore
      }
      counter.incrementAndGet();

      return true;
    }
  }

  private static class TestException extends RuntimeException {
  }
}
