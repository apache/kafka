package io.confluent.streaming.internal;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.confluent.streaming.PunctuationScheduler;
import io.confluent.streaming.testutil.TestProcessor;
import org.junit.Test;

public class PunctuationSchedulerImplTest {

  @Test
  public void testScheduling() {
    PunctuationQueue queue = new PunctuationQueue();

    TestProcessor<String, String> proc1 = new TestProcessor<String, String>();
    PunctuationScheduler sched1 = new PunctuationSchedulerImpl(queue, proc1);

    TestProcessor<String, String> proc2 = new TestProcessor<String, String>();
    PunctuationScheduler sched2 = new PunctuationSchedulerImpl(queue, proc2);

    assertEquals(0, proc1.punctuated.size());
    assertEquals(0, proc2.punctuated.size());

    proc1.init(new ProcessorContextImpl(null, null, sched1));
    proc2.init(new ProcessorContextImpl(null, null, sched2));

    sched1.schedule(500);
    sched2.schedule(1000);

    assertEquals(0, proc1.punctuated.size());
    assertEquals(0, proc2.punctuated.size());

    queue.mayPunctuate(999);

    assertEquals(1, proc1.punctuated.size());
    assertEquals(999, (long) proc1.punctuated.get(0));
    assertEquals(0, proc2.punctuated.size());

    proc1.punctuated.clear();
    queue.mayPunctuate(1000);

    assertEquals(0, proc1.punctuated.size());
    assertEquals(1, proc2.punctuated.size());
    assertEquals(1000, (long) proc2.punctuated.get(0));

    proc2.punctuated.clear();
    queue.mayPunctuate(2000);
    assertEquals(0, proc1.punctuated.size());
    assertEquals(0, proc2.punctuated.size());

    sched1.schedule(3000);
    queue.mayPunctuate(4000);

    assertEquals(1, proc1.punctuated.size());
    assertEquals(0, proc2.punctuated.size());
    assertEquals(4000, (long) proc1.punctuated.get(0));
  }

  @Test
  public void testCanceling() {
    PunctuationQueue queue = new PunctuationQueue();

    TestProcessor<String, String> proc1 = new TestProcessor<String, String>();
    PunctuationScheduler sched1 = new PunctuationSchedulerImpl(queue, proc1);

    TestProcessor<String, String> proc2 = new TestProcessor<String, String>();
    PunctuationScheduler sched2 = new PunctuationSchedulerImpl(queue, proc2);

    assertEquals(0, proc1.punctuated.size());
    assertEquals(0, proc2.punctuated.size());

    proc1.init(new ProcessorContextImpl(null, null, sched1));
    proc2.init(new ProcessorContextImpl(null, null, sched2));

    sched1.schedule(500);
    sched2.schedule(1000);

    assertEquals(0, proc1.punctuated.size());
    assertEquals(0, proc2.punctuated.size());

    sched1.cancel();

    queue.mayPunctuate(1000);

    assertEquals(0, proc1.punctuated.size());
    assertEquals(1, proc2.punctuated.size());
    assertEquals(1000, (long) proc2.punctuated.get(0));

    sched1.schedule(2000);
    sched1.cancel();
    sched1.schedule(3000);

    queue.mayPunctuate(2000);
    assertEquals(0, proc1.punctuated.size());

    queue.mayPunctuate(3000);
    assertEquals(1, proc1.punctuated.size());
  }

  @Test
  public void testDuplicateScheduling() {
    PunctuationQueue queue = new PunctuationQueue();

    TestProcessor<String, String> proc1 = new TestProcessor<String, String>();
    PunctuationScheduler sched1 = new PunctuationSchedulerImpl(queue, proc1);

    assertEquals(0, proc1.punctuated.size());

    proc1.init(new ProcessorContextImpl(null, null, sched1));

    sched1.schedule(500);

    boolean exceptionRaised = false;
    try {
      sched1.schedule(1000);
    }
    catch (IllegalStateException e) {
      exceptionRaised = true;
    }

    assertTrue(exceptionRaised);
  }

}
