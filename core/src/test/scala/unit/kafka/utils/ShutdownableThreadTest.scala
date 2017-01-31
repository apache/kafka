package unit.kafka.utils

import java.util.concurrent.{CountDownLatch, TimeUnit}

import kafka.utils.{Exit, ShutdownableThread, TestUtils}
import org.apache.kafka.common.internals.FatalExitError
import org.junit.Test
import org.junit.Assert.{assertTrue, assertEquals}

class ShutdownableThreadTest {

  @Test
  def testShutdownIfCalledAfterThreadStart(): Unit = {
    @volatile var statusCodeOption: Option[Int] = None
    Exit.setExitProcedure { (statusCode, _) =>
      statusCodeOption = Some(statusCode)
      // sleep until interrupted to simulate the fact that `System.exit()` never returns
      Thread.sleep(Long.MaxValue)
      throw new AssertionError()
    }
    val latch = new CountDownLatch(1)
    val thread = new ShutdownableThread("shutdownable-thread-test") {
      override def doWork: Unit = {
        latch.countDown()
        throw new FatalExitError
      }
    }
    thread.start()
    assertTrue("doWork was not invoked", latch.await(10, TimeUnit.SECONDS))

    thread.shutdown()
    TestUtils.waitUntilTrue(() => statusCodeOption.isDefined, "Status code was not set by exit procedure")
    assertEquals(1, statusCodeOption.get)
  }

  @Test
  def testShutdownIfCalledAfterFatalExitExceptionIsThrown(): Unit = {
    @volatile var statusCodeOption: Option[Int] = None
    Exit.setExitProcedure { (statusCode, _) =>
      statusCodeOption = Some(statusCode)
      // sleep until interrupted to simulate the fact that `System.exit()` never returns
      Thread.sleep(Long.MaxValue)
      throw new AssertionError()
    }

    val thread = new ShutdownableThread("shutdownable-thread-test") {
      override def doWork: Unit = throw new FatalExitError
    }

    thread.start()
    TestUtils.waitUntilTrue(() => !thread.isRunning.get, "Thread did not transition to `running` state")

    thread.shutdown()
    TestUtils.waitUntilTrue(() => statusCodeOption.isDefined, "Status code was not set by exit procedure")
    assertEquals(1, statusCodeOption.get)
  }

}
