/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package unit.kafka.common

import kafka.utils.ShutdownableThread
import org.apache.kafka.common.{ExitException4Test, SecurityManager4Test}
import org.apache.kafka.common.errors.FatalExitError
import org.junit.{Assert, After, Test, Before}

class ExitTest {

  //TODO: make all unit tests perform this init (perhaps by inheriting from a base test class)
  @Before
  def init() {
    System.setSecurityManager(new SecurityManager4Test());
  }

  @After
  def tearDown() {
    System.setSecurityManager(null);
  }

  @Test
  def testSystemExitInShutdownableThread() {
    class FatalExitError4Test(exitStatus: Integer) extends FatalExitError(exitStatus) {
      var shutdownInvokerThread: Thread = null
      var exitIsInvoked = false
      override def setUncaughtExceptionHandler(t: Thread): Unit = {
        shutdownInvokerThread = t
        t.setUncaughtExceptionHandler(
          new Thread.UncaughtExceptionHandler() {
            def uncaughtException(t: Thread, e: Throwable): Unit = {
              exitIsInvoked = e.isInstanceOf[ExitException4Test]
            }
          }
        )
      }
    }
    val exitError = new FatalExitError4Test(1)
    new ShutdownableThread("exit-test-thread") {
      override def doWork(): Unit = throw exitError
    }.run()
    exitError.shutdownInvokerThread.join(1000)
    Assert.assertTrue(exitError.exitIsInvoked)
  }

  @Test
  def testDeadlockInShutdownHook(): Unit = {
    var shutdownableThread: Thread = null
    class FatalExitError4Test(exitStatus: Integer) extends FatalExitError(exitStatus) {
      var shutdownHookFinished = false;
      // instead of calling System.exit simulate running a shutdown hook that could cause a deadlock by joining for the invoker thread
      override def doShutdownSystem(): Unit = {
        val shutdownHookThread = new Thread() {
          shutdownableThread.join()
        }
        shutdownHookThread.start()
        shutdownHookThread.join()
        shutdownHookFinished = true
      }
    }
    val exc = new FatalExitError4Test(1)
    shutdownableThread = new ShutdownableThread("exit-test-thread") {
      override def doWork(): Unit = throw exc
    }
    shutdownableThread.start()
    shutdownableThread.join(1000)
    Assert.assertFalse(exc.shutdownHookFinished)
  }

}
