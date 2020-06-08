/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.utils

import java.util.concurrent.{CountDownLatch, TimeUnit}

import org.apache.kafka.common.internals.FatalExitError
import org.junit.{After, Test}
import org.junit.Assert.{assertEquals, assertTrue}

class ShutdownableThreadTest {

  @After
  def tearDown(): Unit = Exit.resetExitProcedure()

  @Test
  def testShutdownWhenCalledAfterThreadStart(): Unit = {
    @volatile var statusCodeOption: Option[Int] = None
    Exit.setExitProcedure { (statusCode, _) =>
      statusCodeOption = Some(statusCode)
      // Sleep until interrupted to emulate the fact that `System.exit()` never returns
      Thread.sleep(Long.MaxValue)
      throw new AssertionError
    }
    val latch = new CountDownLatch(1)
    val thread = new ShutdownableThread("shutdownable-thread-test") {
      override def doWork(): Unit = {
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

}
