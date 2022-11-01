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
package kafka.test.junit

import kafka.utils.Exit
import org.junit.jupiter.api.Assertions.{assertInstanceOf, assertNotNull, assertThrows, assertTrue}
import org.junit.jupiter.api.{MethodOrderer, Order, Test, TestMethodOrder}

import java.util.concurrent.{CountDownLatch, TimeUnit}
import java.util.concurrent.atomic.AtomicReference

object SystemExitExtensionTest {
  private val lateExitStatus = 923
  private val lateExitOrigin = new AtomicReference[Object]

  @volatile private var lateExitReady: CountDownLatch = _
  @volatile private var lateExitInvoked: CountDownLatch = _
}

@TestMethodOrder(classOf[MethodOrderer.OrderAnnotation])
class SystemExitExtensionTest {

  @Test
  @Order(1)
  def testSystemExitInvokedOnMainTestThread(): Unit = {
    SystemExitExtension.allowExitFromCurrentTest()

    val exitStatus = 618
    // The caller should know that the attempt to terminate the JVM has failed
    assertThrows(classOf[AssertionError], () => Exit.exit(exitStatus))
    // The extension should track that exit has been called in this test as well
    SystemExitExtension.assertExitCalledFromCurrentTest(exitStatus)
  }

  @Test
  @Order(2)
  def testSystemExitInvokedOnSeparateTestThread(): Unit = {
    SystemExitExtension.allowExitFromCurrentTest()

    val exitStatus = 815
    val exitInvoked = new CountDownLatch(1)
    val exitException = new AtomicReference[Throwable]()
    new Thread(() => {
      try {
        Exit.exit(exitStatus)
      } catch {
        case t: Throwable => exitException.set(t)
      } finally {
        exitInvoked.countDown()
      }
    }).start()

    awaitLatch(
      exitInvoked,
      "Thread created during this test did not complete execution in time"
    )
    // Wait for our separate thread to complete
    exitInvoked.await(10, TimeUnit.SECONDS)

    // The caller should know that the attempt to terminate the JVM has failed
    assertNotNull(exitException.get)
    assertInstanceOf(classOf[AssertionError], exitException.get)

    // The extension should track that exit has been called in this test as well
    SystemExitExtension.assertExitCalledFromCurrentTest(exitStatus)
  }

  @Test
  @Order(3)
  // This test doesn't actually make any assertions; all it does is "leak" a thread that
  // then invokes System::exit later on while a different test is running, so that that
  // other test can ensure that the call was properly handled and attributed to this one
  def spawnLateExitingTest(): Unit = {
    SystemExitExtension.allowExitFromCurrentTest()

    SystemExitExtensionTest.lateExitOrigin.set(this)
    SystemExitExtensionTest.lateExitReady = new CountDownLatch(1)
    SystemExitExtensionTest.lateExitInvoked = new CountDownLatch(1)

    new Thread(() => {
      // Await this latch indefinitely
      SystemExitExtensionTest.lateExitReady.await()
      try {
        Exit.exit(SystemExitExtensionTest.lateExitStatus)
      } finally {
        SystemExitExtensionTest.lateExitInvoked.countDown()
      }
    }).start()
  }

  @Test
  @Order(4)
  // This test ensures that the "leaked" thread from the above test is properly
  // handled and attributed to that test
  def testSystemExitInvokedOnLeakedTestThread(): Unit = {
    assertNotNull(SystemExitExtensionTest.lateExitOrigin.get())
    SystemExitExtensionTest.lateExitReady.countDown()
    awaitLatch(
      SystemExitExtensionTest.lateExitInvoked,
      "Leaked thread from prior test did not complete execution in time"
    )

    SystemExitExtension.assertExitFromPriorTest(SystemExitExtensionTest.lateExitOrigin.get(), SystemExitExtensionTest.lateExitStatus)
  }

  private def awaitLatch(latch: CountDownLatch, assertionMessage: String): Unit = {
    assertTrue(latch.await(10, TimeUnit.SECONDS), assertionMessage)
  }

}
