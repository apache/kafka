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
import org.apache.kafka.common.errors.FatalExitError
import org.junit.{Test, Before}
import org.junit.Assert.assertTrue

class ExitTest {
  @Before
  def init() {
    // TODO: to be replaced with proper mocking
    FatalExitError.testMode();
  }

  @Test
  def testSystemExitInShutdownableThread() {
    new ShutdownableThread("exit-test-thread") {
      override def doWork(): Unit = throw new FatalExitError(1)
    }.run()
    /**
     * if we reach this point it means that the {@link FatalExitError} is properly caught
     * and yet it successfully disabled invoking {@link System.exit} for unit tests
     */
    assertTrue(true);
  }
}
