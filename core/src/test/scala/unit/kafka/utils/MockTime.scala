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

package kafka.utils

import org.apache.kafka.common.utils.{MockTime => JMockTime}

/**
 * A class used for unit testing things which depend on the Time interface.
 * There a couple of difference between this class and `org.apache.kafka.common.utils.MockTime`:
 *
 * 1. This has an associated scheduler instance for managing background tasks in a deterministic way.
 * 2. This doesn't support the `auto-tick` functionality as it interacts badly with the current implementation of `MockScheduler`.
 */
class MockTime(currentTimeMs: Long, currentHiResTimeNs: Long) extends JMockTime(0, currentTimeMs, currentHiResTimeNs) {

  def this() = this(System.currentTimeMillis(), System.nanoTime())

  val scheduler = new MockScheduler(this)

  override def sleep(ms: Long): Unit = {
    super.sleep(ms)
    scheduler.tick()
  }

}
