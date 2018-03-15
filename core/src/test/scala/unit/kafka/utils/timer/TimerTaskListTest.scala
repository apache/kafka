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
package kafka.utils.timer

import org.junit.Assert._
import java.util.concurrent.atomic._
import org.junit.Test

class TimerTaskListTest {

  private class TestTask(val delayMs: Long) extends TimerTask {
    def run(): Unit = { }
  }

  private def size(list: TimerTaskList): Int = {
    var count = 0
    list.foreach(_ => count += 1)
    count
  }

  @Test
  def testAll() {
    val sharedCounter = new AtomicInteger(0)
    val list1 = new TimerTaskList(sharedCounter)
    val list2 = new TimerTaskList(sharedCounter)
    val list3 = new TimerTaskList(sharedCounter)

    val tasks = (1 to 10).map { i =>
      val task = new TestTask(0L)
      list1.add(new TimerTaskEntry(task, 10L))
      assertEquals(i, sharedCounter.get)
      task
    }

    assertEquals(tasks.size, sharedCounter.get)

    // reinserting the existing tasks shouldn't change the task count
    tasks.take(4).foreach { task =>
      val prevCount = sharedCounter.get
      // new TimerTaskEntry(task) will remove the existing entry from the list
      list2.add(new TimerTaskEntry(task, 10L))
      assertEquals(prevCount, sharedCounter.get)
    }
    assertEquals(10 - 4, size(list1))
    assertEquals(4, size(list2))

    assertEquals(tasks.size, sharedCounter.get)

    // reinserting the existing tasks shouldn't change the task count
    tasks.drop(4).foreach { task =>
      val prevCount = sharedCounter.get
      // new TimerTaskEntry(task) will remove the existing entry from the list
      list3.add(new TimerTaskEntry(task, 10L))
      assertEquals(prevCount, sharedCounter.get)
    }
    assertEquals(0, size(list1))
    assertEquals(4, size(list2))
    assertEquals(6, size(list3))

    assertEquals(tasks.size, sharedCounter.get)

    // cancel tasks in lists
    list1.foreach { _.cancel() }
    assertEquals(0, size(list1))
    assertEquals(4, size(list2))
    assertEquals(6, size(list3))

    list2.foreach { _.cancel() }
    assertEquals(0, size(list1))
    assertEquals(0, size(list2))
    assertEquals(6, size(list3))

    list3.foreach { _.cancel() }
    assertEquals(0, size(list1))
    assertEquals(0, size(list2))
    assertEquals(0, size(list3))
  }

}
