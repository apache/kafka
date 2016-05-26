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

import org.junit.Assert._
import org.scalatest.Assertions
import org.junit.{Test, After, Before}

class IteratorTemplateTest extends Assertions {
  
  val lst = (0 until 10)
  val iterator = new IteratorTemplate[Int]() {
    var i = 0
    override def makeNext() = {
      if(i >= lst.size) {
        allDone()
      } else {
        val item = lst(i)
        i += 1
        item
      }
    }
  }

  @Test
  def testIterator() {
    for(i <- 0 until 10) {
      assertEquals("We should have an item to read.", true, iterator.hasNext)
      assertEquals("Checking again shouldn't change anything.", true, iterator.hasNext)
      assertEquals("Peeking at the item should show the right thing.", i, iterator.peek)
      assertEquals("Peeking again shouldn't change anything", i, iterator.peek)
      assertEquals("Getting the item should give the right thing.", i, iterator.next)
    }
    assertEquals("All gone!", false, iterator.hasNext)
    intercept[NoSuchElementException] {
      iterator.peek
    }
    intercept[NoSuchElementException] {
      iterator.next
    }
  }
  
}
