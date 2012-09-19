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

package kafka.log

import junit.framework.Assert._
import org.junit.Test
import org.scalatest.junit.JUnitSuite

class SegmentListTest extends JUnitSuite {

  @Test
  def testAppend() {
    val list = List(1, 2, 3, 4)
    val sl = new SegmentList(list)
    val view = sl.view
    assertEquals(list, view.iterator.toList)
    sl.append(5)
    assertEquals("Appending to both should result in list that are still equals", 
                 list ::: List(5), sl.view.iterator.toList)
    assertEquals("But the prior view should still equal the original list", list, view.iterator.toList)
  }
  
  @Test
  def testTrunc() {
    val hd = List(1,2,3)
    val tail = List(4,5,6)
    val sl = new SegmentList(hd ::: tail)
    val view = sl.view
    assertEquals(hd ::: tail, view.iterator.toList)
    val deleted = sl.trunc(3)
    assertEquals(tail, sl.view.iterator.toList)
    assertEquals(hd, deleted.iterator.toList)
    assertEquals("View should remain consistent", hd ::: tail, view.iterator.toList)
  }
  
  @Test
  def testTruncBeyondList() {
    val sl = new SegmentList(List(1, 2))
    sl.trunc(3)
    assertEquals(0, sl.view.length)
  }
  
}
