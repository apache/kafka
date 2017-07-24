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
package kafka.log

import java.io.File

import kafka.utils.TestUtils
import org.apache.kafka.common.requests.FetchResponse.AbortedTransaction
import org.junit.Assert._
import org.junit.{After, Before, Test}
import org.scalatest.junit.JUnitSuite

class TransactionIndexTest extends JUnitSuite {
  var file: File = _
  var index: TransactionIndex = _
  val offset = 0L

  @Before
  def setup(): Unit = {
    file = TestUtils.tempFile()
    index = new TransactionIndex(offset, file)
  }

  @After
  def teardown(): Unit = {
    index.close()
  }

  @Test
  def testPositionSetCorrectlyWhenOpened(): Unit = {
    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 0, lastOffset = 10, lastStableOffset = 11),
      new AbortedTxn(producerId = 1L, firstOffset = 5, lastOffset = 15, lastStableOffset = 13),
      new AbortedTxn(producerId = 2L, firstOffset = 18, lastOffset = 35, lastStableOffset = 25),
      new AbortedTxn(producerId = 3L, firstOffset = 32, lastOffset = 50, lastStableOffset = 40))
    abortedTxns.foreach(index.append)
    index.close()

    val reopenedIndex = new TransactionIndex(0L, file)
    val anotherAbortedTxn = new AbortedTxn(producerId = 3L, firstOffset = 50, lastOffset = 60, lastStableOffset = 55)
    reopenedIndex.append(anotherAbortedTxn)
    assertEquals(abortedTxns ++ List(anotherAbortedTxn), reopenedIndex.allAbortedTxns)
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testSanityCheck(): Unit = {
    val abortedTxns = List(
      new AbortedTxn(producerId = 0L, firstOffset = 0, lastOffset = 10, lastStableOffset = 11),
      new AbortedTxn(producerId = 1L, firstOffset = 5, lastOffset = 15, lastStableOffset = 13),
      new AbortedTxn(producerId = 2L, firstOffset = 18, lastOffset = 35, lastStableOffset = 25),
      new AbortedTxn(producerId = 3L, firstOffset = 32, lastOffset = 50, lastStableOffset = 40))
    abortedTxns.foreach(index.append)
    index.close()

    // open the index with a different starting offset to fake invalid data
    val reopenedIndex = new TransactionIndex(100L, file)
    reopenedIndex.sanityCheck()
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testLastOffsetMustIncrease(): Unit = {
    index.append(new AbortedTxn(producerId = 1L, firstOffset = 5, lastOffset = 15, lastStableOffset = 13))
    index.append(new AbortedTxn(producerId = 0L, firstOffset = 0, lastOffset = 15, lastStableOffset = 11))
  }

  @Test(expected = classOf[IllegalArgumentException])
  def testLastOffsetCannotDecrease(): Unit = {
    index.append(new AbortedTxn(producerId = 1L, firstOffset = 5, lastOffset = 15, lastStableOffset = 13))
    index.append(new AbortedTxn(producerId = 0L, firstOffset = 0, lastOffset = 10, lastStableOffset = 11))
  }

  @Test
  def testCollectAbortedTransactions(): Unit = {
    val abortedTransactions = List(
      new AbortedTxn(producerId = 0L, firstOffset = 0, lastOffset = 10, lastStableOffset = 11),
      new AbortedTxn(producerId = 1L, firstOffset = 5, lastOffset = 15, lastStableOffset = 13),
      new AbortedTxn(producerId = 2L, firstOffset = 18, lastOffset = 35, lastStableOffset = 25),
      new AbortedTxn(producerId = 3L, firstOffset = 32, lastOffset = 50, lastStableOffset = 40))

    abortedTransactions.foreach(index.append)

    var result = index.collectAbortedTxns(0L, 100L)
    assertEquals(abortedTransactions, result.abortedTransactions)
    assertFalse(result.isComplete)

    result = index.collectAbortedTxns(0L, 32)
    assertEquals(abortedTransactions.take(3), result.abortedTransactions)
    assertTrue(result.isComplete)

    result = index.collectAbortedTxns(0L, 35)
    assertEquals(abortedTransactions, result.abortedTransactions)
    assertTrue(result.isComplete)

    result = index.collectAbortedTxns(10, 35)
    assertEquals(abortedTransactions, result.abortedTransactions)
    assertTrue(result.isComplete)

    result = index.collectAbortedTxns(11, 35)
    assertEquals(abortedTransactions.slice(1, 4), result.abortedTransactions)
    assertTrue(result.isComplete)

    result = index.collectAbortedTxns(20, 41)
    assertEquals(abortedTransactions.slice(2, 4), result.abortedTransactions)
    assertFalse(result.isComplete)
  }

  @Test
  def testTruncate(): Unit = {
    val abortedTransactions = List(
      new AbortedTxn(producerId = 0L, firstOffset = 0, lastOffset = 10, lastStableOffset = 2),
      new AbortedTxn(producerId = 1L, firstOffset = 5, lastOffset = 15, lastStableOffset = 16),
      new AbortedTxn(producerId = 2L, firstOffset = 18, lastOffset = 35, lastStableOffset = 25),
      new AbortedTxn(producerId = 3L, firstOffset = 32, lastOffset = 50, lastStableOffset = 40))

    abortedTransactions.foreach(index.append)

    index.truncateTo(51)
    assertEquals(abortedTransactions, index.collectAbortedTxns(0L, 100L).abortedTransactions)

    index.truncateTo(50)
    assertEquals(abortedTransactions.take(3), index.collectAbortedTxns(0L, 100L).abortedTransactions)

    index.truncate()
    assertEquals(List.empty[AbortedTransaction], index.collectAbortedTxns(0L, 100L).abortedTransactions)
  }

  @Test
  def testAbortedTxnSerde(): Unit = {
    val pid = 983493L
    val firstOffset = 137L
    val lastOffset = 299L
    val lastStableOffset = 200L

    val abortedTxn = new AbortedTxn(pid, firstOffset, lastOffset, lastStableOffset)
    assertEquals(AbortedTxn.CurrentVersion, abortedTxn.version)
    assertEquals(pid, abortedTxn.producerId)
    assertEquals(firstOffset, abortedTxn.firstOffset)
    assertEquals(lastOffset, abortedTxn.lastOffset)
    assertEquals(lastStableOffset, abortedTxn.lastStableOffset)
  }

  @Test
  def testRenameIndex(): Unit = {
    val renamed = TestUtils.tempFile()
    index.append(new AbortedTxn(producerId = 0L, firstOffset = 0, lastOffset = 10, lastStableOffset = 2))

    index.renameTo(renamed)
    index.append(new AbortedTxn(producerId = 1L, firstOffset = 5, lastOffset = 15, lastStableOffset = 16))

    val abortedTxns = index.collectAbortedTxns(0L, 100L).abortedTransactions
    assertEquals(2, abortedTxns.size)
    assertEquals(0, abortedTxns(0).firstOffset)
    assertEquals(5, abortedTxns(1).firstOffset)
  }

}
